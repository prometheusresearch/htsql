#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.assemble`
========================

This module implements the assemble adapter.
"""


from ..adapter import Adapter, adapts
from .code import (Expression, Code, Space, ScalarSpace, ProductSpace,
                   FilteredSpace, OrderedSpace, MaskedSpace,
                   Unit, ColumnUnit, AggregateUnit, CorrelatedUnit,
                   QueryExpression, SegmentExpression,
                   GroupExpression, AggregateGroupExpression)
from .term import (RoutingTerm, ScalarTerm, TableTerm, FilterTerm, JoinTerm,
                   CorrelationTerm, ProjectionTerm, OrderTerm, WrapperTerm,
                   SegmentTerm, QueryTerm, ParallelTie, SeriesTie)


class AssemblingState(object):

    def __init__(self):
        self.next_id = 1
        self.baseline_stack = []
        self.baseline = None
        self.mask_stack = []
        self.mask = None

    def make_id(self):
        id = self.next_id
        self.next_id += 1
        return id

    def push_baseline(self, baseline):
        assert isinstance(baseline, Space) and baseline.is_inflated
        self.baseline_stack.append(self.baseline)
        self.baseline = baseline

    def pop_baseline(self):
        self.baseline = self.baseline_stack.pop()

    def push_mask(self, mask):
        assert isinstance(mask, Space)
        self.mask_stack.append(self.mask)
        self.mask = mask

    def pop_mask(self):
        self.mask = self.mask_stack.pop()

    def assemble(self, expression, baseline=None, mask=None):
        return assemble(expression, self, baseline=baseline, mask=mask)

    def inject(self, term, expressions):
        if not expressions:
            return term
        if len(expressions) == 1:
            expression = expressions[0]
        else:
            expression = GroupExpression(expressions, term.binding)
        inject = Inject(expression, term, self)
        return inject()


class Assemble(Adapter):

    adapts(Expression)

    def __init__(self, expression, state):
        assert isinstance(expression, Expression)
        assert isinstance(state, AssemblingState)
        self.expression = expression
        self.state = state

    def __call__(self):
        raise NotImplementedError("the assemble adapter is not implemented"
                                  " for a %r node" % self.expression)


class Inject(Adapter):

    adapts(Expression)

    def __init__(self, expression, term, state):
        assert isinstance(expression, Expression)
        assert isinstance(term, RoutingTerm)
        assert isinstance(state, AssemblingState)
        self.expression = expression
        self.term = term
        self.state = state

    def __call__(self):
        raise NotImplementedError("the inject adapter is not implemented"
                                  " for a %r node" % self.expression)


class AssembleQuery(Assemble):

    adapts(QueryExpression)

    def __call__(self):
        segment = None
        if self.expression.segment is not None:
            segment = self.state.assemble(self.expression.segment)
        return QueryTerm(segment, self.expression)


class AssembleSegment(Assemble):

    adapts(SegmentExpression)

    def __call__(self):
        kid = self.state.assemble(self.expression.space,
                                  baseline=self.expression.space.scalar,
                                  mask=self.expression.space.scalar)
        order = self.expression.space.ordering()
        codes = self.expression.elements + [code for code, direction in order]
        kid = self.state.inject(kid, codes)
        kid = OrderTerm(self.state.make_id(), kid, order, None, None,
                        kid.space, kid.routes.copy())
        return SegmentTerm(self.state.make_id(), kid, self.expression.elements,
                           kid.space, kid.routes.copy())


class AssembleSpace(Assemble):

    adapts(Space)

    def __init__(self, space, state):
        assert isinstance(space, Space)
        assert isinstance(state.baseline, Space)
        backbone = space.inflate()
        assert backbone.concludes(state.baseline)
        super(AssembleSpace, self).__init__(space, state)
        self.space = space
        self.state = state
        self.baseline = state.baseline
        self.mask = state.mask
        self.backbone = backbone


class InjectSpace(Inject):

    adapts(Space)

    def __init__(self, space, term, state):
        assert isinstance(space, Space)
        assert term.space.spans(space)
        super(InjectSpace, self).__init__(space, term, state)
        self.space = space
        self.term = term
        self.state = state

    def __call__(self):
        if self.space in self.term.routes:
            return self.term
        unmasked_space = self.space.prune(self.term.space)
        if unmasked_space in self.term.routes:
            routes = self.term.routes.copy()
            routes[self.space] = routes[unmasked_space]
            return self.term.clone(routes=routes)
        if self.term.backbone.concludes(unmasked_space):
            id = self.state.make_id()
            next_axis = self.term.baseline
            while next_axis.base != unmasked_space:
                next_axis = next_axis.base
            lkid = self.state.inject(self.term, [next_axis])
            assert unmasked_space not in lkid.routes
            rkid = self.state.assemble(unmasked_space,
                                       baseline=unmasked_space,
                                       mask=unmasked_space.scalar)
            assert unmasked_space.base not in rkid.routes
            tie = SeriesTie(next_axis, is_backward=True)
            routes = lkid.routes.copy()
            routes[unmasked_space] = rkid[unmasked_space]
            routes[self.space] = rkid[unmasked_space]
            return JoinTerm(id, lkid, rkid, [tie], True, lkid.space, routes)
        id = self.state.make_id()
        baseline = unmasked_space
        while not baseline.is_inflated:
            baseline = baseline.base
        lkid = self.term
        rkid = self.state.assemble(self.space,
                                   baseline=baseline,
                                   mask=self.term.space)
        ties = []
        if lkid.backbone.concludes(rkid.baseline):
            lkid = self.state.inject(lkid, [rkid.baseline])
            axis = lkid.backbone
            while rkid.baseline.base != axis:
                if axis in rkid.routes:
                    tie = ParallelTie(axis)
                    ties.append(tie)
            ties.reverse()
        else:
            lkid = self.state.inject(lkid, [rkid.baseline.base])
            tie = SeriesTie(rkid.baseline)
            ties.append(tie)
        is_inner = rkid.space.dominates(lkid.space)
        routes = lkid.routes.copy()
        routes[self.space] = rkid.routes[self.space]
        routes[unmasked_space] = rkid.routes[self.space]
        return JoinTerm(id, lkid, rkid, ties, is_inner, lkid.space, routes)


class AssembleScalar(AssembleSpace):

    adapts(ScalarSpace)

    def __call__(self):
        id = self.state.make_id()
        routes = { self.space: id }
        return ScalarTerm(id, self.space, routes)


class AssembleProduct(AssembleSpace):

    adapts(ProductSpace)

    def __call__(self):
        if self.backbone == self.baseline:
            id = self.state.make_id()
            routes = { self.space: id, self.backbone: id }
            return TableTerm(id, self.space, routes)
        term = self.state.assemble(self.space.base)
        if self.backbone in term.routes and self.space.conforms(term.space):
            routes = term.routes.copy()
            routes[self.space] = routes[self.backbone]
            return term.clone(routes=routes)
        id = self.state.make_id()
        lkid = term
        rkid = self.state.assemble(self.space, baseline=self.backbone)
        routes = lkid.routes.copy()
        routes[self.space] = rkid.routes[self.space]
        routes[self.backbone] = rkid.routes[self.backbone]
        tie = SeriesTie(self.space)
        return JoinTerm(id, lkid, rkid, [tie], True, self.space, routes)


class AssembleFiltered(AssembleSpace):

    adapts(FilteredSpace)

    def __call__(self):
        term = self.state.assemble(self.space.base)
        if self.space.prune(self.mask) == term.space.prune(self.mask):
            routes = term.routes.copy()
            routes[self.space] = routes[self.backbone]
            return term.clone(space=self.space, routes=routes)
        id = self.state.make_id()
        kid = self.state.inject(term, [self.space.filter])
        routes = kid.routes.copy()
        routes[self.space] = routes[self.backbone]
        return FilterTerm(id, kid, self.space.filter, self.space, routes)


class AssembleOrdered(AssembleSpace):

    adapts(OrderedSpace)

    def __call__(self):
        if (self.space.prune(self.mask) == self.space.base.prune(self.mask)
                or (self.space.limit is None and self.space.offset is None)):
            term = self.state.assemble(self.space.base)
            routes = term.routes.copy()
            routes[self.space] = routes[self.backbone]
            return term.clone(space=self.space, routes=routes)
        id = self.state.make_id()
        kid = self.state.assemble(self.space.base,
                                  baseline=self.space.scalar,
                                  mask=self.space.scalar)
        order = self.space.ordering()
        codes = [code for code, direction in order]
        kid = self.state.inject(kid, codes)
        routes = kid.routes.copy()
        routes[self.space] = routes[self.backbone]
        return OrderTerm(id, kid, order, self.space.limit, self.space.offset,
                         self.space, routes)


class InjectCode(Inject):

    adapts(Code)

    def __call__(self):
        return self.state.inject(self.term, self.expression.units)


class InjectUnit(Inject):

    adapts(Unit)

    def __init__(self, unit, term, state):
        assert isinstance(unit, Unit)
        super(InjectUnit, self).__init__(unit, term, state)
        self.unit = unit
        self.space = unit.space

    def __call__(self):
        raise NotImplementedError("the inject adapter is not implemented"
                                  " for a %r node" % self.unit)


class InjectColumn(Inject):

    adapts(ColumnUnit)

    def __call__(self):
        if not self.unit.singular(self.term.space):
            raise AssembleError("expected a singular expression",
                                self.unit.mark)
        return self.state.inject(self.term, [self.unit.space])


class InjectAggregate(Inject):

    adapts(AggregateUnit)

    def __call__(self):
        if not self.unit.singular(self.term.space):
            raise AssembleError("expected a singular expression",
                                self.unit.mark)
        if self.unit in self.term.routes:
            return self.term
        is_native = self.space.dominates(self.term.space)
        if is_native:
            ground_term = self.term
        else:
            baseline = self.space.prune(self.term.space)
            while not baseline.is_inflated:
                baseline = baseline.base
            ground_term = self.state.assemble(self.space,
                                              baseline=baseline,
                                              mask=self.term.space)
        baseline = self.unit.plural_space.prune(ground_term.space)
        while not baseline.is_inflated:
            baseline = baseline.base
        if not ground_term.space.spans(baseline):
            while not ground_term.space.spans(baseline.base):
                baseline = baseline.base
        plural_term = self.state.assemble(self.unit.plural_space,
                                          baseline=baseline,
                                          mask=ground_term.space)
        plural_term = self.state.inject(plural_term, [self.unit.composite])
        projected_space = None
        ties = []
        axes = []
        if ground_term.backbone.concludes(plural_term.baseline):
            ground_term = self.state.inject(ground_term,
                                            [plural_term.baseline])
            axis = ground_term.backbone
            while axis not in plural_term.routes:
                axis = axis.baseline
            projected_space = MaskedSpace(axis, ground_term.space,
                                          self.unit.binding)
            while axis in plural_term.routes:
                tie = ParallelTie(axis)
                ties.append(tie)
                axes.append(axis)
                axis = axis.base
            ties.reverse()
            axes.reverse()
        else:
            axis = plural_term.baseline
            ground_term = self.state.inject(ground_term, [axis.base])
            projected_space = MaskedSpace(axis.base, ground_term.space,
                                          self.unit.binding)
            tie = SeriesTie(axis)
            ties.append(tie)
            axes.append(axis)
        id = self.state.make_id()
        routes = {}
        for axis in axes:
            routes[axis] = plural_term.routes[axis]
        routes[projected_space] = routes[axes[-1]]
        routes[projected_space.inflate()] = routes[axes[-1]]
        projected_term = ProjectionTerm(id, plural_term, ties,
                                        projected_space, routes)
        id = self.state.make_id()
        lkid = ground_term
        rkid = projected_term
        is_inner = projected_term.space.dominates(ground_term.space)
        routes = lkid.routes.copy()
        routes[self.unit] = projected_term.id
        term = JoinTerm(id, lkid, rkid, ties, is_inner, lkid.space, routes)
        if is_native:
            return term
        id = self.state.make_id()
        lkid = self.term
        rkid = term
        ties = []
        if lkid.backbone.concludes(rkid.baseline):
            lkid = self.state.inject(lkid, [rkid.baseline])
            axis = lkid.backbone
            while rkid.baseline.base != axis:
                if axis in rkid.routes:
                    tie = ParallelTie(axis)
                    ties.append(tie)
            ties.reverse()
        else:
            lkid = self.state.inject(lkid, [rkid.baseline.base])
            tie = SeriesTie(rkid.baseline)
            ties.append(tie)
        is_inner = rkid.space.dominates(lkid.space)
        routes = lkid.routes.copy()
        routes[self.unit] = projected_term.id
        return JoinTerm(id, lkid, rkid, ties, is_inner, lkid.space, routes)


class InjectCorrelated(Inject):

    adapts(CorrelatedUnit)

    def __call__(self):
        if not self.unit.singular(self.term.space):
            raise AssembleError("expected a singular expression",
                                self.unit.mark)
        if self.unit in self.term.routes:
            return self.term
        is_native = self.space.dominates(self.term.space)
        if is_native:
            ground_term = self.term
        else:
            baseline = self.space.prune(self.term.space)
            while not baseline.is_inflated:
                baseline = baseline.base
            ground_term = self.state.assemble(self.space,
                                              baseline=baseline,
                                              mask=self.term.space)
        baseline = self.unit.plural_space.prune(ground_term.space)
        while not baseline.is_inflated:
            baseline = baseline.base
        if not ground_term.space.spans(baseline):
            while not ground_term.space.spans(baseline.base):
                baseline = baseline.base
        plural_term = self.state.assemble(self.unit.plural_space,
                                          baseline=baseline,
                                          mask=ground_term.space)
        plural_term = self.state.inject(plural_term, [self.unit.composite])
        plural_term = WrapperTerm(self.state.make_id(), plural_term,
                                  plural_term.space, plural_term.routes.copy())
        ties = []
        axes = []
        if ground_term.backbone.concludes(plural_term.baseline):
            ground_term = self.state.inject(ground_term,
                                            [plural_term.baseline])
            axis = ground_term.backbone
            while axis not in plural_term.routes:
                axis = axis.baseline
            while axis in plural_term.routes:
                tie = ParallelTie(axis)
                ties.append(tie)
                axes.append(axis)
                axis = axis.base
            ties.reverse()
        else:
            axis = plural_term.baseline
            ground_term = self.state.inject(ground_term, [axis.base])
            tie = SeriesTie(axis)
            ties.append(tie)
            axes.append(axis)
        id = self.state.make_id()
        lkid = ground_term
        rkid = plural_term
        routes = lkid.routes.copy()
        routes[self.unit] = plural_term.id
        term = CorrelationTerm(id, lkid, rkid, ties, lkid.space, routes)
        if is_native:
            return term
        id = self.state.make_id()
        lkid = self.term
        rkid = term
        ties = []
        if lkid.backbone.concludes(rkid.baseline):
            lkid = self.state.inject(lkid, [rkid.baseline])
            axis = lkid.backbone
            while rkid.baseline.base != axis:
                if axis in rkid.routes:
                    tie = ParallelTie(axis)
                    ties.append(tie)
            ties.reverse()
        else:
            lkid = self.state.inject(lkid, [rkid.baseline.base])
            tie = SeriesTie(rkid.baseline)
            ties.append(tie)
        is_inner = rkid.space.dominates(lkid.space)
        routes = lkid.routes.copy()
        routes[self.unit] = plural_term.id
        return JoinTerm(id, lkid, rkid, ties, is_inner, lkid.space, routes)


class InjectGroup(Inject):

    adapts(GroupExpression)

    def __call__(self):
        units = []
        for code in self.expression.codes:
            for unit in code.units:
                units.append(unit)
        aggregate_forms = []
        aggregate_form_to_units = {}
        for unit in units:
            if (isinstance(unit, AggregateUnit) and
                    unit not in self.term.routes):
                form = (unit.plural_space, unit.space)
                if form not in aggregate_form_to_units:
                    aggregate_forms.append(form)
                    aggregate_form_to_units[form] = []
                aggregate_form_to_units[form].append(unit)
        aggregate_groups = []
        for form in aggregate_forms:
            plural_space, space = form
            form_units = aggregate_form_to_units[form]
            if len(form_units) > 1:
                group = AggregateGroupExpression(plural_space, space,
                                                 form_units, self.term.binding)
                aggregate_groups.append(group)
        term = self.term
        for group in aggregate_groups:
            term = self.state.inject(term, [group])
        for unit in units:
            term = self.state.inject(term, [unit])
        return term


class InjectAggregateGroup(Inject):

    adapts(AggregateGroupExpression)

    def __call__(self):
        term = self.term
        for aggregate in self.expression.aggregates:
            term = self.state.inject(term, [aggregate])
        return term


def assemble(expression, state=None, baseline=None, mask=None):
    if state is None:
        state = AssemblingState()
    if baseline is not None:
        state.push_baseline(baseline)
    if mask is not None:
        state.push_mask(mask)
    assemble = Assemble(expression, state)
    term = assemble()
    if baseline is not None:
        state.pop_baseline()
    if mask is not None:
        state.pop_mask()
    return term


