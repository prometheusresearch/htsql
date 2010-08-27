#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.assembler`
=========================

This module implements the assemble adapter.
"""


from ..adapter import Adapter, adapts
from .code import (Code, Space, ScalarSpace, FreeTableSpace, JoinedTableSpace,
                   ScreenSpace, OrderedSpace, RelativeSpace, Expression, Unit,
                   ColumnUnit, AggregateUnit, CorrelatedUnit,
                   SegmentCode, QueryCode)
from .term import (TableTerm, ScalarTerm, FilterTerm, JoinTerm,
                   CorrelationTerm, ProjectionTerm, OrderingTerm, HangingTerm,
                   SegmentTerm, QueryTerm, ParallelTie, SeriesTie,
                   LEFT, RIGHT, FORWARD)


class Assembler(object):

    def assemble(self, code, *args):
        assemble = Assemble(code, self)
        return assemble.assemble(*args)

    def inject(self, code, term):
        assemble = Assemble(code, self)
        return assemble.inject(term)


class Assemble(Adapter):

    adapts(Code, Assembler)

    def __init__(self, code, assembler):
        self.code = code
        self.assembler = assembler

    def assemble(self):
        raise NotImplementedError()

    def inject(self, term):
        raise NotImplementedError()


class AssembleSpace(Assemble):

    adapts(Space, Assembler)

    def __init__(self, space, assembler):
        self.space = space
        self.assembler = assembler

    def assemble(self, baseline):
        raise NotImplementedError()


class AssembleExpression(Assemble):

    adapts(Expression, Assembler)

    def inject(self, term):
        for unit in self.code.get_units():
            term = self.assembler.inject(unit, term)
        return term


class AssembleUnit(AssembleExpression):

    adapts(Unit, Assembler)

    def inject(self, term):
        raise NotImplementedError()


class AssembleScalar(AssembleSpace):

    adapts(ScalarSpace, Assembler)

    def assemble(self, baseline):
        assert baseline == self.space
        routes = {}
        routes[self.space] = []
        return ScalarTerm(self.space, baseline, routes, self.space.mark)

    def inject(self, term):
        if self.space in term.routes:
            return term
        axis = term.baseline
        while axis.parent != self.space:
            axis = axis.parent
        term = self.assembler.inject(axis, term)
        assert self.space not in term.routes
        left = term
        right_routes = {}
        right_routes[self.space] = []
        right = ScalarTerm(self.space, self.space,
                           right_routes, self.space.mark)
        tie = SeriesTie(axis, is_reverse=True)
        routes = {}
        for key in left.routes:
            routes[key] = [LEFT] + left.routes[key]
        routes[self.space] = [RIGHT]
        return JoinTerm(left, right, [tie], True, left.space, left.baseline,
                        routes, left.mark)


class AssembleFreeTable(AssembleSpace):

    adapts(FreeTableSpace, Assembler)

    def assemble(self, baseline):
        backbone = self.space.axes()
        if baseline == backbone:
            routes = {}
            routes[self.space] = []
            routes[backbone] = []
            return TableTerm(self.space.table, self.space, baseline,
                             routes, self.space.mark)
        left = self.assembler.assemble(self.space.parent, baseline)
        right_routes = {}
        right_routes[self.space] = []
        right_routes[backbone] = []
        right = TableTerm(self.space.table, self.space, backbone,
                          right_routes, self.space.mark)
        routes = {}
        for key in left.routes:
            routes[key] = [LEFT] + left.routes[key]
        routes[self.space] = [RIGHT]
        routes[backbone] = [RIGHT]
        tie = SeriesTie(self.space)
        return JoinTerm(left, right, [tie], True, self.space, baseline,
                        routes, self.space.mark)

    def inject(self, term):
        if self.space in term.routes:
            return term
        assert term.space.spans(self.space)
        space = self.space.inflate(term.space)
        if self.space != space:
            term = self.assembler.inject(space, term)
            routes = term.routes.copy()
            routes[self.space] = routes[space]
            return term.clone(routes=routes)
        backbone = self.space.axes()
        term_backbone = term.space.axes()
        assert term_backbone.concludes(backbone)
        if self.space == backbone:
            axis = term_backbone
            while axis.parent != self.space:
                axis = axis.parent
            left = self.assembler.inject(axis, term)
            assert self.space not in left.routes
            right_routes = {}
            right_routes[self.space] = []
            right = TableTerm(self.space.table, self.space, self.space,
                              right_routes, self.space.mark)
            tie = SeriesTie(axis, is_reverse=True)
            routes = {}
            for key in left.routes:
                routes[key] = [LEFT] + left.routes[key]
            routes[self.space] = [RIGHT]
            return JoinTerm(left, right, [tie], True, left.space,
                            left.baseline, routes, left.mark)
        left = term
        left = self.assembler.inject(self.space.parent, left)
        left = self.assembler.inject(backbone, left)
        assert self.space not in left.routes
        right_routes = {}
        right_routes[self.space] = []
        right_routes[backbone] = []
        right = TableTerm(self.space.table, self.space, backbone,
                          right_routes, self.space.mark)
        backbone_tie = ParallelTie(backbone)
        parent_tie = SeriesTie(self.space)
        routes = {}
        for key in left.routes:
            routes[key] = [LEFT] + left.routes[key]
        routes[self.space] = [RIGHT]
        return JoinTerm(left, right, [backbone_tie, parent_tie], False,
                        left.space, left.baseline, routes, left.mark)


class AssembleJoinedTable(AssembleSpace):

    adapts(JoinedTableSpace, Assembler)

    def assemble(self, baseline):
        backbone = self.space.axes()
        if baseline == backbone:
            routes = {}
            routes[self.space] = []
            routes[backbone] = []
            return TableTerm(self.space.table, self.space, baseline,
                             routes, self.space.mark)
        term = self.assembler.assemble(self.space.parent, baseline)
        assert self.space not in term.routes
        if backbone in term.routes:
            if term.space.conforms(self.space):
                routes = term.routes.copy()
                routes[self.space] = routes[backbone]
                term = term.clone(space=self.space, routes=routes)
                return term
            assert term.space.dominates(self.space)
        left = term
        right_routes = {}
        right_routes[self.space] = []
        right_routes[backbone] = []
        right = TableTerm(self.space.table, self.space, backbone,
                          right_routes, self.space.mark)
        routes = {}
        for key in left.routes:
            routes[key] = [LEFT]+left.routes[key]
        routes[self.space] = [RIGHT]
        routes[backbone] = [RIGHT]
        tie = SeriesTie(self.space)
        return JoinTerm(left, right, [tie], True, self.space, baseline,
                        routes, self.space.mark)

    def inject(self, term):
        if self.space in term.routes:
            return term
        assert term.space.spans(self.space)
        space = self.space.inflate(term.space)
        if self.space != space:
            term = self.assembler.inject(space, term)
            routes = term.routes.copy()
            routes[self.space] = routes[space]
            return term.clone(routes=routes)
        backbone = self.space.axes()
        term_backbone = term.space.axes()
        if term_backbone.concludes(self.space):
            axis = term_backbone
            while axis.parent != self.space:
                axis = axis.parent
            left = self.assembler.inject(axis, term)
            assert self.space not in left.routes
            right_routes = {}
            right_routes[self.space] = []
            right = TableTerm(self.space.table, self.space, self.space,
                              right_routes, self.space.mark)
            tie = SeriesTie(axis, is_reverse=True)
            routes = {}
            for key in left.routes:
                routes[key] = [LEFT] + left.routes[key]
            routes[self.space] = [RIGHT]
            return JoinTerm(left, right, [tie], True, left.space,
                            left.baseline, routes, left.mark)
        left = term
        left = self.assembler.inject(self.space.parent, left)
        if not self.space.is_contracting:
            left = self.assembler.inject(backbone, left)
        assert self.space not in left.routes
        right_routes = {}
        right_routes[self.space] = []
        right_routes[backbone] = []
        right = TableTerm(self.space.table, self.space, backbone,
                          right_routes, self.space.mark)
        ties = []
        if not self.space.is_contracting:
            backbone_tie = ParallelTie(backbone)
            ties.append(backbone_tie)
        parent_tie = SeriesTie(self.space)
        ties.append(parent_tie)
        is_inner = True
        space = self.space
        while not term_backbone.concludes(space):
            if not space.is_expanding:
                is_inner = False
                break
            space = space.parent
        routes = {}
        for key in left.routes:
            routes[key] = [LEFT] + left.routes[key]
        routes[self.space] = [RIGHT]
        return JoinTerm(left, right, ties, is_inner,
                        left.space, left.baseline, routes, left.mark)


class AssembleScreen(AssembleSpace):

    adapts(ScreenSpace, Assembler)

    def assemble(self, baseline):
        child = self.assembler.assemble(self.space.parent, baseline)
        child = self.assembler.inject(self.space.filter, child)
        assert self.space not in child.routes
        routes = {}
        for key in child.routes:
            routes[key] = [FORWARD] + child.routes[key]
        routes[self.space] = [FORWARD] + child.routes[self.space.parent]
        return FilterTerm(child, self.space.filter, self.space, baseline,
                          routes, self.space.mark)

    def inject(self, term):
        if self.space in term.routes:
            return term
        assert term.space.spans(self.space)
        space = self.space.inflate(term.space)
        if self.space != space:
            term = self.assembler.inject(space, term)
            routes = term.routes.copy()
            routes[self.space] = routes[space]
            return term.clone(routes=routes)
        left = term
        term_backbone = term.space.axes()
        baseline = self.space.axes()
        right = self.assembler.assemble(self.space, baseline)
        ties = []
        if term_backbone.concludes(baseline) or baseline.parent in left.routes:
            axes = baseline
            while axes in right.routes:
                left = self.assembler.inject(axes, left)
                tie = ParallelTie(axes)
                ties.append(tie)
                axes = axes.parent
        else:
            space = self.space
            while not space.is_axis:
                space = space.parent
            assert space in right.routes
            left = self.assembler.inject(space.parent, left)
            tie = SeriesTie(space)
        routes = {}
        for key in left.routes:
            routes[key] = [LEFT] + left.routes[key]
        routes[self.space] = [RIGHT] + right.routes[self.space]
        return JoinTerm(left, right, ties, False,
                        left.space, left.baseline, routes, left.mark)


class AssembleOrdered(AssembleSpace):

    adapts(OrderedSpace, Assembler)

    def assemble(self, baseline):
        child = self.assembler.assemble(self.space.parent, baseline)
        order = []
        codes = set()
        for code, dir in self.space.ordering():
            if code not in codes:
                order.append((code, dir))
                codes.add(code)
        for code, dir in order:
            child = self.assembler.inject(code, child)
        assert self.space not in child.routes
        routes = {}
        for key in child.routes:
            routes[key] = [FORWARD] + child.routes[key]
        routes[self.space] = [FORWARD] + child.routes[self.space.parent]
        return OrderingTerm(child, order,
                            self.space.limit, self.space.offset,
                            self.space, baseline, routes, self.space.mark)


    def inject(self, term):
        assert self.space in term.routes
        return term


class AssembleColumn(AssembleUnit):

    adapts(ColumnUnit, Assembler)

    def inject(self, term):
        return self.assembler.inject(self.code.space, term)


class AssembleAggregate(AssembleUnit):

    adapts(AggregateUnit, Assembler)

    def inject(self, term):
        if self.code in term.routes:
            return term
        assert term.space.spans(self.code.space)
        assert not term.space.spans(self.code.plural_space)
        assert not self.code.space.spans(self.code.plural_space)
        assert self.code.plural_space.spans(self.code.space)
        with_base_term = (not self.code.space.dominates(term.space))
        if with_base_term:
            base_space = self.code.space
        else:
            base_space = term.space
            left = term
        base_backbone = base_space.axes()
        plural_space = self.code.plural_space.inflate(base_space)
        baseline = plural_space
        while not base_space.concludes(baseline.parent):
            baseline = baseline.parent
        baseline = baseline.axes()
        plural_term = self.assembler.assemble(plural_space, baseline)
        plural_term = self.assembler.inject(self.code.expression, plural_term)
        ties = []
        axes = []
        if (base_backbone.concludes(baseline)
                or baseline.parent in plural_term.routes):
            axis = baseline
            while axis in plural_term.routes:
                axes.append(axis)
                tie = ParallelTie(axis)
                ties.append(tie)
                axis = axis.parent
        else:
            axes.append(baseline)
            tie = SeriesTie(baseline)
            ties.append(tie)
        relative_space = RelativeSpace(baseline.parent,
                                       plural_space, self.code.mark)
        routes = {}
        for axis in axes:
            routes[axis] = [FORWARD] + plural_term.routes[axis]
        routes[self.code] = []
        projected_term = ProjectionTerm(plural_term, ties, relative_space,
                                        baseline.parent, routes,
                                        self.code.mark)
        if with_base_term:
            assert False
        if (base_backbone.concludes(baseline)
                or baseline.parent in plural_term.routes):
            for axis in axes:
                term = self.assembler.inject(axis, term)
        else:
            for axis in axes:
                term = self.assembler.inject(axis.parent, term)
        left = term
        right = projected_term
        routes = {}
        for key in left.routes:
            routes[key] = [LEFT] + left.routes[key]
        routes[self.code] = [RIGHT]
        return JoinTerm(left, right, ties, False,
                        left.space, left.baseline, routes, left.mark)


class AssembleCorrelated(AssembleUnit):

    adapts(CorrelatedUnit, Assembler)

    def inject(self, term):
        if self.code in term.routes:
            return term
        assert term.space.spans(self.code.space)
        assert not term.space.spans(self.code.plural_space)
        assert not self.code.space.spans(self.code.plural_space)
        assert self.code.plural_space.spans(self.code.space)
        with_base_term = (not self.code.space.dominates(term.space))
        if with_base_term:
            base_space = self.code.space
        else:
            base_space = term.space
            left = term
        base_backbone = base_space.axes()
        plural_space = self.code.plural_space.inflate(base_space)
        baseline = plural_space
        while not base_space.concludes(baseline.parent):
            baseline = baseline.parent
        baseline = baseline.axes()
        plural_term = self.assembler.assemble(plural_space, baseline)
        plural_term = self.assembler.inject(self.code.expression, plural_term)
        routes = {}
        for key in plural_term.routes:
            routes[key] = [FORWARD] + plural_term.routes[key]
        plural_term = HangingTerm(plural_term,
                                  plural_term.space, plural_term.baseline,
                                  routes, plural_term.mark)
        ties = []
        axes = []
        if (base_backbone.concludes(baseline)
                or baseline.parent in plural_term.routes):
            axis = baseline
            while axis in plural_term.routes:
                axes.append(axis)
                tie = ParallelTie(axis)
                ties.append(tie)
                axis = axis.parent
        else:
            axes.append(baseline)
            tie = SeriesTie(baseline)
            ties.append(tie)
        if with_base_term:
            assert False
        if (base_backbone.concludes(baseline)
                or baseline.parent in plural_term.routes):
            for axis in axes:
                term = self.assembler.inject(axis, term)
        else:
            for axis in axes:
                term = self.assembler.inject(axis.parent, term)
        left = term
        right = plural_term
        routes = {}
        for key in left.routes:
            routes[key] = [LEFT] + left.routes[key]
        routes[self.code] = [RIGHT]
        return CorrelationTerm(left, right, ties,
                               left.space, left.baseline, routes, left.mark)


class AssembleSegment(Assemble):

    adapts(SegmentCode, Assembler)

    def assemble(self):
        child = self.assembler.assemble(self.code.space, self.code.space.scalar)
        child = self.assembler.inject(self.code, child)
        select = self.code.elements
        routes = {}
        for key in child.routes:
            routes[key] = [FORWARD] + child.routes[key]
        return SegmentTerm(child, select, child.space, child.baseline,
                           routes, self.code.mark)

    def inject(self, term):
        for element in self.code.elements:
            for unit in element.get_units():
                term = self.assembler.inject(unit, term)
        return term


class AssembleQuery(Assemble):

    adapts(QueryCode, Assembler)

    def assemble(self):
        segment = None
        if self.code.segment is not None:
            segment = self.assembler.assemble(self.code.segment)
        return QueryTerm(self.code, segment, self.code.mark)


