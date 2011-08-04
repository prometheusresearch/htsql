#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql.tr.rewrite`
=======================

This module implements the rewriting process.
"""


from ..adapter import Adapter, adapts
from ..domain import BooleanDomain
from .coerce import coerce
from .flow import (Expression, QueryExpr, SegmentExpr, Flow, RootFlow,
                   QuotientFlow, ComplementFlow, MonikerFlow, ForkedFlow,
                   LinkedFlow, FilteredFlow, OrderedFlow,
                   Code, LiteralCode, CastCode, FormulaCode, Unit,
                   CompoundUnit, ScalarUnit, ScalarBatchUnit,
                   AggregateUnitBase, AggregateUnit, AggregateBatchUnit,
                   KernelUnit, CoveringUnit)
from .signature import Signature, OrSig, AndSig
from .fn.signature import IfSig


class RewritingState(object):

    def __init__(self):
        self.root = None
        self.mask = None
        self.mask_stack = []
        self.collection = None
        self.collection_stack = []
        self.replacements = None
        self.replacements_stack = []

    def set_root(self, flow):
        assert isinstance(flow, RootFlow)
        assert self.root is None
        assert self.mask is None
        assert self.collection is None
        assert self.replacements is None
        self.root = flow
        self.mask = flow
        self.collection = []
        self.replacements = {}

    def flush(self):
        assert self.root is not None
        assert self.mask is self.root
        assert not self.mask_stack
        assert not self.collection_stack
        assert not self.replacements_stack
        self.root = None
        self.mask = None
        self.collection = None
        self.replacements = None

    def push_mask(self, mask):
        assert isinstance(mask, Flow)
        self.mask_stack.append(self.mask)
        self.mask = mask

    def pop_mask(self):
        self.mask = self.mask_stack.pop()

    def save_collection(self):
        self.collection_stack.append(self.collection)
        self.replacements_stack.append(self.replacements)
        self.collection = []
        self.replacements = {}

    def restore_collection(self):
        self.collection = self.collection_stack.pop()
        self.replacements = self.replacements_stack.pop()

    def memorize(self, expression, replacement):
        assert isinstance(expression, Expression)
        assert isinstance(replacement, Expression)
        assert expression not in self.replacements
        self.replacements[expression] = replacement

    def rewrite(self, expression):
        return rewrite(expression, self)

    def unmask(self, expression, mask=None):
        if mask is not None:
            self.push_mask(mask)
        unmask = Unmask(expression, self)
        expression = unmask()
        if mask is not None:
            self.pop_mask()
        return expression

    def collect(self, expression):
        collect = Collect(expression, self)
        collect()

    def recombine(self):
        duplicates = set()
        scalar_flows = []
        scalar_flow_to_units = {}
        for unit in self.collection:
            if isinstance(unit, ScalarUnit):
                if unit in duplicates:
                    continue
                duplicates.add(unit)
                flow = unit.flow
                if flow not in scalar_flow_to_units:
                    scalar_flows.append(flow)
                    scalar_flow_to_units[flow] = []
                scalar_flow_to_units[flow].append(unit)
        for flow in scalar_flows:
            batch_units = scalar_flow_to_units[flow]
            if len(batch_units) <= 1:
                continue
            codes = [unit.code for unit in batch_units]
            self.save_collection()
            for code in codes:
                self.collect(code)
            self.recombine()
            codes = [self.replace(code) for code in codes]
            self.restore_collection()
            for idx, unit in enumerate(batch_units):
                code = codes[idx]
                companions = codes[:idx]+codes[idx+1:]
                batch = ScalarBatchUnit(code, companions, flow,
                                        unit.binding)
                self.memorize(unit, batch)

        duplicates = set()
        aggregate_flow_pairs = []
        aggregate_flow_pair_to_units = {}
        for unit in self.collection:
            if isinstance(unit, AggregateUnit):
                if unit in duplicates:
                    continue
                duplicates.add(unit)
                flow = unit.flow
                plural_flow = unit.plural_flow
                while isinstance(plural_flow, FilteredFlow):
                    plural_flow = plural_flow.base
                pair = (plural_flow, flow)
                if pair not in aggregate_flow_pair_to_units:
                    aggregate_flow_pairs.append(pair)
                    aggregate_flow_pair_to_units[pair] = []
                aggregate_flow_pair_to_units[pair].append(unit)
        for pair in aggregate_flow_pairs:
            batch_units = aggregate_flow_pair_to_units[pair]
            if len(batch_units) <= 1:
                continue
            plural_flow, flow = pair
            candidate_flows = []
            candidate_flow = batch_units[0].plural_flow
            candidate_flows.append(candidate_flow)
            while isinstance(candidate_flow, FilteredFlow):
                candidate_flow = candidate_flow.base
                candidate_flows.append(candidate_flow)
            candidate_flows.reverse()
            for unit in batch_units[1:]:
                alternate_flows = []
                alternate_flow = unit.plural_flow
                alternate_flows.append(alternate_flow)
                while isinstance(alternate_flow, FilteredFlow):
                    alternate_flow = alternate_flow.base
                    alternate_flows.append(alternate_flow)
                alternate_flows.reverse()
                if len(alternate_flows) < len(candidate_flows):
                    candidate_flows = candidate_flows[-len(alternate_flows):]
                for idx in range(len(candidate_flows)):
                    if candidate_flows[idx] != alternate_flows[idx]:
                        assert idx > 0
                        candidate_flows = candidate_flows[:idx]
                        break
            combined_flow = candidate_flows[-1]
            codes = []
            filters = []
            for unit in batch_units:
                code = unit.code
                code_filters = []
                unit_flow = unit.plural_flow
                while unit_flow != combined_flow:
                    code_filters.append(unit_flow.filter)
                    unit_flow = unit_flow.base
                if code_filters:
                    if len(code_filters) > 1:
                        code_filter = FormulaCode(AndSig(),
                                                  coerce(BooleanDomain()),
                                                  unit.flow.binding,
                                                  ops=code_filters)
                    else:
                        [code_filter] = code_filters
                    filters.append(code_filters[-1])
                    op = code.op
                    op = FormulaCode(IfSig(), op.domain, op.binding,
                                     predicates=[code_filter],
                                     consequents=[op],
                                     alternative=None)
                    code = code.clone(op=op)
                codes.append(code)
            if all(unit.plural_flow != combined_flow
                   for unit in batch_units):
                if len(filters) > 1:
                    filter = FormulaCode(OrSig(), coerce(BooleanDomain()),
                                         combined_flow.binding,
                                         ops=filters)
                else:
                    [filter] = filters
                combined_flow = FilteredFlow(combined_flow, filter,
                                             combined_flow.binding)
            self.save_collection()
            for code in codes:
                self.collect(code)
            self.recombine()
            codes = [self.replace(code) for code in codes]
            self.restore_collection()
            for idx, unit in enumerate(batch_units):
                code = codes[idx]
                companions = codes[:idx]+codes[idx+1:]
                batch = AggregateBatchUnit(code, companions, combined_flow,
                                           flow, unit.binding)
                self.memorize(unit, batch)

    def replace(self, expression):
        if expression in self.replacements:
            return self.replacements[expression]
        replace = Replace(expression, self)
        replacement = replace()
        self.replacements[expression] = replacement
        return replacement


class Rewrite(Adapter):

    adapts(Expression)

    def __init__(self, expression, state):
        assert isinstance(expression, Expression)
        assert isinstance(state, RewritingState)
        self.expression = expression
        self.state = state

    def __call__(self):
        raise NotImplementedError("the rewrite adapter is not implemented"
                                  " for a %r node" % self.expression)


class Unmask(Adapter):

    adapts(Expression)

    def __init__(self, expression, state):
        assert isinstance(expression, Expression)
        assert isinstance(state, RewritingState)
        self.expression = expression
        self.state = state

    def __call__(self):
        raise NotImplementedError("the unmask adapter is not implemented"
                                  " for a %r node" % self.expression)


class Collect(Adapter):

    adapts(Expression)

    def __init__(self, expression, state):
        assert isinstance(expression, Expression)
        assert isinstance(state, RewritingState)
        self.expression = expression
        self.state = state

    def __call__(self):
        raise NotImplementedError("the collect adapter is not implemented"
                                  " for a %r node" % self.expression)


class Replace(Adapter):

    adapts(Expression)

    def __init__(self, expression, state):
        assert isinstance(expression, Expression)
        assert isinstance(state, RewritingState)
        self.expression = expression
        self.state = state

    def __call__(self):
        raise NotImplementedError("the replace adapter is not implemented"
                                  " for a %r node" % self.expression)


class RewriteQuery(Rewrite):

    adapts(QueryExpr)

    def __call__(self):
        segment = None
        if self.expression.segment is not None:
            segment = self.state.rewrite(self.expression.segment)
        return self.expression.clone(segment=segment)


class RewriteSegment(Rewrite):

    adapts(SegmentExpr)

    def __call__(self):
        self.state.set_root(self.expression.flow.root)
        elements = [self.state.rewrite(element)
                    for element in self.expression.elements]
        flow = self.state.rewrite(self.expression.flow)
        elements = [self.state.unmask(element, mask=flow)
                    for element in elements]
        flow = self.state.unmask(flow)
        self.state.collect(flow)
        for element in elements:
            self.state.collect(element)
        self.state.recombine()
        flow = self.state.replace(flow)
        elements = [self.state.replace(element)
                    for element in elements]
        self.state.flush()
        return self.expression.clone(flow=flow, elements=elements)


class RewriteFlow(Rewrite):

    adapts(Flow)

    def __init__(self, flow, state):
        super(RewriteFlow, self).__init__(flow, state)
        self.flow = flow

    def __call__(self):
        if self.flow.base is None:
            return self.flow
        base = self.state.rewrite(self.flow.base)
        return self.flow.clone(base=base)


class UnmaskFlow(Unmask):

    adapts(Flow)

    def __init__(self, flow, state):
        super(UnmaskFlow, self).__init__(flow, state)
        self.flow = flow

    def __call__(self):
        if self.flow.base is None:
            return self.flow
        base = self.state.unmask(self.flow.base)
        return self.flow.clone(base=base)


class CollectInFlow(Collect):

    adapts(Flow)

    def __init__(self, flow, state):
        super(CollectInFlow, self).__init__(flow, state)
        self.flow = flow

    def __call__(self):
        if self.flow.base is None:
            return
        self.state.collect(self.flow.base)


class ReplaceInFlow(Replace):

    adapts(Flow)

    def __init__(self, flow, state):
        super(ReplaceInFlow, self).__init__(flow, state)
        self.flow = flow

    def __call__(self):
        if self.flow.base is None:
            return self.flow
        base = self.state.replace(self.flow.base)
        return self.flow.clone(base=base)


class RewriteQuotient(RewriteFlow):

    adapts(QuotientFlow)

    def __call__(self):
        base = self.state.rewrite(self.flow.base)
        seed = self.state.rewrite(self.flow.family.seed)
        kernel = [self.state.rewrite(code)
                  for code in self.flow.family.kernel]
        return self.flow.clone(base=base, seed=seed, kernel=kernel)


class UnmaskQuotient(UnmaskFlow):

    adapts(QuotientFlow)

    def __call__(self):
        kernel = [self.state.unmask(code, mask=self.flow.family.seed)
                  for code in self.flow.family.kernel]
        seed = self.state.unmask(self.flow.family.seed, mask=self.flow.base)
        base = self.state.unmask(self.flow.base)
        return self.flow.clone(base=base, seed=seed, kernel=kernel)


class ReplaceInQuotient(Collect):

    adapts(QuotientFlow)

    def __call__(self):
        base = self.state.replace(self.flow.base)
        self.state.save_collection()
        self.state.collect(self.flow.seed)
        for code in self.flow.family.kernel:
            self.state.collect(code)
        self.state.recombine()
        seed = self.state.replace(self.flow.family.seed)
        kernel = [self.state.replace(code)
                  for code in self.flow.family.kernel]
        self.state.restore_collection()
        return self.flow.clone(base=base, seed=seed, kernel=kernel)


class RewriteMoniker(RewriteFlow):

    adapts(MonikerFlow)

    def __call__(self):
        base = self.state.rewrite(self.flow.base)
        seed = self.state.rewrite(self.flow.seed)
        return self.flow.clone(base=base, seed=seed)


class UnmaskMoniker(UnmaskFlow):

    adapts(MonikerFlow)

    def __call__(self):
        seed = self.state.unmask(self.flow.seed, mask=self.flow.base)
        base = self.state.unmask(self.flow.base)
        return self.flow.clone(base=base, seed=seed)


class ReplaceInMoniker(Replace):

    adapts(MonikerFlow)

    def __call__(self):
        base = self.state.replace(self.flow.base)
        self.state.save_collection()
        self.state.collect(self.flow.seed)
        self.state.recombine()
        seed = self.state.replace(self.flow.seed)
        self.state.restore_collection()
        return self.flow.clone(base=base, seed=seed)


class RewriteForked(RewriteFlow):

    adapts(ForkedFlow)

    def __call__(self):
        base = self.state.rewrite(self.flow.base)
        seed = self.state.rewrite(self.flow.seed)
        kernel = [self.state.rewrite(code)
                  for code in self.flow.kernel]
        return self.flow.clone(base=base, seed=seed, kernel=kernel)


class UnmaskForked(UnmaskFlow):

    adapts(ForkedFlow)

    def __call__(self):
        seed_mask = self.flow.base
        while not seed_mask.is_axis:
            seed_mask = seed_mask.base
        seed = self.state.unmask(self.flow.seed, mask=seed_mask)
        kernel = [self.state.unmask(code, mask=self.flow.base)
                  for code in self.flow.kernel]
        base = self.state.unmask(self.flow.base)
        return self.flow.clone(base=base, seed=seed, kernel=kernel)


class CollectInForked(Collect):

    adapts(ForkedFlow)

    def __call__(self):
        self.state.collect(self.flow.base)
        for code in self.flow.kernel:
            self.state.collect(code)


class ReplaceInForked(Replace):

    adapts(ForkedFlow)

    def __call__(self):
        base = self.state.replace(self.flow.base)
        kernel = [self.state.replace(code) for code in self.flow.kernel]
        self.state.save_collection()
        self.state.collect(self.flow.seed)
        self.state.recombine()
        seed = self.state.replace(self.flow.seed)
        self.state.restore_collection()
        return self.flow.clone(base=base, seed=seed, kernel=kernel)


class RewriteLinked(RewriteFlow):

    adapts(LinkedFlow)

    def __call__(self):
        base = self.state.rewrite(self.flow.base)
        seed = self.state.rewrite(self.flow.seed)
        kernel = [self.state.rewrite(code)
                  for code in self.flow.kernel]
        counter_kernel = [self.state.rewrite(code)
                          for code in self.flow.counter_kernel]
        return self.flow.clone(base=base, seed=seed, kernel=kernel,
                                counter_kernel=counter_kernel)


class UnmaskLinked(UnmaskFlow):

    adapts(LinkedFlow)

    def __call__(self):
        base = self.state.unmask(self.flow.base)
        seed = self.state.unmask(self.flow.seed, mask=self.flow.base)
        kernel = [self.state.unmask(code, mask=self.flow.seed)
                  for code in self.flow.kernel]
        counter_kernel = [self.state.unmask(code, mask=self.flow.base)
                          for code in self.flow.counter_kernel]
        return self.flow.clone(base=base, seed=seed, kernel=kernel,
                                counter_kernel=counter_kernel)


class CollectInLinked(Collect):

    adapts(LinkedFlow)

    def __call__(self):
        self.state.collect(self.flow.base)
        for code in self.flow.counter_kernel:
            self.state.collect(code)


class ReplaceInLinked(Replace):

    adapts(LinkedFlow)

    def __call__(self):
        base = self.state.replace(self.flow.base)
        counter_kernel = [self.state.replace(code)
                          for code in self.flow.counter_kernel]
        self.state.save_collection()
        self.state.collect(self.flow.seed)
        for code in self.flow.kernel:
            self.state.collect(code)
        self.state.recombine()
        seed = self.state.replace(self.flow.seed)
        kernel = [self.state.replace(code)
                  for code in self.flow.kernel]
        self.state.restore_collection()
        return self.flow.clone(base=base, seed=seed, kernel=kernel,
                                counter_kernel=counter_kernel)


class RewriteFiltered(RewriteFlow):

    adapts(FilteredFlow)

    def __call__(self):
        base = self.state.rewrite(self.flow.base)
        filter = self.state.rewrite(self.flow.filter)
        if (isinstance(filter, LiteralCode) and
            isinstance(filter.domain, BooleanDomain) and
            filter.value is True):
            return base
        #if isinstance(base, FilteredFlow):
        #    filter = FormulaCode(AndSig(), coerce(BooleanDomain()),
        #                         filter.binding,
        #                         ops=[base.filter, filter])
        #    return base.clone(filter=filter)
        return self.flow.clone(base=base, filter=filter)


class UnmaskFiltered(UnmaskFlow):

    adapts(FilteredFlow)

    def __call__(self):
        if (self.flow.prune(self.state.mask)
                == self.flow.base.prune(self.state.mask)):
            return self.state.unmask(self.flow.base)
        if self.flow.base.dominates(self.state.mask):
            filter = self.state.unmask(self.flow.filter)
        else:
            filter = self.state.unmask(self.flow.filter,
                                       mask=self.flow.base)
        base = self.state.unmask(self.flow.base)
        return self.flow.clone(base=base, filter=filter)


class CollectInFiltered(Collect):

    adapts(FilteredFlow)

    def __call__(self):
        self.state.collect(self.flow.base)
        self.state.collect(self.flow.filter)


class ReplaceInFiltered(Replace):

    adapts(FilteredFlow)

    def __call__(self):
        base = self.state.replace(self.flow.base)
        filter = self.state.replace(self.flow.filter)
        return self.flow.clone(base=base, filter=filter)


class RewriteOrdered(RewriteFlow):

    adapts(OrderedFlow)

    def __call__(self):
        base = self.state.rewrite(self.flow.base)
        order = [(self.state.rewrite(code), direction)
                 for code, direction in self.flow.order]
        return self.flow.clone(base=base, order=order)


class UnmaskOrdered(UnmaskFlow):

    adapts(OrderedFlow)

    def __call__(self):
        if (self.flow.prune(self.state.mask)
                == self.flow.base.prune(self.state.mask)):
            return self.state.unmask(self.flow.base)
        if self.flow.base.dominates(self.state.mask):
            order = [(self.state.unmask(code), direction)
                     for code, direction in self.flow.order]
        else:
            order = [(self.state.unmask(code, mask=self.flow.base),
                      direction)
                     for code, direction in self.flow.order]
        if self.flow.is_expanding:
            base = self.state.unmask(self.flow.base)
        else:
            base = self.state.unmask(self.flow.base, mask=self.flow.root)
        return self.flow.clone(base=base, order=order)


class CollectInOrdered(Collect):

    adapts(OrderedFlow)

    def __call__(self):
        self.state.collect(self.flow.base)
        for code, direction in self.flow.order:
            self.state.collect(code)


class ReplaceInOrdered(Replace):

    adapts(OrderedFlow)

    def __call__(self):
        base = self.state.replace(self.flow.base)
        order = [(self.state.replace(code), direction)
                 for code, direction in self.flow.order]
        return self.flow.clone(base=base, order=order)


class RewriteCode(Rewrite):

    adapts(Code)

    def __init__(self, code, state):
        super(RewriteCode, self).__init__(code, state)
        self.code = code

    def __call__(self):
        return self.code


class UnmaskCode(Unmask):

    adapts(Code)

    def __init__(self, code, state):
        super(UnmaskCode, self).__init__(code, state)
        self.code = code

    def __call__(self):
        return self.code


class CollectInCode(Collect):

    adapts(Code)

    def __init__(self, code, state):
        super(CollectInCode, self).__init__(code, state)
        self.code = code

    def __call__(self):
        for unit in self.code.units:
            self.state.collect(unit)


class ReplaceInCode(Replace):

    adapts(Code)

    def __init__(self, code, state):
        super(ReplaceInCode, self).__init__(code, state)
        self.code = code

    def __call__(self):
        return self.code


class RewriteCast(RewriteCode):

    adapts(CastCode)

    def __call__(self):
        base = self.state.rewrite(self.code.base)
        return self.code.clone(base=base)


class UnmaskCast(UnmaskCode):

    adapts(CastCode)

    def __call__(self):
        base = self.state.unmask(self.code.base)
        return self.code.clone(base=base)


class ReplaceCast(ReplaceInCode):

    adapts(CastCode)

    def __call__(self):
        base = self.state.replace(self.code.base)
        return self.code.clone(base=base)


class RewriteFormula(RewriteCode):

    adapts(FormulaCode)

    def __call__(self):
        rewrite = RewriteBySignature(self.code, self.state)
        return rewrite()


class UnmaskFormula(UnmaskCode):

    adapts(FormulaCode)

    def __call__(self):
        arguments = self.code.arguments.map(self.state.unmask)
        return FormulaCode(self.code.signature, self.code.domain,
                           self.code.binding, **arguments)


class ReplaceInFormula(ReplaceInCode):

    adapts(FormulaCode)

    def __call__(self):
        arguments = self.code.arguments.map(self.state.replace)
        return FormulaCode(self.code.signature, self.code.domain,
                           self.code.binding, **arguments)


class RewriteBySignature(Adapter):

    adapts(Signature)

    @classmethod
    def dispatch(interface, code, *args, **kwds):
        assert isinstance(code, FormulaCode)
        return (type(code.signature),)

    def __init__(self, code, state):
        assert isinstance(code, FormulaCode)
        assert isinstance(state, RewritingState)
        self.code = code
        self.state = state
        self.signature = code.signature
        self.domain = code.domain
        self.arguments = code.arguments

    def __call__(self):
        arguments = self.arguments.map(self.state.rewrite)
        return FormulaCode(self.signature, self.domain,
                           self.code.binding, **arguments)


class RewriteUnit(RewriteCode):

    adapts(Unit)

    def __init__(self, unit, state):
        super(RewriteUnit, self).__init__(unit, state)
        self.unit = unit

    def __call__(self):
        flow = self.state.rewrite(self.unit.flow)
        return self.unit.clone(flow=flow)


class UnmaskUnit(UnmaskCode):

    adapts(Unit)

    def __init__(self, unit, state):
        super(UnmaskUnit, self).__init__(unit, state)
        self.unit = unit

    def __call__(self):
        flow = self.state.unmask(self.unit.flow)
        return self.unit.clone(flow=flow)


class CollectInUnit(CollectInCode):

    adapts(Unit)

    def __init__(self, unit, state):
        super(CollectInUnit, self).__init__(unit, state)
        self.unit = unit

    def __call__(self):
        self.state.collect(self.unit.flow)
        self.state.collection.append(self.unit)


class ReplaceInUnit(ReplaceInCode):

    adapts(Unit)

    def __init__(self, unit, state):
        super(ReplaceInUnit, self).__init__(unit, state)
        self.unit = unit

    def __call__(self):
        flow = self.state.replace(self.unit.flow)
        return self.unit.clone(flow=flow)


class RewriteCompound(RewriteUnit):

    adapts(CompoundUnit)

    def __call__(self):
        code = self.state.rewrite(self.unit.code)
        flow = self.state.rewrite(self.unit.flow)
        return self.unit.clone(code=code, flow=flow)


class ReplaceInCompound(ReplaceInUnit):

    adapts(CompoundUnit)

    def __call__(self):
        flow = self.state.replace(self.unit.flow)
        self.state.save_collection()
        self.state.collect(self.unit.code)
        self.state.recombine()
        code = self.state.replace(self.unit.code)
        self.state.restore_collection()
        return self.unit.clone(flow=flow, code=code)


class UnmaskScalar(UnmaskUnit):

    adapts(ScalarUnit)

    def __call__(self):
        if self.unit.flow.dominates(self.state.mask):
            code = self.state.unmask(self.unit.code)
            return code
        else:
            code = self.state.unmask(self.unit.code, mask=self.unit.flow)
        flow = self.state.unmask(self.unit.flow)
        return self.unit.clone(flow=flow, code=code)


class RewriteAggregate(RewriteUnit):

    adapts(AggregateUnitBase)

    def __call__(self):
        code = self.state.rewrite(self.unit.code)
        flow = self.state.rewrite(self.unit.flow)
        plural_flow = self.state.rewrite(self.unit.plural_flow)
        return self.unit.clone(code=code, flow=flow, plural_flow=plural_flow)


class UnmaskAggregate(UnmaskUnit):

    adapts(AggregateUnitBase)

    def __call__(self):
        code = self.state.unmask(self.unit.code, mask=self.unit.plural_flow)
        if self.unit.flow.dominates(self.state.mask):
            plural_flow = self.state.unmask(self.unit.plural_flow)
        else:
            plural_flow = self.state.unmask(self.unit.plural_flow,
                                            mask=self.unit.flow)
        flow = self.state.unmask(self.unit.flow)
        return self.unit.clone(code=code, flow=flow, plural_flow=plural_flow)


class ReplaceInAggregate(ReplaceInUnit):

    adapts(AggregateUnitBase)

    def __call__(self):
        flow = self.state.replace(self.unit.flow)
        self.state.save_collection()
        self.state.collect(self.unit.code)
        self.state.collect(self.unit.plural_flow)
        self.state.recombine()
        code = self.state.replace(self.unit.code)
        plural_flow = self.state.replace(self.unit.plural_flow)
        self.state.restore_collection()
        return self.unit.clone(code=code, flow=flow, plural_flow=plural_flow)


class UnmaskKernel(UnmaskUnit):

    adapts(KernelUnit)

    def __call__(self):
        code = self.state.unmask(self.unit.code,
                                 mask=self.unit.flow.family.seed)
        flow = self.state.unmask(self.unit.flow)
        return self.unit.clone(flow=flow, code=code)


def rewrite(expression, state=None):
    if state is None:
        state = RewritingState()
    rewrite = Rewrite(expression, state)
    expression = rewrite()
    return expression


