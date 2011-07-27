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
from .flow import (Expression, QueryExpr, SegmentExpr, Flow, RootFlow,
                   QuotientFlow, MonikerFlow, ForkedFlow, LinkedFlow,
                   FilteredFlow, OrderedFlow,
                   Code, LiteralCode, CastCode, FormulaCode, Unit, ScalarUnit,
                   AggregateUnitBase, KernelUnit, ComplementUnit,
                   MonikerUnit, ForkedUnit, LinkedUnit)
from .signature import Signature


class RewritingState(object):

    def __init__(self):
        self.root = None
        self.mask = None
        self.mask_stack = []

    def set_root(self, flow):
        assert isinstance(flow, RootFlow)
        assert self.root is None
        assert self.mask is None
        self.root = flow
        self.mask = flow

    def flush(self):
        assert self.root is not None
        assert self.mask is self.root
        assert not self.mask_stack
        self.root = None
        self.mask = None

    def push_mask(self, mask):
        assert isinstance(mask, Flow)
        self.mask_stack.append(self.mask)
        self.mask = mask

    def pop_mask(self):
        self.mask = self.mask_stack.pop()

    def rewrite(self, expression, mask=None):
        return rewrite(expression, self, mask=mask)


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
        elements = [self.state.rewrite(element, mask=self.expression.flow)
                    for element in self.expression.elements]
        flow = self.state.rewrite(self.expression.flow)
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


class RewriteQuotient(RewriteFlow):

    adapts(QuotientFlow)

    def __call__(self):
        kernel = [self.state.rewrite(code, mask=self.flow.family.seed)
                  for code in self.flow.family.kernel]
        seed = self.state.rewrite(self.flow.family.seed, mask=self.flow.base)
        base = self.state.rewrite(self.flow.base)
        return self.flow.clone(base=base, seed=seed, kernel=kernel)


class RewriteMoniker(RewriteFlow):

    adapts(MonikerFlow)

    def __call__(self):
        seed = self.state.rewrite(self.flow.seed, mask=self.flow.base)
        base = self.state.rewrite(self.flow.base)
        return self.flow.clone(base=base, seed=seed)


class RewriteForked(RewriteFlow):

    adapts(ForkedFlow)

    def __call__(self):
        seed_mask = self.flow.base
        while not seed_mask.is_axis:
            seed_mask = seed_mask.base
        seed = self.state.rewrite(self.flow.seed, mask=seed_mask)
        kernel = [self.state.rewrite(code, mask=self.flow.base)
                  for code in self.flow.kernel]
        base = self.state.rewrite(self.flow.base)
        return self.flow.clone(base=base, seed=seed, kernel=kernel)


class RewriteLinked(RewriteFlow):

    adapts(LinkedFlow)

    def __call__(self):
        base = self.state.rewrite(self.flow.base)
        seed = self.state.rewrite(self.flow.seed, mask=self.flow.base)
        kernel = [self.state.rewrite(code, mask=self.flow.seed)
                  for code in self.flow.kernel]
        counter_kernel = [self.state.rewrite(code, mask=self.flow.base)
                          for code in self.flow.counter_kernel]
        return self.flow.clone(base=base, seed=seed, kernel=kernel,
                                counter_kernel=counter_kernel)


class RewriteFiltered(RewriteFlow):

    adapts(FilteredFlow)

    def __call__(self):
        if (self.flow.prune(self.state.mask)
                == self.flow.base.prune(self.state.mask)):
            return self.state.rewrite(self.flow.base)
        if self.flow.base.dominates(self.state.mask):
            filter = self.state.rewrite(self.flow.filter)
        else:
            filter = self.state.rewrite(self.flow.filter,
                                        mask=self.flow.base)
        base = self.state.rewrite(self.flow.base)
        if (isinstance(filter, LiteralCode) and
            isinstance(filter.domain, BooleanDomain) and
            filter.value is True):
            return base
        return self.flow.clone(base=base, filter=filter)


class RewriteOrdered(RewriteFlow):

    adapts(OrderedFlow)

    def __call__(self):
        if (self.flow.prune(self.state.mask)
                == self.flow.base.prune(self.state.mask)):
            return self.state.rewrite(self.flow.base)
        if self.flow.base.dominates(self.state.mask):
            order = [(self.state.rewrite(code), direction)
                     for code, direction in self.flow.order]
        else:
            order = [(self.state.rewrite(code, mask=self.flow.base),
                      direction)
                     for code, direction in self.flow.order]
        if self.flow.is_expanding:
            base = self.state.rewrite(self.flow.base)
        else:
            base = self.state.rewrite(self.flow.base, mask=self.flow.root)
        return self.flow.clone(base=base, order=order)


class RewriteCode(Rewrite):

    adapts(Code)

    def __init__(self, code, state):
        super(RewriteCode, self).__init__(code, state)
        self.code = code

    def __call__(self):
        return self.code


class RewriteCast(RewriteCode):

    adapts(CastCode)

    def __call__(self):
        base = self.state.rewrite(self.code.base)
        return self.code.clone(base=base)


class RewriteFormula(RewriteCode):

    adapts(FormulaCode)

    def __call__(self):
        rewrite = RewriteBySignature(self.code, self.state)
        return rewrite()


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


class RewriteScalar(RewriteUnit):

    adapts(ScalarUnit)

    def __call__(self):
        if self.unit.flow.dominates(self.state.mask):
            code = self.state.rewrite(self.unit.code)
        else:
            code = self.state.rewrite(self.unit.code, mask=self.unit.flow)
        flow = self.state.rewrite(self.unit.flow)
        return self.unit.clone(flow=flow, code=code)


class RewriteAggregate(RewriteUnit):

    adapts(AggregateUnitBase)

    def __call__(self):
        code = self.state.rewrite(self.unit.code, mask=self.unit.plural_flow)
        if self.unit.flow.dominates(self.state.mask):
            plural_flow = self.state.rewrite(self.unit.plural_flow)
        else:
            plural_flow = self.state.rewrite(self.unit.plural_flow,
                                              mask=self.unit.flow)
        flow = self.state.rewrite(self.unit.flow)
        return self.unit.clone(flow=flow, plural_flow=plural_flow,
                               code=code)


class RewriteKernel(RewriteUnit):

    adapts(KernelUnit)

    def __call__(self):
        code = self.state.rewrite(self.unit.code,
                                  mask=self.unit.flow.family.seed)
        flow = self.state.rewrite(self.unit.flow)
        return self.unit.clone(flow=flow, code=code)


class RewriteComplement(RewriteUnit):

    adapts(ComplementUnit)

    def __call__(self):
        code = self.state.rewrite(self.unit.code,
                                  mask=self.unit.flow.base.family.seed)
        flow = self.state.rewrite(self.unit.flow)
        return self.unit.clone(flow=flow, code=code)


class RewriteMonikerUnit(RewriteUnit):

    adapts(MonikerUnit)

    def __call__(self):
        code = self.state.rewrite(self.unit.code,
                                  mask=self.unit.flow.seed)
        flow = self.state.rewrite(self.unit.flow)
        return self.unit.clone(flow=flow, code=code)


class RewriteForkedUnit(RewriteUnit):

    adapts(ForkedUnit)

    def __call__(self):
        code = self.state.rewrite(self.unit.code,
                                  mask=self.unit.flow.base)
        flow = self.state.rewrite(self.unit.flow)
        return self.unit.clone(flow=flow, code=code)


class RewriteLinkedUnit(RewriteUnit):

    adapts(LinkedUnit)

    def __call__(self):
        code = self.state.rewrite(self.unit.code,
                                  mask=self.unit.flow.seed)
        flow = self.state.rewrite(self.unit.flow)
        return self.unit.clone(flow=flow, code=code)


def rewrite(expression, state=None, mask=None):
    if state is None:
        state = RewritingState()
    if mask is not None:
        state.push_mask(mask)
    rewrite = Rewrite(expression, state)
    expression = rewrite()
    if mask is not None:
        state.pop_mask()
    return expression


