#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.rewrite`
=======================

This module implements the rewriting process.
"""


from ..adapter import Adapter, adapts
from ..domain import BooleanDomain
from .code import (Expression, QueryExpr, SegmentExpr, Space, ScalarSpace,
                   QuotientSpace, FilteredSpace, OrderedSpace,
                   Code, LiteralCode, CastCode, FormulaCode, Unit, ScalarUnit,
                   AggregateUnitBase, KernelUnit, ComplementUnit)
from .signature import Signature


class RewritingState(object):

    def __init__(self):
        self.scalar = None
        self.mask = None
        self.mask_stack = []

    def set_scalar(self, space):
        assert isinstance(space, ScalarSpace)
        assert self.scalar is None
        assert self.mask is None
        self.scalar = space
        self.mask = space

    def flush(self):
        assert self.scalar is not None
        assert self.mask is self.scalar
        assert not self.mask_stack
        self.scalar = None
        self.mask = None

    def push_mask(self, mask):
        assert isinstance(mask, Space)
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
        self.state.set_scalar(self.expression.space.scalar)
        elements = [self.state.rewrite(element, mask=self.expression.space)
                    for element in self.expression.elements]
        space = self.state.rewrite(self.expression.space)
        self.state.flush()
        return self.expression.clone(space=space, elements=elements)


class RewriteSpace(Rewrite):

    adapts(Space)

    def __init__(self, space, state):
        super(RewriteSpace, self).__init__(space, state)
        self.space = space

    def __call__(self):
        if self.space.base is None:
            return self.space
        base = self.state.rewrite(self.space.base)
        return self.space.clone(base=base)


class RewriteQuotient(RewriteSpace):

    adapts(QuotientSpace)

    def __call__(self):
        kernel = [self.state.rewrite(code, mask=self.space.family.seed)
                  for code in self.space.family.kernel]
        seed = self.state.rewrite(self.space.family.seed, mask=self.space.base)
        base = self.state.rewrite(self.space.base)
        return self.space.clone(base=base, seed=seed, kernel=kernel)


class RewriteFiltered(RewriteSpace):

    adapts(FilteredSpace)

    def __call__(self):
        if (self.space.prune(self.state.mask)
                == self.space.base.prune(self.state.mask)):
            return self.state.rewrite(self.space.base)
        if self.space.base.dominates(self.state.mask):
            filter = self.state.rewrite(self.space.filter)
        else:
            filter = self.state.rewrite(self.space.filter,
                                        mask=self.space.base)
        base = self.state.rewrite(self.space.base)
        if (isinstance(filter, LiteralCode) and
            isinstance(filter.domain, BooleanDomain) and
            filter.value is True):
            return base
        return self.space.clone(base=base, filter=filter)


class RewriteOrdered(RewriteSpace):

    adapts(OrderedSpace)

    def __call__(self):
        if (self.space.prune(self.state.mask)
                == self.space.base.prune(self.state.mask)):
            return self.state.rewrite(self.space.base)
        if self.space.base.dominates(self.state.mask):
            order = [(self.state.rewrite(code), direction)
                     for code, direction in self.space.order]
        else:
            order = [(self.state.rewrite(code, mask=self.space.base),
                      direction)
                     for code, direction in self.space.order]
        if self.space.is_expanding:
            base = self.state.rewrite(self.space.base)
        else:
            base = self.state.rewrite(self.space.base, mask=self.space.scalar)
        return self.space.clone(base=base, order=order)


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
        space = self.state.rewrite(self.unit.space)
        return self.unit.clone(space=space)


class RewriteScalar(RewriteUnit):

    adapts(ScalarUnit)

    def __call__(self):
        if self.unit.space.dominates(self.state.mask):
            code = self.state.rewrite(self.unit.code)
        else:
            code = self.state.rewrite(self.unit.code, mask=self.unit.space)
        space = self.state.rewrite(self.unit.space)
        return self.unit.clone(space=space, code=code)


class RewriteAggregate(RewriteUnit):

    adapts(AggregateUnitBase)

    def __call__(self):
        code = self.state.rewrite(self.unit.code, mask=self.unit.plural_space)
        if self.unit.space.dominates(self.state.mask):
            plural_space = self.state.rewrite(self.unit.plural_space)
        else:
            plural_space = self.state.rewrite(self.unit.plural_space,
                                              mask=self.unit.space)
        space = self.state.rewrite(self.unit.space)
        return self.unit.clone(space=space, plural_space=plural_space,
                               code=code)


class RewriteKernel(RewriteUnit):

    adapts(KernelUnit)

    def __call__(self):
        code = self.state.rewrite(self.unit.code,
                                  mask=self.unit.space.family.seed)
        space = self.state.rewrite(self.unit.space)
        return self.unit.clone(space=space, code=code)


class RewriteComplement(RewriteUnit):

    adapts(ComplementUnit)

    def __call__(self):
        code = self.state.rewrite(self.unit.code,
                                  mask=self.unit.space.base.family.seed)
        space = self.state.rewrite(self.unit.space)
        return self.unit.clone(space=space, code=code)


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


