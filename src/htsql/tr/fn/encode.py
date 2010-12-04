#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.fn.encode`
=========================
"""


from ...adapter import Adapter, adapts, adapts_many
from ...domain import UntypedDomain, BooleanDomain
from ..encode import EncodeBySignature, EncodingState
from ..error import EncodeError
from ..coerce import coerce
from ..binding import LiteralBinding, CastBinding
from ..code import (LiteralCode, ScalarUnit, CorrelatedUnit,
                    AggregateUnit, FilteredSpace, FormulaCode)
from .signature import (Signature, NotSig, NullIfSig, IfNullSig, QuantifySig,
                        ExistsSig, AggregateSig, QuantifySig,
                        CountSig, SumSig)


class EncodeAggregate(EncodeBySignature):

    adapts(AggregateSig)

    def aggregate(self, op, space):
        plural_units = [unit for unit in op.units
                             if not space.spans(unit.space)]
        if not plural_units:
            raise EncodeError("a plural operand is required", op.mark)
        plural_spaces = []
        for unit in plural_units:
            if any(plural_space.dominates(unit.space)
                   for plural_space in plural_spaces):
                continue
            plural_spaces = [plural_space
                             for plural_space in plural_spaces
                             if not unit.space.dominates(plural_space)]
            plural_spaces.append(unit.space)
        if len(plural_spaces) > 1:
            raise EncodeError("invalid plural operand", op.mark)
        plural_space = plural_spaces[0]
        if not plural_space.spans(space):
            raise EncodeError("invalid plural operand", op.mark)
        return plural_space

    def __call__(self):
        op = self.state.encode(self.binding.op)
        space = self.state.relate(self.binding.base)
        plural_space = self.aggregate(op, space)
        aggregate = AggregateUnit(op, plural_space, space, self.binding)
        wrap = WrapAggregate(aggregate, self.state)
        wrapper = wrap()
        wrapper = ScalarUnit(wrapper, space, self.binding)
        return wrapper


class WrapAggregate(Adapter):

    adapts(Signature)

    @classmethod
    def dispatch(cls, unit, *args, **kwds):
        assert isinstance(unit, AggregateUnit)
        if not isinstance(unit.code, FormulaCode):
            return (Signature,)
        return (type(unit.code.signature),)

    def __init__(self, unit, state):
        assert isinstance(unit, AggregateUnit)
        assert isinstance(state, EncodingState)
        self.unit = unit
        self.state = state
        self.code = unit.code

    def __call__(self):
        return self.unit


class EncodeCount(EncodeBySignature):

    adapts(CountSig)

    def __call__(self):
        op = self.state.encode(self.binding.op)
        false_literal = LiteralCode(False, op.domain, op.binding)
        op = FormulaCode(NullIfSig(), op.domain, op.binding,
                         lop=op, rop=false_literal)
        return FormulaCode(CountSig(), self.binding.domain, self.binding,
                           op=op)


class WrapCountSum(WrapAggregate):

    adapts_many(CountSig, SumSig)

    def __call__(self):
        zero_literal = LiteralBinding('0', UntypedDomain(),
                                      self.unit.syntax)
        zero_literal = CastBinding(zero_literal, self.unit.domain,
                                   self.unit.syntax)
        zero_literal = self.state.encode(zero_literal)
        return FormulaCode(IfNullSig(), self.unit.domain, self.unit.binding,
                           lop=self.unit, rop=zero_literal)


class EncodeQuantify(EncodeAggregate):

    adapts(QuantifySig)

    def __call__(self):
        op = self.state.encode(self.binding.op)
        space = self.state.relate(self.binding.base)
        plural_space = self.aggregate(op, space)
        if self.signature.polarity < 0:
            op = FormulaCode(NotSig(), op.domain, op.binding, op=op)
        plural_space = FilteredSpace(plural_space, op, self.binding)
        true_literal = LiteralCode(True, coerce(BooleanDomain()), self.binding)
        aggregate = CorrelatedUnit(true_literal, plural_space, space,
                                   self.binding)
        wrapper = FormulaCode(ExistsSig(), op.domain, self.binding,
                              op=aggregate)
        if self.signature.polarity < 0:
            wrapper = FormulaCode(NotSig(), wrapper.domain, wrapper.binding,
                                  op=wrapper)
        wrapper = ScalarUnit(wrapper, space, self.binding)
        return wrapper


