#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql.core.tr.fn.encode`
==============================
"""


from ...adapter import Adapter, adapt, adapt_many, adapt_none
from ...domain import UntypedDomain, BooleanDomain, IntegerDomain
from ..encode import EncodeBySignature, EncodingState
from ..error import EncodeError
from ..coerce import coerce
from ..binding import RootBinding, LiteralBinding, CastBinding
from ..flow import (LiteralCode, ScalarUnit, CorrelatedUnit,
                    AggregateUnit, FilteredFlow, FormulaCode)
from ..signature import Signature, NotSig, NullIfSig, IfNullSig, CompareSig
from .signature import (ExistsSig, AggregateSig, QuantifySig,
                        CountSig, SumSig, ReplaceSig, ConcatenateSig,
                        LikeSig, ContainsSig, HeadSig, TailSig, SliceSig, AtSig,
                        SubstringSig, LengthSig, AddSig, SubtractSig,
                        IfSig, ReversePolaritySig)


class EncodeFunction(EncodeBySignature):

    adapt_none()


class EncodeLength(EncodeFunction):

    adapt(LengthSig)

    def __call__(self):
        code = super(EncodeLength, self).__call__()
        zero = LiteralCode(0, code.domain, code.binding)
        return FormulaCode(IfNullSig(), code.domain, code.binding,
                           lop=code, rop=zero)


class EncodeContains(EncodeFunction):

    adapt(ContainsSig)

    def __call__(self):
        lop = self.state.encode(self.binding.lop)
        rop = self.state.encode(self.binding.rop)
        if isinstance(rop, LiteralCode):
            if rop.value is not None:
                value = (u"%" + rop.value.replace(u"\\", u"\\\\")
                                         .replace(u"%", u"\\%")
                                         .replace(u"_", u"\\_") + u"%")
                rop = rop.clone(value=value)
        else:
            backslash_literal = LiteralCode(u"\\", rop.domain, self.binding)
            xbackslash_literal = LiteralCode(u"\\\\", rop.domain, self.binding)
            percent_literal = LiteralCode(u"%", rop.domain, self.binding)
            xpercent_literal = LiteralCode(u"\\%", rop.domain, self.binding)
            underscore_literal = LiteralCode(u"_", rop.domain, self.binding)
            xunderscore_literal = LiteralCode(u"\\_", rop.domain, self.binding)
            rop = FormulaCode(ReplaceSig(), rop.domain, self.binding,
                              op=rop, old=backslash_literal,
                              new=xbackslash_literal)
            rop = FormulaCode(ReplaceSig(), rop.domain, self.binding,
                              op=rop, old=percent_literal,
                              new=xpercent_literal)
            rop = FormulaCode(ReplaceSig(), rop.domain, self.binding,
                              op=rop, old=underscore_literal,
                              new=xunderscore_literal)
            rop = FormulaCode(ConcatenateSig(), rop.domain, self.binding,
                              lop=percent_literal, rop=rop)
            rop = FormulaCode(ConcatenateSig(), rop.domain, self.binding,
                              lop=rop, rop=percent_literal)
        return FormulaCode(self.signature.clone_to(LikeSig),
                           self.domain, self.binding, lop=lop, rop=rop)


class EncodeHead(EncodeFunction):

    adapt(HeadSig)

    def __call__(self):
        op = self.state.encode(self.binding.op)
        op_length = FormulaCode(LengthSig(), coerce(IntegerDomain()),
                                self.binding, op=op)
        zero_literal = LiteralCode(0, coerce(IntegerDomain()), self.binding)
        one_literal = LiteralCode(1, coerce(IntegerDomain()), self.binding)
        if self.binding.length is None:
            length = one_literal
        else:
            length = self.state.encode(self.binding.length)
        if isinstance(length, LiteralCode):
            if length.value is None:
                length = one_literal
            if length.value >= 0:
                return FormulaCode(SubstringSig(), self.binding.domain,
                                   self.binding, op=op, start=one_literal,
                                   length=length)
        length = FormulaCode(IfNullSig(), length.domain, self.binding,
                             lop=length, rop=one_literal)
        negative_length = FormulaCode(AddSig(), length.domain, self.binding,
                                      lop=op_length, rop=length)
        if_positive = FormulaCode(CompareSig('>='), coerce(BooleanDomain()),
                                  self.binding, lop=length, rop=zero_literal)
        if_negative = FormulaCode(CompareSig('>='), coerce(BooleanDomain()),
                                  self.binding, lop=negative_length,
                                  rop=zero_literal)
        length = FormulaCode(IfSig(), length.domain, self.binding,
                             predicates=[if_positive, if_negative],
                             consequents=[length, negative_length],
                             alternative=zero_literal)
        return FormulaCode(SubstringSig(), self.binding.domain, self.binding,
                           op=op, start=one_literal, length=length)


class EncodeTail(EncodeFunction):

    adapt(TailSig)

    def __call__(self):
        op = self.state.encode(self.binding.op)
        op_length = FormulaCode(LengthSig(), coerce(IntegerDomain()),
                                self.binding, op=op)
        zero_literal = LiteralCode(0, coerce(IntegerDomain()), self.binding)
        one_literal = LiteralCode(1, coerce(IntegerDomain()), self.binding)
        if self.binding.length is None:
            length = one_literal
        else:
            length = self.state.encode(self.binding.length)
        if isinstance(length, LiteralCode):
            if length.value is None:
                length = one_literal
            if length.value < 0:
                start = length.clone(value=1-length.value)
                return FormulaCode(SubstringSig(), self.binding.domain,
                                   self.binding, op=op,
                                   start=start, length=None)
        length = FormulaCode(IfNullSig(), length.domain, self.binding,
                             lop=length, rop=one_literal)
        start = FormulaCode(SubtractSig(), length.domain, self.binding,
                            lop=one_literal, rop=length)
        positive_start = FormulaCode(AddSig(), length.domain, self.binding,
                                     lop=op_length, rop=start)
        if_negative = FormulaCode(CompareSig('<'), coerce(BooleanDomain()),
                                  self.binding, lop=length, rop=zero_literal)
        if_positive = FormulaCode(CompareSig('<='), coerce(BooleanDomain()),
                                  self.binding, lop=length, rop=op_length)
        start = FormulaCode(IfSig(), length.domain, self.binding,
                            predicates=[if_negative, if_positive],
                            consequents=[start, positive_start],
                            alternative=one_literal)
        return FormulaCode(SubstringSig(), self.binding.domain, self.binding,
                           op=op, start=start, length=None)


class EncodeSlice(EncodeFunction):

    adapt(SliceSig)

    def __call__(self):
        op = self.state.encode(self.binding.op)
        op_length = FormulaCode(LengthSig(), coerce(IntegerDomain()),
                                self.binding, op=op)
        null_literal = LiteralCode(None, coerce(IntegerDomain()), self.binding)
        zero_literal = LiteralCode(0, coerce(IntegerDomain()), self.binding)
        one_literal = LiteralCode(1, coerce(IntegerDomain()), self.binding)
        if self.binding.left is None:
            left = zero_literal
        else:
            left = self.state.encode(self.binding.left)
        if self.binding.right is None:
            right = null_literal
        else:
            right = self.state.encode(self.binding.right)
        if isinstance(left, LiteralCode) and left.value is None:
            start = one_literal
        elif isinstance(left, LiteralCode) and left.value >= 0:
            start = left.clone(value=left.value+1)
        else:
            left = FormulaCode(IfNullSig(), left.domain, self.binding,
                               lop=left, rop=zero_literal)
            start = left
            negative_start = FormulaCode(AddSig(), left.domain, self.binding,
                                         lop=left, rop=op_length)
            if_positive = FormulaCode(CompareSig('>='), coerce(BooleanDomain()),
                                      self.binding, lop=start,
                                      rop=zero_literal)
            if_negative = FormulaCode(CompareSig('>='), coerce(BooleanDomain()),
                                      self.binding, lop=negative_start,
                                      rop=zero_literal)
            start = FormulaCode(IfSig(), left.domain, self.binding,
                                predicates=[if_positive, if_negative],
                                consequents=[start, negative_start],
                                alternative=zero_literal)
            start = FormulaCode(AddSig(), left.domain, self.binding,
                                lop=start, rop=one_literal)
        if isinstance(right, LiteralCode) and right.value is None:
            return FormulaCode(SubstringSig(), self.binding.domain,
                               self.binding, op=op,
                               start=start, length=None)
        elif isinstance(right, LiteralCode) and right.value >= 0:
            if isinstance(start, LiteralCode):
                assert start.value >= 0
                value = right.value-start.value+1
                if value < 0:
                    value = 0
                length = right.clone(value=value)
                return FormulaCode(SubstringSig(), self.binding.domain,
                                   self.binding, op=op,
                                   start=start, length=length)
            end = right.clone(value=right.value+1)
        else:
            if not isinstance(right, LiteralCode):
                right = FormulaCode(IfNullSig(), right.domain, self.binding,
                                    lop=right, rop=op_length)
            end = right
            negative_end = FormulaCode(AddSig(), right.domain, self.binding,
                                       lop=right, rop=op_length)
            if_positive = FormulaCode(CompareSig('>='), coerce(BooleanDomain()),
                                      self.binding, lop=end,
                                      rop=zero_literal)
            if_negative = FormulaCode(CompareSig('>='), coerce(BooleanDomain()),
                                      self.binding, lop=negative_end,
                                      rop=zero_literal)
            end = FormulaCode(IfSig(), right.domain, self.binding,
                                predicates=[if_positive, if_negative],
                                consequents=[end, negative_end],
                                alternative=zero_literal)
            end = FormulaCode(AddSig(), right.domain, self.binding,
                                lop=end, rop=one_literal)
        length = FormulaCode(SubtractSig(), coerce(IntegerDomain()),
                             self.binding, lop=end, rop=start)
        condition = FormulaCode(CompareSig('<'), coerce(BooleanDomain()),
                                self.binding, lop=start, rop=end)
        length = FormulaCode(IfSig(), length.domain, self.binding,
                             predicates=[condition],
                             consequents=[length],
                             alternative=zero_literal)
        return FormulaCode(SubstringSig(), self.binding.domain, self.binding,
                           op=op, start=start, length=length)


class EncodeAt(EncodeFunction):

    adapt(AtSig)

    def __call__(self):
        op = self.state.encode(self.binding.op)
        op_length = FormulaCode(LengthSig(), coerce(IntegerDomain()),
                                self.binding, op=op)
        zero_literal = LiteralCode(0, coerce(IntegerDomain()), self.binding)
        one_literal = LiteralCode(1, coerce(IntegerDomain()), self.binding)
        index = self.state.encode(self.binding.index)
        if self.binding.length is not None:
            length = self.state.encode(self.binding.length)
        else:
            length = one_literal
        if isinstance(index, LiteralCode) and index.value is None:
            return FormulaCode(SubstringSig(), self.binding.domain,
                               self.binding, op=op, start=index,
                               length=zero_literal)
        if isinstance(length, LiteralCode) and length.value is None:
            length = one_literal
        if (isinstance(index, LiteralCode) and index.value >= 0
                and isinstance(length, LiteralCode)):
            index_value = index.value
            length_value = length.value
            if length_value < 0:
                index_value += length_value
                length_value = -length_value
            if index_value < 0:
                length_value += index_value
                index_value = 0
            if length_value < 0:
                length_value = 0
            start = index.clone(value=index_value+1)
            length = length.clone(value=length_value)
            return FormulaCode(SubstringSig(), self.binding.domain,
                               self.binding, op=op, start=start, length=length)
        length = FormulaCode(IfNullSig(), length.domain, self.binding,
                             lop=length, rop=one_literal)
        negative_index = FormulaCode(AddSig(), index.domain, self.binding,
                                     lop=index, rop=op_length)
        condition = FormulaCode(CompareSig('>='), coerce(BooleanDomain()),
                                self.binding, lop=index, rop=zero_literal)
        index = FormulaCode(IfSig(), index.domain, self.binding,
                            predicates=[condition], consequents=[index],
                            alternative=negative_index)
        condition = FormulaCode(CompareSig('>='), coerce(BooleanDomain()),
                                self.binding, lop=length, rop=zero_literal)
        negative_index = FormulaCode(AddSig(), index.domain, self.binding,
                                     lop=index, rop=length)
        negative_length = FormulaCode(ReversePolaritySig(), length.domain,
                                      self.binding, op=length)
        index = FormulaCode(IfSig(), index.domain, self.binding,
                            predicates=[condition], consequents=[index],
                            alternative=negative_index)
        length = FormulaCode(IfSig(), length.domain, self.binding,
                             predicates=[condition], consequents=[length],
                             alternative=negative_length)
        condition = FormulaCode(CompareSig('>='), coerce(BooleanDomain()),
                                self.binding, lop=index, rop=zero_literal)
        negative_length = FormulaCode(AddSig(), length.domain, self.binding,
                                      lop=length, rop=index)
        index = FormulaCode(IfSig(), index.domain, self.binding,
                            predicates=[condition], consequents=[index],
                            alternative=zero_literal)
        length = FormulaCode(IfSig(), length.domain, self.binding,
                             predicates=[condition], consequents=[length],
                             alternative=negative_length)
        condition = FormulaCode(CompareSig('>='), coerce(BooleanDomain()),
                                self.binding, lop=length, rop=zero_literal)
        length = FormulaCode(IfSig(), length.domain, self.binding,
                             predicates=[condition], consequents=[length],
                             alternative=zero_literal)
        start = FormulaCode(AddSig(), index.domain, self.binding,
                            lop=index, rop=one_literal)
        return FormulaCode(SubstringSig(), self.binding.domain,
                           self.binding, op=op, start=start, length=length)


class EncodeReplace(EncodeFunction):

    adapt(ReplaceSig)

    def __call__(self):
        op = self.state.encode(self.binding.op)
        old = self.state.encode(self.binding.old)
        new = self.state.encode(self.binding.new)
        empty = LiteralCode(u'', old.domain, self.binding)
        old = FormulaCode(IfNullSig(), old.domain, self.binding,
                          lop=old, rop=empty)
        new = FormulaCode(IfNullSig(), old.domain, self.binding,
                          lop=new, rop=empty)
        return FormulaCode(self.signature, self.domain, self.binding,
                           op=op, old=old, new=new)


class EncodeAggregate(EncodeFunction):

    adapt(AggregateSig)

    def aggregate(self, op, flow, plural_flow):
        if plural_flow is None:
            plural_units = [unit for unit in op.units
                                 if not flow.spans(unit.flow)]
            if not plural_units:
                raise EncodeError("a plural operand is expected", op.mark)
            plural_flows = []
            for unit in plural_units:
                if any(plural_flow.dominates(unit.flow)
                       for plural_flow in plural_flows):
                    continue
                plural_flows = [plural_flow
                                 for plural_flow in plural_flows
                                 if not unit.flow.dominates(plural_flow)]
                plural_flows.append(unit.flow)
            if len(plural_flows) > 1:
                raise EncodeError("cannot deduce an unambiguous"
                                  " aggregate flow", op.mark)
            [plural_flow] = plural_flows
        if flow.spans(plural_flow):
            raise EncodeError("a plural operand is expected", op.mark)
        if not plural_flow.spans(flow):
            raise EncodeError("a descendant operand is expected",
                              op.mark)
        # FIXME: handled by the compiler.
        #if not all(plural_flow.spans(unit.flow)
        #           for unit in plural_units):
        #    raise EncodeError("a descendant operand is expected",
        #                      op.mark)
        return plural_flow

    def __call__(self):
        op = self.state.encode(self.binding.op)
        flow = self.state.relate(self.binding.base)
        plural_flow = None
        if self.binding.plural_base is not None:
            plural_flow = self.state.relate(self.binding.plural_base)
        plural_flow = self.aggregate(op, flow, plural_flow)
        aggregate = AggregateUnit(op, plural_flow, flow, self.binding)
        wrapper = WrapAggregate.__invoke__(aggregate, self.state)
        wrapper = ScalarUnit(wrapper, flow, self.binding)
        return wrapper


class WrapAggregate(Adapter):

    adapt(Signature)

    @classmethod
    def __dispatch__(interface, unit, *args, **kwds):
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


class EncodeCount(EncodeFunction):

    adapt(CountSig)

    def __call__(self):
        op = self.state.encode(self.binding.op)
        false_literal = LiteralCode(False, op.domain, op.binding)
        op = FormulaCode(NullIfSig(), op.domain, op.binding,
                         lop=op, rop=false_literal)
        return FormulaCode(CountSig(), self.binding.domain, self.binding,
                           op=op)


class WrapCountSum(WrapAggregate):

    adapt_many(CountSig, SumSig)

    def __call__(self):
        root = RootBinding(self.unit.syntax)
        zero_literal = LiteralBinding(root, u'0', UntypedDomain(),
                                      self.unit.syntax)
        zero_literal = CastBinding(zero_literal, self.unit.domain,
                                   self.unit.syntax)
        zero_literal = self.state.encode(zero_literal)
        return FormulaCode(IfNullSig(), self.unit.domain, self.unit.binding,
                           lop=self.unit, rop=zero_literal)


class EncodeQuantify(EncodeAggregate):

    adapt(QuantifySig)

    def __call__(self):
        op = self.state.encode(self.binding.op)
        flow = self.state.relate(self.binding.base)
        plural_flow = None
        if self.binding.plural_base is not None:
            plural_flow = self.state.relate(self.binding.plural_base)
        plural_flow = self.aggregate(op, flow, plural_flow)
        if self.signature.polarity < 0:
            op = FormulaCode(NotSig(), op.domain, op.binding, op=op)
        plural_flow = FilteredFlow(plural_flow, op, self.binding)
        true_literal = LiteralCode(True, coerce(BooleanDomain()), self.binding)
        aggregate = CorrelatedUnit(true_literal, plural_flow, flow,
                                   self.binding)
        wrapper = FormulaCode(ExistsSig(), op.domain, self.binding,
                              op=aggregate)
        if self.signature.polarity < 0:
            wrapper = FormulaCode(NotSig(), wrapper.domain, wrapper.binding,
                                  op=wrapper)
        wrapper = ScalarUnit(wrapper, flow, self.binding)
        return wrapper


