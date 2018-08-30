#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


"""
:mod:`htsql.core.tr.fn.encode`
==============================
"""


from ...adapter import Adapter, adapt, adapt_many, adapt_none
from ...domain import UntypedDomain, BooleanDomain, IntegerDomain
from ..encode import EncodeBySignature, EncodingState
from ...error import Error, translate_guard
from ..coerce import coerce
from ..flow import RootFlow, LiteralFlow, CastFlow
from ..space import (LiteralCode, ScalarUnit, CorrelatedUnit, AggregateUnit,
        FilteredSpace, FormulaCode)
from ..signature import Signature, NotSig, NullIfSig, IfNullSig, CompareSig
from .signature import (ExistsSig, AggregateSig, QuantifySig, CountSig, SumSig,
        ReplaceSig, ConcatenateSig, LikeSig, ContainsSig, HasPrefixSig,
        HeadSig, TailSig, SliceSig, AtSig, SubstringSig, LengthSig, AddSig,
        SubtractSig, IfSig, ReversePolaritySig)


class EncodeFunction(EncodeBySignature):

    adapt_none()


class EncodeLength(EncodeFunction):

    adapt(LengthSig)

    def __call__(self):
        code = super(EncodeLength, self).__call__()
        zero = LiteralCode(0, code.domain, code.flow)
        return FormulaCode(IfNullSig(), code.domain, code.flow,
                           lop=code, rop=zero)


class EncodeContains(EncodeFunction):

    adapt(ContainsSig)

    def __call__(self):
        lop = self.state.encode(self.flow.lop)
        rop = self.state.encode(self.flow.rop)
        if isinstance(rop, LiteralCode):
            if rop.value is not None:
                value = ("%" + rop.value.replace("\\", "\\\\")
                                         .replace("%", "\\%")
                                         .replace("_", "\\_") + "%")
                rop = rop.clone(value=value)
        else:
            backslash_literal = LiteralCode("\\", rop.domain, self.flow)
            xbackslash_literal = LiteralCode("\\\\", rop.domain, self.flow)
            percent_literal = LiteralCode("%", rop.domain, self.flow)
            xpercent_literal = LiteralCode("\\%", rop.domain, self.flow)
            underscore_literal = LiteralCode("_", rop.domain, self.flow)
            xunderscore_literal = LiteralCode("\\_", rop.domain, self.flow)
            rop = FormulaCode(ReplaceSig(), rop.domain, self.flow,
                              op=rop, old=backslash_literal,
                              new=xbackslash_literal)
            rop = FormulaCode(ReplaceSig(), rop.domain, self.flow,
                              op=rop, old=percent_literal,
                              new=xpercent_literal)
            rop = FormulaCode(ReplaceSig(), rop.domain, self.flow,
                              op=rop, old=underscore_literal,
                              new=xunderscore_literal)
            rop = FormulaCode(ConcatenateSig(), rop.domain, self.flow,
                              lop=percent_literal, rop=rop)
            rop = FormulaCode(ConcatenateSig(), rop.domain, self.flow,
                              lop=rop, rop=percent_literal)
        return FormulaCode(self.signature.clone_to(LikeSig),
                           self.domain, self.flow, lop=lop, rop=rop)


class EncodeHasPrefix(EncodeFunction):

    adapt(HasPrefixSig)

    def __call__(self):
        lop = self.state.encode(self.flow.lop)
        rop = self.state.encode(self.flow.rop)
        if isinstance(rop, LiteralCode):
            if rop.value is not None:
                value = (rop.value.replace("\\", "\\\\")
                                  .replace("%", "\\%")
                                  .replace("_", "\\_") + "%")
                rop = rop.clone(value=value)
        else:
            backslash_literal = LiteralCode("\\", rop.domain, self.flow)
            xbackslash_literal = LiteralCode("\\\\", rop.domain, self.flow)
            percent_literal = LiteralCode("%", rop.domain, self.flow)
            xpercent_literal = LiteralCode("\\%", rop.domain, self.flow)
            underscore_literal = LiteralCode("_", rop.domain, self.flow)
            xunderscore_literal = LiteralCode("\\_", rop.domain, self.flow)
            rop = FormulaCode(ReplaceSig(), rop.domain, self.flow,
                              op=rop, old=backslash_literal,
                              new=xbackslash_literal)
            rop = FormulaCode(ReplaceSig(), rop.domain, self.flow,
                              op=rop, old=percent_literal,
                              new=xpercent_literal)
            rop = FormulaCode(ReplaceSig(), rop.domain, self.flow,
                              op=rop, old=underscore_literal,
                              new=xunderscore_literal)
            rop = FormulaCode(ConcatenateSig(), rop.domain, self.flow,
                              lop=rop, rop=percent_literal)
        return FormulaCode(LikeSig(polarity=+1, is_case_sensitive=True),
                           self.domain, self.flow, lop=lop, rop=rop)


class EncodeHead(EncodeFunction):

    adapt(HeadSig)

    def __call__(self):
        op = self.state.encode(self.flow.op)
        op_length = FormulaCode(LengthSig(), coerce(IntegerDomain()),
                                self.flow, op=op)
        zero_literal = LiteralCode(0, coerce(IntegerDomain()), self.flow)
        one_literal = LiteralCode(1, coerce(IntegerDomain()), self.flow)
        if self.flow.length is None:
            length = one_literal
        else:
            length = self.state.encode(self.flow.length)
        if isinstance(length, LiteralCode):
            if length.value is None:
                length = one_literal
            if length.value >= 0:
                return FormulaCode(SubstringSig(), self.flow.domain,
                                   self.flow, op=op, start=one_literal,
                                   length=length)
        length = FormulaCode(IfNullSig(), length.domain, self.flow,
                             lop=length, rop=one_literal)
        negative_length = FormulaCode(AddSig(), length.domain, self.flow,
                                      lop=op_length, rop=length)
        if_positive = FormulaCode(CompareSig('>='), coerce(BooleanDomain()),
                                  self.flow, lop=length, rop=zero_literal)
        if_negative = FormulaCode(CompareSig('>='), coerce(BooleanDomain()),
                                  self.flow, lop=negative_length,
                                  rop=zero_literal)
        length = FormulaCode(IfSig(), length.domain, self.flow,
                             predicates=[if_positive, if_negative],
                             consequents=[length, negative_length],
                             alternative=zero_literal)
        return FormulaCode(SubstringSig(), self.flow.domain, self.flow,
                           op=op, start=one_literal, length=length)


class EncodeTail(EncodeFunction):

    adapt(TailSig)

    def __call__(self):
        op = self.state.encode(self.flow.op)
        op_length = FormulaCode(LengthSig(), coerce(IntegerDomain()),
                                self.flow, op=op)
        zero_literal = LiteralCode(0, coerce(IntegerDomain()), self.flow)
        one_literal = LiteralCode(1, coerce(IntegerDomain()), self.flow)
        if self.flow.length is None:
            length = one_literal
        else:
            length = self.state.encode(self.flow.length)
        if isinstance(length, LiteralCode):
            if length.value is None:
                length = one_literal
            if length.value < 0:
                start = length.clone(value=1-length.value)
                return FormulaCode(SubstringSig(), self.flow.domain,
                                   self.flow, op=op,
                                   start=start, length=None)
        length = FormulaCode(IfNullSig(), length.domain, self.flow,
                             lop=length, rop=one_literal)
        start = FormulaCode(SubtractSig(), length.domain, self.flow,
                            lop=one_literal, rop=length)
        positive_start = FormulaCode(AddSig(), length.domain, self.flow,
                                     lop=op_length, rop=start)
        if_negative = FormulaCode(CompareSig('<'), coerce(BooleanDomain()),
                                  self.flow, lop=length, rop=zero_literal)
        if_positive = FormulaCode(CompareSig('<='), coerce(BooleanDomain()),
                                  self.flow, lop=length, rop=op_length)
        start = FormulaCode(IfSig(), length.domain, self.flow,
                            predicates=[if_negative, if_positive],
                            consequents=[start, positive_start],
                            alternative=one_literal)
        return FormulaCode(SubstringSig(), self.flow.domain, self.flow,
                           op=op, start=start, length=None)


class EncodeSlice(EncodeFunction):

    adapt(SliceSig)

    def __call__(self):
        op = self.state.encode(self.flow.op)
        op_length = FormulaCode(LengthSig(), coerce(IntegerDomain()),
                                self.flow, op=op)
        null_literal = LiteralCode(None, coerce(IntegerDomain()), self.flow)
        zero_literal = LiteralCode(0, coerce(IntegerDomain()), self.flow)
        one_literal = LiteralCode(1, coerce(IntegerDomain()), self.flow)
        if self.flow.left is None:
            left = zero_literal
        else:
            left = self.state.encode(self.flow.left)
        if self.flow.right is None:
            right = null_literal
        else:
            right = self.state.encode(self.flow.right)
        if isinstance(left, LiteralCode) and left.value is None:
            start = one_literal
        elif isinstance(left, LiteralCode) and left.value >= 0:
            start = left.clone(value=left.value+1)
        else:
            left = FormulaCode(IfNullSig(), left.domain, self.flow,
                               lop=left, rop=zero_literal)
            start = left
            negative_start = FormulaCode(AddSig(), left.domain, self.flow,
                                         lop=left, rop=op_length)
            if_positive = FormulaCode(CompareSig('>='), coerce(BooleanDomain()),
                                      self.flow, lop=start,
                                      rop=zero_literal)
            if_negative = FormulaCode(CompareSig('>='), coerce(BooleanDomain()),
                                      self.flow, lop=negative_start,
                                      rop=zero_literal)
            start = FormulaCode(IfSig(), left.domain, self.flow,
                                predicates=[if_positive, if_negative],
                                consequents=[start, negative_start],
                                alternative=zero_literal)
            start = FormulaCode(AddSig(), left.domain, self.flow,
                                lop=start, rop=one_literal)
        if isinstance(right, LiteralCode) and right.value is None:
            return FormulaCode(SubstringSig(), self.flow.domain,
                               self.flow, op=op,
                               start=start, length=None)
        elif isinstance(right, LiteralCode) and right.value >= 0:
            if isinstance(start, LiteralCode):
                assert start.value >= 0
                value = right.value-start.value+1
                if value < 0:
                    value = 0
                length = right.clone(value=value)
                return FormulaCode(SubstringSig(), self.flow.domain,
                                   self.flow, op=op,
                                   start=start, length=length)
            end = right.clone(value=right.value+1)
        else:
            if not isinstance(right, LiteralCode):
                right = FormulaCode(IfNullSig(), right.domain, self.flow,
                                    lop=right, rop=op_length)
            end = right
            negative_end = FormulaCode(AddSig(), right.domain, self.flow,
                                       lop=right, rop=op_length)
            if_positive = FormulaCode(CompareSig('>='), coerce(BooleanDomain()),
                                      self.flow, lop=end,
                                      rop=zero_literal)
            if_negative = FormulaCode(CompareSig('>='), coerce(BooleanDomain()),
                                      self.flow, lop=negative_end,
                                      rop=zero_literal)
            end = FormulaCode(IfSig(), right.domain, self.flow,
                                predicates=[if_positive, if_negative],
                                consequents=[end, negative_end],
                                alternative=zero_literal)
            end = FormulaCode(AddSig(), right.domain, self.flow,
                                lop=end, rop=one_literal)
        length = FormulaCode(SubtractSig(), coerce(IntegerDomain()),
                             self.flow, lop=end, rop=start)
        condition = FormulaCode(CompareSig('<'), coerce(BooleanDomain()),
                                self.flow, lop=start, rop=end)
        length = FormulaCode(IfSig(), length.domain, self.flow,
                             predicates=[condition],
                             consequents=[length],
                             alternative=zero_literal)
        return FormulaCode(SubstringSig(), self.flow.domain, self.flow,
                           op=op, start=start, length=length)


class EncodeAt(EncodeFunction):

    adapt(AtSig)

    def __call__(self):
        op = self.state.encode(self.flow.op)
        op_length = FormulaCode(LengthSig(), coerce(IntegerDomain()),
                                self.flow, op=op)
        zero_literal = LiteralCode(0, coerce(IntegerDomain()), self.flow)
        one_literal = LiteralCode(1, coerce(IntegerDomain()), self.flow)
        index = self.state.encode(self.flow.index)
        if self.flow.length is not None:
            length = self.state.encode(self.flow.length)
        else:
            length = one_literal
        if isinstance(index, LiteralCode) and index.value is None:
            return FormulaCode(SubstringSig(), self.flow.domain,
                               self.flow, op=op, start=index,
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
            return FormulaCode(SubstringSig(), self.flow.domain,
                               self.flow, op=op, start=start, length=length)
        length = FormulaCode(IfNullSig(), length.domain, self.flow,
                             lop=length, rop=one_literal)
        negative_index = FormulaCode(AddSig(), index.domain, self.flow,
                                     lop=index, rop=op_length)
        condition = FormulaCode(CompareSig('>='), coerce(BooleanDomain()),
                                self.flow, lop=index, rop=zero_literal)
        index = FormulaCode(IfSig(), index.domain, self.flow,
                            predicates=[condition], consequents=[index],
                            alternative=negative_index)
        condition = FormulaCode(CompareSig('>='), coerce(BooleanDomain()),
                                self.flow, lop=length, rop=zero_literal)
        negative_index = FormulaCode(AddSig(), index.domain, self.flow,
                                     lop=index, rop=length)
        negative_length = FormulaCode(ReversePolaritySig(), length.domain,
                                      self.flow, op=length)
        index = FormulaCode(IfSig(), index.domain, self.flow,
                            predicates=[condition], consequents=[index],
                            alternative=negative_index)
        length = FormulaCode(IfSig(), length.domain, self.flow,
                             predicates=[condition], consequents=[length],
                             alternative=negative_length)
        condition = FormulaCode(CompareSig('>='), coerce(BooleanDomain()),
                                self.flow, lop=index, rop=zero_literal)
        negative_length = FormulaCode(AddSig(), length.domain, self.flow,
                                      lop=length, rop=index)
        index = FormulaCode(IfSig(), index.domain, self.flow,
                            predicates=[condition], consequents=[index],
                            alternative=zero_literal)
        length = FormulaCode(IfSig(), length.domain, self.flow,
                             predicates=[condition], consequents=[length],
                             alternative=negative_length)
        condition = FormulaCode(CompareSig('>='), coerce(BooleanDomain()),
                                self.flow, lop=length, rop=zero_literal)
        length = FormulaCode(IfSig(), length.domain, self.flow,
                             predicates=[condition], consequents=[length],
                             alternative=zero_literal)
        start = FormulaCode(AddSig(), index.domain, self.flow,
                            lop=index, rop=one_literal)
        return FormulaCode(SubstringSig(), self.flow.domain,
                           self.flow, op=op, start=start, length=length)


class EncodeReplace(EncodeFunction):

    adapt(ReplaceSig)

    def __call__(self):
        op = self.state.encode(self.flow.op)
        old = self.state.encode(self.flow.old)
        new = self.state.encode(self.flow.new)
        empty = LiteralCode('', old.domain, self.flow)
        old = FormulaCode(IfNullSig(), old.domain, self.flow,
                          lop=old, rop=empty)
        new = FormulaCode(IfNullSig(), old.domain, self.flow,
                          lop=new, rop=empty)
        return FormulaCode(self.signature, self.domain, self.flow,
                           op=op, old=old, new=new)


class EncodeAggregate(EncodeFunction):

    adapt(AggregateSig)

    def aggregate(self, op, space, plural_space):
        with translate_guard(op):
            if plural_space is None:
                plural_units = [unit for unit in op.units
                                     if not space.spans(unit.space)]
                if not plural_units:
                    raise Error("Expected a plural operand")
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
                    raise Error("Cannot deduce an unambiguous"
                                " aggregate flow")
                [plural_space] = plural_spaces
            if space.spans(plural_space):
                raise Error("Expected a plural operand")
            if not plural_space.spans(space):
                raise Error("Expected a descendant operand")
        # FIXME: handled by the compiler.
        #if not all(plural_space.spans(unit.space)
        #           for unit in plural_units):
        #    raise Error("a descendant operand is expected",
        #                op.mark)
        return plural_space

    def __call__(self):
        op = self.state.encode(self.flow.op)
        space = self.state.relate(self.flow.base)
        plural_space = None
        if self.flow.plural_base is not None:
            plural_space = self.state.relate(self.flow.plural_base)
        plural_space = self.aggregate(op, space, plural_space)
        aggregate = AggregateUnit(op, plural_space, space, self.flow)
        wrapper = WrapAggregate.__invoke__(aggregate, self.state)
        wrapper = ScalarUnit(wrapper, space, self.flow)
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
        op = self.state.encode(self.flow.op)
        false_literal = LiteralCode(False, op.domain, op.flow)
        op = FormulaCode(NullIfSig(), op.domain, op.flow,
                         lop=op, rop=false_literal)
        return FormulaCode(CountSig(), self.flow.domain, self.flow,
                           op=op)


class WrapCountSum(WrapAggregate):

    adapt_many(CountSig, SumSig)

    def __call__(self):
        root = RootFlow(self.unit.binding)
        zero_literal = LiteralFlow(root, '0', UntypedDomain(),
                                      self.unit.binding)
        zero_literal = CastFlow(zero_literal, self.unit.domain,
                                   self.unit.binding)
        zero_literal = self.state.encode(zero_literal)
        return FormulaCode(IfNullSig(), self.unit.domain, self.unit.flow,
                           lop=self.unit, rop=zero_literal)


class EncodeQuantify(EncodeAggregate):

    adapt(QuantifySig)

    def __call__(self):
        op = self.state.encode(self.flow.op)
        space = self.state.relate(self.flow.base)
        plural_space = None
        if self.flow.plural_base is not None:
            plural_space = self.state.relate(self.flow.plural_base)
        plural_space = self.aggregate(op, space, plural_space)
        if self.signature.polarity < 0:
            op = FormulaCode(NotSig(), op.domain, op.flow, op=op)
        plural_space = FilteredSpace(plural_space, op, self.flow)
        true_literal = LiteralCode(True, coerce(BooleanDomain()), self.flow)
        aggregate = CorrelatedUnit(true_literal, plural_space, space,
                                   self.flow)
        wrapper = FormulaCode(ExistsSig(), op.domain, self.flow,
                              op=aggregate)
        if self.signature.polarity < 0:
            wrapper = FormulaCode(NotSig(), wrapper.domain, wrapper.flow,
                                  op=wrapper)
        wrapper = ScalarUnit(wrapper, space, self.flow)
        return wrapper


