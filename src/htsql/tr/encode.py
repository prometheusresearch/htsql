#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.encoder`
=======================

This module implements the encoding adapter.
"""


from ..adapter import Adapter, adapts
from ..domain import Domain, UntypedDomain, TupleDomain, BooleanDomain
from .error import EncodeError
from .binding import (Binding, RootBinding, QueryBinding, SegmentBinding,
                      TableBinding, FreeTableBinding, JoinedTableBinding,
                      ColumnBinding, LiteralBinding, SieveBinding,
                      SortBinding, EqualityBinding, TotalEqualityBinding,
                      ConjunctionBinding, DisjunctionBinding,
                      NegationBinding, CastBinding, WrapperBinding,
                      DirectionBinding)
from .code import (ScalarSpace, CrossProductSpace, JoinProductSpace,
                   FilteredSpace, OrderedSpace,
                   QueryExpression, SegmentExpression, LiteralCode,
                   EqualityCode, TotalEqualityCode,
                   ConjunctionCode, DisjunctionCode, NegationCode,
                   CastCode, ColumnUnit)


class EncodingState(object):

    with_cache = True

    def __init__(self):
        self.binding_to_code = {}
        self.binding_to_space = {}

    def encode(self, binding):
        if self.with_cache:
            if binding not in self.binding_to_code:
                code = encode(binding, self)
                self.binding_to_code[binding] = code
            return self.binding_to_code[binding]
        return encode(binding, self)

    def relate(self, binding):
        if self.with_cache:
            if binding not in self.binding_to_space:
                space = relate(binding, self)
                self.binding_to_space[binding] = space
            return self.binding_to_space[binding]
        return relate(binding, self)

    def direct(self, binding):
        return direct(binding, self)


class EncodeBase(Adapter):

    adapts(Binding)

    def __init__(self, binding, state):
        assert isinstance(binding, Binding)
        assert isinstance(state, EncodingState)
        self.binding = binding
        self.state = state


class Encode(EncodeBase):

    def __call__(self):
        raise EncodeError("expected a valid code expression",
                          self.binding.mark)


class Relate(EncodeBase):

    def __call__(self):
        raise EncodeError("expected a valid space expression",
                          self.binding.mark)


class Direct(EncodeBase):

    def __call__(self):
        return None


class EncodeQuery(Encode):

    adapts(QueryBinding)

    def __call__(self):
        segment = None
        if self.binding.segment is not None:
            segment = self.state.encode(self.binding.segment)
        return QueryExpression(segment, self.binding)


class EncodeSegment(Encode):

    adapts(SegmentBinding)

    def __call__(self):
        space = self.state.relate(self.binding.base)
        order = []
        elements = []
        for binding in self.binding.elements:
            element = self.state.encode(binding)
            direction = self.state.direct(binding)
            if direction is not None:
                order.append((element, direction))
            elements.append(element)
        space = OrderedSpace(space, order, None, None, self.binding)
        return SegmentExpression(space, elements, self.binding)


class RelateRoot(Relate):

    adapts(RootBinding)

    def __call__(self):
        return ScalarSpace(None, self.binding)


class RelateFreeTable(Relate):

    adapts(FreeTableBinding)

    def __call__(self):
        base = self.state.relate(self.binding.base)
        return CrossProductSpace(base, self.binding.table, self.binding)


class RelateJoinedTable(Relate):

    adapts(JoinedTableBinding)

    def __call__(self):
        space = self.state.relate(self.binding.base)
        for join in self.binding.joins:
            space = JoinProductSpace(space, join, self.binding)
        return space


class RelateSieve(Relate):

    adapts(SieveBinding)

    def __call__(self):
        space = self.state.relate(self.binding.base)
        filter = self.state.encode(self.binding.filter)
        return FilteredSpace(space, filter, self.binding)


class DirectSieve(Direct):

    adapts(SieveBinding)

    def __call__(self):
        return self.state.direct(self.binding.base)


class RelateSort(Relate):

    adapts(SortBinding)

    def __call__(self):
        space = self.state.relate(self.binding.base)
        order = []
        for binding in self.binding.order:
            code = self.state.encode(binding)
            direction = self.state.direct(binding)
            if direction is None:
                direction = +1
            order.append((code, direction))
        limit = self.binding.limit
        offset = self.binding.offset
        return OrderedSpace(space, order, limit, offset, self.binding)


class EncodeColumn(Encode):

    adapts(ColumnBinding)

    def __call__(self):
        space = self.state.relate(self.binding.base)
        return ColumnUnit(self.binding.column, space, self.binding)


class RelateColumn(Relate):

    adapts(ColumnBinding)

    def __call__(self):
        if self.binding.link is not None:
            return self.state.relate(self.binding.link)
        return super(RelateColumn, self).__call__()


class EncodeLiteral(Encode):

    adapts(LiteralBinding)

    def __call__(self):
        return LiteralCode(self.binding.value, self.binding.domain,
                           self.binding)


class EncodeEquality(Encode):

    adapts(EqualityBinding)

    def __call__(self):
        lop = self.state.encode(self.binding.lop)
        rop = self.state.encode(self.binding.rop)
        return EqualityCode(lop, rop, self.binding)


class EncodeTotalEquality(Encode):

    adapts(TotalEqualityBinding)

    def __call__(self):
        lop = self.state.encode(self.binding.lop)
        rop = self.state.encode(self.binding.rop)
        return TotalEqualityCode(lop, rop, self.binding)


class EncodeConjunction(Encode):

    adapts(ConjunctionBinding)

    def __call__(self):
        ops = [self.state.encode(op) for op in self.binding.ops]
        return ConjunctionCode(ops, self.binding)


class EncodeDisjunction(Encode):

    adapts(DisjunctionBinding)

    def __call__(self):
        ops = [self.state.encode(op) for op in self.binding.ops]
        return DisjunctionCode(ops, self.binding)


class EncodeNegation(Encode):

    adapts(NegationBinding)

    def __call__(self):
        op = self.state.encode(self.binding.op)
        return NegationCode(op, self.binding)


class Convert(Adapter):

    adapts(Domain, Domain)

    @classmethod
    def dispatch(interface, binding, *args, **kwds):
        assert isinstance(binding, CastBinding)
        return (type(binding.base.domain), type(binding.domain))

    def __init__(self, binding, state):
        assert isinstance(binding, CastBinding)
        assert isinstance(state, EncodingState)
        self.binding = binding
        self.base = binding.base
        self.domain = binding.domain
        self.state = state

    def __call__(self):
        base = self.state.encode(self.base)
        if base.domain == self.domain:
            return base
        return CastCode(base, self.domain, self.binding)


class ConvertUntyped(Convert):

    adapts(UntypedDomain, Domain)

    def __call__(self):
        base = self.state.encode(self.base)
        assert isinstance(base, LiteralCode)
        assert isinstance(base.domain, UntypedDomain)
        try:
            value = self.domain.parse(base.value)
        except ValueError, exc:
            raise EncodeError(str(exc), self.binding.mark)
        return LiteralCode(value, self.domain, self.binding)


class ConvertTupleToBoolean(Convert):

    adapts(TupleDomain, BooleanDomain)

    def __call__(self):
        space = self.state.relate(self.binding.base)
        if space.table is None:
            raise EncodeError("expected a space with a prominent table",
                              self.binding.mark)
        for column in space.table.columns:
            if not column.is_nullable:
                break
        else:
            raise EncodeError("expected a table with at least one"
                              " non-nullable column", self.binding.mark)
        unit = ColumnUnit(column, space, self.binding)
        literal = LiteralCode(None, column.domain, self.binding)
        return NegationCode(TotalEqualityCode(unit, literal, self.binding),
                            self.binding)


class EncodeCast(Encode):

    adapts(CastBinding)

    def __call__(self):
        convert = Convert(self.binding, self.state)
        return convert()


class DirectCast(Direct):

    adapts(CastBinding)

    def __call__(self):
        return self.state.direct(self.binding.base)


class EncodeWrapper(Encode):

    adapts(WrapperBinding)

    def __call__(self):
        return self.state.encode(self.binding.base)


class RelateWrapper(Relate):

    adapts(WrapperBinding)

    def __call__(self):
        return self.state.relate(self.binding.base)


class DirectWrapper(Direct):

    adapts(WrapperBinding)

    def __call__(self):
        return self.state.direct(self.binding.base)


class DirectDirection(Direct):

    adapts(DirectionBinding)

    def __call__(self):
        direction = self.binding.direction
        base_direction = self.state.direct(self.binding.base)
        if base_direction is not None:
            direction *= base_direction
        return direction


def encode(binding, state=None):
    if state is None:
        state = EncodingState()
    encode = Encode(binding, state)
    return encode()


def relate(binding, state=None):
    if state is None:
        state = EncodingState()
    relate = Relate(binding, state)
    return relate()


def direct(binding, state=None):
    if state is None:
        state = EncodingState()
    direct = Direct(binding, state)
    return direct()


