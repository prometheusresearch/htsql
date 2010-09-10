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
from ..domain import UntypedDomain, TupleDomain
from .binding import (Binding, RootBinding, QueryBinding, SegmentBinding,
                      TableBinding, FreeTableBinding, JoinedTableBinding,
                      ColumnBinding, LiteralBinding, SieveBinding,
                      SortBinding, EqualityBinding, InequalityBinding,
                      TotalEqualityBinding, TotalInequalityBinding,
                      ConjunctionBinding, DisjunctionBinding,
                      NegationBinding, CastBinding, WrapperBinding)
from .code import (ScalarSpace, FreeTableSpace, JoinedTableSpace,
                   ScreenSpace, OrderedSpace, LiteralExpression, ColumnUnit,
                   TupleExpression, QueryCode, SegmentCode, ElementExpression,
                   EqualityExpression, InequalityExpression,
                   TotalEqualityExpression, TotalInequalityExpression,
                   ConjunctionExpression, DisjunctionExpression,
                   NegationExpression, CastExpression)
from .lookup import Lookup
from ..error import InvalidArgumentError


class Encoder(object):

    def encode(self, binding):
        encode = Encode(binding, self)
        return encode.encode()

    def encode_element(self, binding):
        encode = Encode(binding, self)
        return encode.encode_element()

    def relate(self, binding):
        encode = Encode(binding, self)
        return encode.relate()


class Encode(Adapter):

    adapts(Binding, Encoder)

    def __init__(self, binding, encoder):
        self.binding = binding
        self.encoder = encoder

    def encode(self):
        raise InvalidArgumentError("unable to encode a node",
                                   self.binding.mark)

    def encode_element(self):
        code = self.encode()
        return ElementExpression(code)

    def relate(self):
        raise InvalidArgumentError("unable to relate a node",
                                   self.binding.mark)


class EncodeQuery(Encode):

    adapts(QueryBinding, Encoder)

    def encode(self):
        segment = None
        if self.binding.segment is not None:
            segment = self.encoder.encode(self.binding.segment)
        return QueryCode(self.binding, segment, self.binding.mark)


class EncodeSegment(Encode):

    adapts(SegmentBinding, Encoder)

    def encode(self):
        space = self.encoder.relate(self.binding.base)
        space = OrderedSpace(space, [], None, None, space.mark)
        elements = []
        for binding in self.binding.elements:
            element = self.encoder.encode_element(binding)
            elements.append(element.code)
        return SegmentCode(space, elements, self.binding.mark)


class EncodeRoot(Encode):

    adapts(RootBinding, Encoder)

    def relate(self):
        return ScalarSpace(None, self.binding.mark)


class EncodeFreeTable(Encode):

    adapts(FreeTableBinding, Encoder)

    def relate(self):
        parent = self.encoder.relate(self.binding.base)
        return FreeTableSpace(parent, self.binding.table, self.binding.mark)


class EncodeJoinedTable(Encode):

    adapts(JoinedTableBinding, Encoder)

    def relate(self):
        space = self.encoder.relate(self.binding.base)
        for join in self.binding.joins:
            space = JoinedTableSpace(space, join, self.binding.mark)
        return space


class EncodeColumn(Encode):

    adapts(ColumnBinding, Encoder)

    def relate(self):
        if self.binding.link is not None:
            return self.encoder.relate(self.binding.link)
        return super(EncodeColumn, self).relate()

    def encode(self):
        space = self.encoder.relate(self.binding.base)
        return ColumnUnit(self.binding.column, space, self.binding.mark)


class EncodeSieve(Encode):

    adapts(SieveBinding, Encoder)

    def relate(self):
        space = self.encoder.relate(self.binding.base)
        filter = self.encoder.encode(self.binding.filter)
        return ScreenSpace(space, filter, self.binding.mark)


class EncodeSort(Encode):

    adapts(SortBinding, Encoder)

    def relate(self):
        space = self.encoder.relate(self.binding.base)
        order = [(self.encoder.encode(binding), +1)
                 for binding in self.binding.order]
        limit = self.binding.limit
        offset = self.binding.offset
        return OrderedSpace(space, order, limit, offset, self.binding.mark)


#class EncodeTuple(Encode):
#
#    adapts(TupleBinding, Encoder)
#
#    def encode(self):
#        space = self.encoder.relate(self.binding.binding)
#        return TupleExpression(space, self.binding.mark)


class EncodeLiteral(Encode):

    adapts(LiteralBinding, Encoder)

    def encode(self):
        return LiteralExpression(self.binding.value, self.binding.domain,
                                 self.binding.mark)


class EncodeEquality(Encode):

    adapts(EqualityBinding, Encoder)

    def encode(self):
        left = self.encoder.encode(self.binding.lop)
        right = self.encoder.encode(self.binding.rop)
        return EqualityExpression(left, right, self.binding.mark)


class EncodeInequality(Encode):

    adapts(InequalityBinding, Encoder)

    def encode(self):
        left = self.encoder.encode(self.binding.lop)
        right = self.encoder.encode(self.binding.rop)
        return InequalityExpression(left, right, self.binding.mark)


class EncodeTotalEquality(Encode):

    adapts(TotalEqualityBinding, Encoder)

    def encode(self):
        left = self.encoder.encode(self.binding.lop)
        right = self.encoder.encode(self.binding.rop)
        return TotalEqualityExpression(left, right, self.binding.mark)


class EncodeTotalInequality(Encode):

    adapts(TotalInequalityBinding, Encoder)

    def encode(self):
        left = self.encoder.encode(self.binding.lop)
        right = self.encoder.encode(self.binding.rop)
        return TotalInequalityExpression(left, right, self.binding.mark)


class EncodeConjunction(Encode):

    adapts(ConjunctionBinding, Encoder)

    def encode(self):
        terms = [self.encoder.encode(op) for op in self.binding.ops]
        return ConjunctionExpression(terms, self.binding.mark)


class EncodeDisjunction(Encode):

    adapts(DisjunctionBinding, Encoder)

    def encode(self):
        terms = [self.encoder.encode(op) for op in self.binding.ops]
        return DisjunctionExpression(terms, self.binding.mark)


class EncodeNegation(Encode):

    adapts(NegationBinding, Encoder)

    def encode(self):
        term = self.encoder.encode(self.binding.op)
        return NegationExpression(term, self.binding.mark)


class EncodeCast(Encode):

    adapts(CastBinding, Encoder)

    def encode(self):
        if isinstance(self.binding.op.domain, TupleDomain):
            space = self.encoder.relate(self.binding.op)
            return TupleExpression(space, self.binding.mark)
        code = self.encoder.encode(self.binding.op)
        if isinstance(code.domain, self.binding.domain.__class__):
            return code
        if isinstance(code.domain, UntypedDomain):
            try:
                value = self.binding.domain.parse(code.value)
            except ValueError, exc:
                raise InvalidArgumentError(str(exc), code.mark)
            return LiteralExpression(value, self.binding.domain,
                                     self.binding.mark)
        return CastExpression(code, self.binding.domain, self.binding.mark)


class EncodeWrapper(Encode):

    adapts(WrapperBinding, Encoder)

    def encode(self):
        return self.encoder.encode(self.binding.base)

    def relate(self):
        return self.encoder.relate(self.binding.base)


