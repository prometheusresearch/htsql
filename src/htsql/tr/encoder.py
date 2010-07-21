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


from ..adapter import Adapter, adapts, find_adapters
from .binding import (Binding, RootBinding, QueryBinding, SegmentBinding,
                      TableBinding, FreeTableBinding, JoinedTableBinding,
                      ColumnBinding, LiteralBinding, SieveBinding,
                      EqualityBinding, InequalityBinding,
                      ConjunctionBinding, DisjunctionBinding,
                      NegationBinding, CastBinding, TupleBinding)
from .code import (ScalarSpace, FreeTableSpace, JoinedTableSpace,
                   ScreenSpace, OrderedSpace, LiteralExpression, ColumnUnit,
                   TupleExpression, QueryCode, SegmentCode, ElementExpression,
                   EqualityExpression, InequalityExpression,
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
        elements = []
        order = []
        order_set = set()
        for binding in self.binding.elements:
            element = self.encoder.encode_element(binding)
            elements.append(element.code)
        if space.table is not None and space.table.primary_key is not None:
            for column_name in space.table.primary_key.origin_column_names:
                column = space.table.columns[column_name]
                code = ColumnUnit(column, space, self.binding.mark)
                if code not in order_set:
                    order.append((code, +1))
                    order_set.add(code)
        if order:
            space = OrderedSpace(space, order, None, None, space.mark)
        return SegmentCode(space, elements, self.binding.mark)


class EncodeRoot(Encode):

    adapts(RootBinding, Encoder)

    def relate(self):
        return ScalarSpace(None, self.binding.mark)


class EncodeFreeTable(Encode):

    adapts(FreeTableBinding, Encoder)

    def relate(self):
        parent = self.encoder.relate(self.binding.parent)
        return FreeTableSpace(parent, self.binding.table, self.binding.mark)


class EncodeJoinedTable(Encode):

    adapts(JoinedTableBinding, Encoder)

    def relate(self):
        space = self.encoder.relate(self.binding.parent)
        for join in self.binding.joins:
            space = JoinedTableSpace(space, join, self.binding.mark)
        return space


class EncodeColumn(Encode):

    adapts(ColumnBinding, Encoder)

    def relate(self):
        return self.encoder.relate(self.binding.parent)

    def encode(self):
        space = self.encoder.relate(self.binding.parent)
        return ColumnUnit(self.binding.column, space, self.binding.mark)


class EncodeSieve(Encode):

    adapts(SieveBinding, Encoder)

    def relate(self):
        space = self.encoder.relate(self.binding.parent)
        filter = self.encoder.encode(self.binding.filter)
        return ScreenSpace(space, filter, self.binding.mark)


class EncodeTuple(Encode):

    adapts(TupleBinding, Encoder)

    def encode(self):
        space = self.encoder.relate(self.binding.binding)
        return TupleExpression(space, self.binding.mark)


class EncodeLiteral(Encode):

    adapts(LiteralBinding, Encoder)

    def encode(self):
        return LiteralExpression(self.binding.value, self.binding.domain,
                                 self.binding.mark)


class EncodeEquality(Encode):

    adapts(EqualityBinding, Encoder)

    def encode(self):
        left = self.encoder.encode(self.binding.left)
        right = self.encoder.encode(self.binding.right)
        return EqualityExpression(left, right, self.binding.mark)


class EncodeInequality(Encode):

    adapts(InequalityBinding, Encoder)

    def encode(self):
        left = self.encoder.encode(self.binding.left)
        right = self.encoder.encode(self.binding.right)
        return InequalityExpression(left, right, self.binding.mark)


class EncodeConjunction(Encode):

    adapts(ConjunctionBinding, Encoder)

    def encode(self):
        terms = [self.encoder.encode(term) for term in self.binding.terms]
        return ConjunctionExpression(terms, self.binding.mark)


class EncodeDisjunction(Encode):

    adapts(DisjunctionBinding, Encoder)

    def encode(self):
        terms = [self.encoder.encode(term) for term in self.binding.terms]
        return DisjunctionExpression(terms, self.binding.mark)


class EncodeNegation(Encode):

    adapts(NegationBinding, Encoder)

    def encode(self):
        term = self.encoder.encode(self.binding.term)
        return NegationExpression(term, self.binding.mark)


class EncodeCast(Encode):

    adapts(CastBinding, Encoder)

    def encode(self):
        code = self.encoder.encode(self.binding.binding)
        return CastExpression(code, self.binding.domain, self.binding.mark)


encode_adapters = find_adapters()


