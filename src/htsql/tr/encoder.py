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
                      SortBinding, EqualityBinding, TotalEqualityBinding,
                      ConjunctionBinding, DisjunctionBinding,
                      NegationBinding, CastBinding, WrapperBinding)
from .code import (ScalarSpace, CrossProductSpace, JoinProductSpace,
                   FilteredSpace, OrderedSpace,
                   QueryExpression, SegmentExpression, LiteralCode,
                   EqualityCode, TotalEqualityCode,
                   ConjunctionCode, DisjunctionCode, NegationCode,
                   CastCode, ColumnUnit)
from ..error import InvalidArgumentError


class Encoder(object):

    def encode(self, binding):
        encode = Encode(binding, self)
        return encode.encode()

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

    def relate(self):
        raise InvalidArgumentError("unable to relate a node",
                                   self.binding.mark)


class EncodeQuery(Encode):

    adapts(QueryBinding, Encoder)

    def encode(self):
        segment = None
        if self.binding.segment is not None:
            segment = self.encoder.encode(self.binding.segment)
        return QueryExpression(segment, self.binding)


class EncodeSegment(Encode):

    adapts(SegmentBinding, Encoder)

    def encode(self):
        space = self.encoder.relate(self.binding.base)
        space = OrderedSpace(space, [], None, None, self.binding)
        elements = []
        for binding in self.binding.elements:
            element = self.encoder.encode(binding)
            elements.append(element)
        return SegmentExpression(space, elements, self.binding)


class EncodeRoot(Encode):

    adapts(RootBinding, Encoder)

    def relate(self):
        return ScalarSpace(None, self.binding)


class EncodeFreeTable(Encode):

    adapts(FreeTableBinding, Encoder)

    def relate(self):
        parent = self.encoder.relate(self.binding.base)
        return CrossProductSpace(parent, self.binding.table, self.binding)


class EncodeJoinedTable(Encode):

    adapts(JoinedTableBinding, Encoder)

    def relate(self):
        space = self.encoder.relate(self.binding.base)
        for join in self.binding.joins:
            space = JoinProductSpace(space, join, self.binding)
        return space


class EncodeColumn(Encode):

    adapts(ColumnBinding, Encoder)

    def relate(self):
        if self.binding.link is not None:
            return self.encoder.relate(self.binding.link)
        return super(EncodeColumn, self).relate()

    def encode(self):
        space = self.encoder.relate(self.binding.base)
        return ColumnUnit(self.binding.column, space, self.binding)


class EncodeSieve(Encode):

    adapts(SieveBinding, Encoder)

    def relate(self):
        space = self.encoder.relate(self.binding.base)
        filter = self.encoder.encode(self.binding.filter)
        return FilteredSpace(space, filter, self.binding)


class EncodeSort(Encode):

    adapts(SortBinding, Encoder)

    def relate(self):
        space = self.encoder.relate(self.binding.base)
        order = [(self.encoder.encode(binding), +1)
                 for binding in self.binding.order]
        limit = self.binding.limit
        offset = self.binding.offset
        return OrderedSpace(space, order, limit, offset, self.binding)


class EncodeLiteral(Encode):

    adapts(LiteralBinding, Encoder)

    def encode(self):
        return LiteralCode(self.binding.value, self.binding.domain,
                           self.binding)


class EncodeEquality(Encode):

    adapts(EqualityBinding, Encoder)

    def encode(self):
        left = self.encoder.encode(self.binding.lop)
        right = self.encoder.encode(self.binding.rop)
        return EqualityCode(left, right, self.binding)


class EncodeTotalEquality(Encode):

    adapts(TotalEqualityBinding, Encoder)

    def encode(self):
        left = self.encoder.encode(self.binding.lop)
        right = self.encoder.encode(self.binding.rop)
        return TotalEqualityCode(left, right, self.binding)


class EncodeConjunction(Encode):

    adapts(ConjunctionBinding, Encoder)

    def encode(self):
        terms = [self.encoder.encode(op) for op in self.binding.ops]
        return ConjunctionCode(terms, self.binding)


class EncodeDisjunction(Encode):

    adapts(DisjunctionBinding, Encoder)

    def encode(self):
        terms = [self.encoder.encode(op) for op in self.binding.ops]
        return DisjunctionCode(terms, self.binding)


class EncodeNegation(Encode):

    adapts(NegationBinding, Encoder)

    def encode(self):
        term = self.encoder.encode(self.binding.op)
        return NegationCode(term, self.binding)


class EncodeCast(Encode):

    adapts(CastBinding, Encoder)

    def encode(self):
        if isinstance(self.binding.op.domain, TupleDomain):
            space = self.encoder.relate(self.binding.op)
            primary_key = space.table.primary_key
            assert primary_key is not None
            column_name = primary_key.origin_column_names[0]
            column = space.table.columns[column_name]
            code = ColumnUnit(column, space, self.binding)
            literal = LiteralCode(None, column.domain, self.binding)
            return NegationCode(TotalEqualityCode(code, literal, self.binding),
                                self.binding)
        code = self.encoder.encode(self.binding.op)
        if isinstance(code.domain, self.binding.domain.__class__):
            return code
        if isinstance(code.domain, UntypedDomain):
            try:
                value = self.binding.domain.parse(code.value)
            except ValueError, exc:
                raise InvalidArgumentError(str(exc), code.mark)
            return LiteralCode(value, self.binding.domain, self.binding)
        return CastCode(code, self.binding.domain, self.binding)


class EncodeWrapper(Encode):

    adapts(WrapperBinding, Encoder)

    def encode(self):
        return self.encoder.encode(self.binding.base)

    def relate(self):
        return self.encoder.relate(self.binding.base)


