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
                      ColumnBinding, LiteralBinding, SieveBinding)
from .code import (ScalarSpace, FreeTableSpace, JoinedTableSpace,
                   ScreenSpace, OrderedSpace, LiteralExpression, ColumnUnit,
                   QueryCode, SegmentCode, ElementExpression)
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
        return ElementExpression(code, None)

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
            if element.order is not None and element.code not in order_set:
                order.append((element.code, element.order))
                order_set.add(element.code)
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
        return self.encoder.relate(self.binding.parent)


class EncodeColumn(Encode):

    adapts(ColumnBinding, Encoder)

    def relate(self):
        space = self.encoder.relate(self.binding.parent)
        binding = self.binding.as_table()
        for join in binding.joins:
            space = JoinedTableSpace(space, join, binding.mark)
        return space

    def encode(self):
        space = self.encoder.relate(self.binding.parent)
        return ColumnUnit(self.binding.column, space, self.binding.mark)


class EncodeSieve(Encode):

    adapts(SieveBinding, Encoder)

    def relate(self):
        space = self.encoder.relate(self.binding.parent)
        filter = self.encoder.encode(self.binding.filter)
        return ScreenSpace(space, filter, self.binding.mark)


encode_adapters = find_adapters()


