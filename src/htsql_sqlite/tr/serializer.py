#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_sqlite.tr.serializer`
=================================

This module adapts the SQL serializer for SQLite.
"""


from htsql.adapter import adapts, find_adapters
from htsql.tr.frame import LeafFrame
from htsql.tr.serializer import Serializer, Format, SerializeLeaf


class SQLiteFormat(Format):

    def true(self):
        return "1"

    def false(self):
        return "0"


class SQLiteSerializeLeaf(SerializeLeaf):

    adapts(LeafFrame, Serializer)

    def serialize(self):
        return self.format.name(self.frame.table.name)


serializer_adapters = find_adapters()


