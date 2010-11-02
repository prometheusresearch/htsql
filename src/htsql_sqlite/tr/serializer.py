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


from htsql.adapter import adapts
from htsql.tr.frame import TableFrame
from htsql.tr.serializer import Serializer, Format, SerializeTable


class SQLiteFormat(Format):

    def true(self):
        return "1"

    def false(self):
        return "0"


class SQLiteSerializeTable(SerializeTable):

    adapts(TableFrame, Serializer)

    def serialize(self):
        return self.format.name(self.frame.table.name)


