#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_sqlite.tr.serialize`
================================

This module adapts the SQL serializer for SQLite.
"""


from htsql.adapter import adapts
from htsql.tr.frame import TableFrame
from htsql.tr.serialize import SerializeTable, SerializeBoolean


class SQLiteSerializeTable(SerializeTable):

    def __call__(self):
        table = self.frame.space.table
        self.state.format("{table:name}", table=table.name)


class SQLiteSerializeBoolean(SerializeBoolean):

    def __call__(self):
        if self.value is True:
            self.state.format("1")
        if self.value is False:
            self.state.format("0")


