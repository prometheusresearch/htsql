#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_sqlite.tr.dump`
===========================

This module adapts the SQL serializer for SQLite.
"""


from htsql.adapter import adapts
from htsql.tr.frame import TableFrame
from htsql.tr.dump import DumpTable, DumpBoolean


class SQLiteDumpTable(DumpTable):

    def __call__(self):
        table = self.frame.space.table
        self.format("{table:name}", table=table.name)


class SQLiteDumpBoolean(DumpBoolean):

    def __call__(self):
        if self.value is True:
            self.format("1")
        if self.value is False:
            self.format("0")


