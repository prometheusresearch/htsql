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
from htsql.tr.dump import (DumpTable, DumpBoolean, DumpDecimal, DumpDate,
                           DumpToFloat, DumpToDecimal, DumpToString,
                           DumpToDate, DumpIsTotallyEqual)
from htsql.tr.error import SerializeError


class SQLiteDumpTable(DumpTable):

    def __call__(self):
        table = self.frame.space.table
        self.format("{table:name}", table=table.name)


class SQLiteDumpBoolean(DumpBoolean):

    def __call__(self):
        if self.value is None:
            self.write("NULL")
        if self.value is True:
            self.format("1")
        if self.value is False:
            self.format("0")


class SQLiteDumpDecimal(DumpDecimal):

    def __call__(self):
        raise SerializeError("decimal data type is not supported",
                             self.phrase.mark)


class SQLiteDumpDate(DumpDate):

    def __call__(self):
        self.write(str(self.value))


class SQLiteDumpToFloat(DumpToFloat):

    def __call__(self):
        self.format("CAST({base} AS REAL)", base=self.base)


class SQLiteDumpToDecimal(DumpToDecimal):

    def __call__(self):
        raise SerializeError("decimal data type is not supported",
                             self.phrase.mark)


class SQLiteDumpToString(DumpToString):

    def __call__(self):
        self.format("CAST({base} AS TEXT)", base=self.base)


class SQLiteDumpToDate(DumpToDate):

    def __call__(self):
        self.format("DATE({base})", base=self.base)


class SQLiteDumpIsTotallyEqual(DumpIsTotallyEqual):

    def __call__(self):
        self.format("({polarity:not}CASE WHEN ({lop} = {rop}) OR"
                    " ({lop} IS NULL AND {rop} IS NULL)"
                    " THEN 1 ELSE 0 END)",
                    self.arguments, self.signature)


