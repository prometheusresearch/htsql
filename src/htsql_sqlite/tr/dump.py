#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_sqlite.tr.dump`
===========================

This module adapts the SQL serializer for SQLite.
"""


from htsql.adapter import adapts
from htsql.domain import BooleanDomain, StringDomain
from htsql.tr.dump import (DumpTable, DumpBoolean, DumpDecimal, DumpDate,
                           DumpToFloat, DumpToDecimal, DumpToString,
                           DumpToDate, DumpIsTotallyEqual)
from htsql.tr.fn.dump import (DumpLength, DumpSubstring, DumpTrim,
                              DumpDateIncrement, DumpDateDecrement,
                              DumpDateDifference, DumpMakeDate,
                              DumpExtractYear, DumpExtractMonth,
                              DumpExtractDay)
from htsql.tr.error import SerializeError


class SQLiteDumpTable(DumpTable):

    def __call__(self):
        table = self.frame.space.family.table
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
        self.format("{value:literal}", value=str(self.value))


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


class SQLiteDumpBooleanToString(SQLiteDumpToString):

    adapts(BooleanDomain, StringDomain)

    def __call__(self):
        self.format("(CASE WHEN {base} THEN 'true'"
                    " WHEN NOT {base} THEN 'false' END)", base=self.base)


class SQLiteDumpToDate(DumpToDate):

    def __call__(self):
        self.format("DATE({base})", base=self.base)


class SQLiteDumpIsTotallyEqual(DumpIsTotallyEqual):

    def __call__(self):
        self.format("({polarity:not}CASE WHEN ({lop} = {rop}) OR"
                    " ({lop} IS NULL AND {rop} IS NULL)"
                    " THEN 1 ELSE 0 END)",
                    self.arguments, self.signature)


class SQLiteDumpLength(DumpLength):

    template = "LENGTH({op})"


class SQLiteDumpSubstring(DumpSubstring):

    def __call__(self):
        if self.phrase.length is None:
            self.format("SUBSTR({op}, {start})", self.phrase)
        else:
            self.format("SUBSTR({op}, {start}, {length})", self.phrase)


class SQLiteDumpTrim(DumpTrim):

    def __call__(self):
        if self.signature.is_left and not self.signature.is_right:
            self.format("LTRIM({op})", self.arguments)
        elif not self.signature.is_left and self.signature.is_right:
            self.format("RTRIM({op})", self.arguments)
        else:
            self.format("TRIM({op})", self.arguments)


class SQLiteDumpDateIncrement(DumpDateIncrement):

    template = "DATE(JULIANDAY({lop}) + {rop})"


class SQLiteDumpDateDecrement(DumpDateDecrement):

    template = "DATE(JULIANDAY({lop}) - {rop})"


class SQLiteDumpDateDifference(DumpDateDifference):

    template = "CAST(JULIANDAY({lop}) - JULIANDAY({rop}) AS INTEGER)"


class SQLiteDumpMakeDate(DumpMakeDate):

    template = ("DATE('0001-01-01', ({year} - 1) || ' years',"
                " ({month} - 1) || ' months', ({day} - 1) || ' days')")


class SQLiteDumpExtractYear(DumpExtractYear):

    template = "CAST(STRFTIME('%Y', {op}) AS INTEGER)"


class SQLiteDumpExtractMonth(DumpExtractMonth):

    template = "CAST(STRFTIME('%m', {op}) AS INTEGER)"


class SQLiteDumpExtractDay(DumpExtractDay):

    template = "CAST(STRFTIME('%d', {op}) AS INTEGER)"


