#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_pgsql.tr.dump`
==========================

This module adapts the SQL serializer for PostgreSQL.
"""


from htsql.tr.dump import (FormatLiteral, DumpBranch, DumpFloat,
                           DumpDecimal, DumpDate, DumpToDecimal, DumpToFloat,
                           DumpToString)
from htsql.tr.fn.dump import (DumpLike, DumpDateIncrement,
                              DumpDateDecrement, DumpDateDifference,
                              DumpMakeDate, DumpExtractYear, DumpExtractMonth,
                              DumpExtractDay)


class PGSQLFormatLiteral(FormatLiteral):

    def __call__(self):
        value = self.value.replace("'", "''")
        if "\\" in value:
            value = value.replace("\\", "\\\\")
            self.stream.write("E'%s'" % value)
        else:
            self.stream.write("'%s'" % value)


class PGSQLDumpBranch(DumpBranch):

    def dump_order(self):
        if not self.frame.order:
            return
        self.newline()
        self.format("ORDER BY ")
        for index, (phrase, direction) in enumerate(self.frame.order):
            if phrase in self.frame.select:
                position = self.frame.select.index(phrase)+1
                self.write(str(position))
            else:
                self.format("{kernel}", kernel=phrase)
            self.format(" {direction:switch{ASC|DESC}}", direction=direction)
            if phrase.is_nullable:
                self.format(" NULLS {direction:switch{FIRST|LAST}}",
                            direction=direction)
            if index < len(self.frame.order)-1:
                self.write(", ")


class PGSQLDumpFloat(DumpFloat):

    def __call__(self):
        self.write("%s::FLOAT8" % repr(self.value))


class PGSQLDumpDecimal(DumpDecimal):

    def __call__(self):
        self.write("%s::NUMERIC" % self.value)


class PGSQLDumpDate(DumpDate):

    def __call__(self):
        self.format("{value:literal}::DATE", value=str(self.value))


class PGSQLDumpToFloat(DumpToFloat):

    def __call__(self):
        self.format("CAST({base} AS FLOAT8)", base=self.base)


class PGSQLDumpToDecimal(DumpToDecimal):

    def __call__(self):
        self.format("CAST({base} AS NUMERIC)", base=self.base)


class PGSQLDumpToString(DumpToString):

    def __call__(self):
        self.format("CAST({base} AS TEXT)", base=self.base)


class PGSQLDumpMakeDate(DumpMakeDate):

    template = ("CAST('0001-01-01'::DATE"
                " + ({year} - 1) * '1 YEAR'::INTERVAL"
                " + ({month} - 1) * '1 MONTH'::INTERVAL"
                " + ({day} - 1) * '1 DAY'::INTERVAL"
                " AS DATE)")


class PGSQLDumpDateIncrement(DumpDateIncrement):

    template = "({lop} + {rop})"


class PGSQLDumpDateDecrement(DumpDateDecrement):

    template = "({lop} - {rop})"


class PGSQLDumpDateDifference(DumpDateDifference):

    template = "({lop} - {rop})"


class PGSQLDumpExtractYear(DumpExtractYear):

    template = "CAST(EXTRACT(YEAR FROM {op}) AS INTEGER)"


class PGSQLDumpExtractMonth(DumpExtractMonth):

    template = "CAST(EXTRACT(MONTH FROM {op}) AS INTEGER)"


class PGSQLDumpExtractDay(DumpExtractDay):

    template = "CAST(EXTRACT(DAY FROM {op}) AS INTEGER)"


class PGSQLDumpLike(DumpLike):

    def __call__(self):
        self.format("({lop} {polarity:not}ILIKE {rop})",
                    self.phrase, self.signature)


