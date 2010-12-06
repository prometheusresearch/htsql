#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_pgsql.tr.dump`
==========================

This module adapts the SQL serializer for PostgreSQL.
"""


from htsql.adapter import adapts
from htsql.domain import Domain, DateDomain
from htsql.tr.dump import (FormatLiteral, DumpBranch, DumpInteger, DumpFloat,
                           DumpDecimal, DumpDate, DumpToDecimal, DumpToFloat,
                           DumpToString)
from htsql.tr.fn.signature import (DateSig, ContainsSig, DateIncrementSig,
                                   DateDecrementSig, DateDifferenceSig)
from htsql.tr.fn.dump import DumpFunction
from htsql.tr.frame import LiteralPhrase, NullPhrase
from htsql.tr.error import SerializeError


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


class PGSQLDumpInteger(DumpInteger):

    def __call__(self):
        if not (-2**63 <= self.value < 2**63):
            raise SerializeError("invalid integer value",
                                 self.phrase.mark)
        self.write(str(self.value))


class PGSQLDumpFloat(DumpFloat):

    def __call__(self):
        value = repr(self.value)
        if value == 'inf':
            value = "'Infinity'"
        elif value == '-inf':
            value = "'-Infinity'"
        elif value == 'nan':
            value = "'NaN'"
        self.format("%s::FLOAT8" % value)


class PGSQLDumpDecimal(DumpDecimal):

    def __call__(self):
        if self.value.is_nan():
            self.write("'NaN'::NUMERIC")
            return
        if not self.value.is_finite():
            raise SerializeError("invalid decimal value",
                                 self.phrase.mark)
        self.format("%s::NUMERIC" % self.value)


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


class PGSQLDumpDateConstructor(DumpFunction):

    adapts(DateSig)
    template = ("CAST('0001-01-01'::DATE"
                " + ({year} - 1) * '1 YEAR'::INTERVAL"
                " + ({month} - 1) * '1 MONTH'::INTERVAL"
                " + ({day} - 1) * '1 DAY'::INTERVAL"
                " AS DATE)")


class PGSQLDumpDateIncrement(DumpFunction):

    adapts(DateIncrementSig)
    template = "({lop} + {rop})"


class PGSQLDumpDateDecrement(DumpFunction):

    adapts(DateDecrementSig)
    template = "({lop} - {rop})"


class PGSQLDumpDateDifference(DumpFunction):

    adapts(DateDifferenceSig)
    template = "({lop} - {rop})"


class PGSQLDumpStringContains(DumpFunction):

    adapts(ContainsSig)

    def __call__(self):
        if isinstance(self.phrase.rop, NullPhrase):
            self.format("({lop} {polarity:not}ILIKE {rop})",
                        self.phrase, self.phrase.signature)
        elif isinstance(self.phrase.rop, LiteralPhrase):
            value = self.phrase.rop.value
            value.replace("%", "\\%").replace("_", "\\_")
            value = "%"+value+"%"
            self.format("({lop} {polarity:not}ILIKE"
                        " {value:literal})",
                        self.phrase, self.phrase.signature,
                        value=value)
        else:
            self.format("({lop} {polarity:not}ILIKE"
                        " ({percent:literal} ||"
                        " REPLACE(REPLACE({rop},"
                        " {percent:literal}, {xpercent:literal}),"
                        " {underline:literal}, {xunderline:literal}) ||"
                        " {percent:literal}))",
                        self.phrase,
                        self.phrase.signature,
                        percent="%", underline="_",
                        xpercent="\\%", xunderline="\\_")


