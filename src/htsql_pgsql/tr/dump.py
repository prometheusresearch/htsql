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
from htsql.tr.dump import (FormatLiteral, DumpBoolean, DumpFloat,
                           DumpByDomain, DumpToDomain,
                           DumpIsTotallyEqual)
from htsql.tr.fn.signature import (DateSig, ContainsSig, DateIncrementSig,
                                   DateDecrementSig, DateDifferenceSig)
from htsql.tr.fn.dump import DumpFunction
from htsql.tr.frame import LiteralPhrase, NullPhrase
from htsql.tr.error import DumpError


class PGSQLFormatLiteral(FormatLiteral):

    def __call__(self):
        value = self.value.replace("'", "''")
        if "\\" in value:
            value = value.replace("\\", "\\\\")
            self.stream.write("E'%s'" % value)
        else:
            self.stream.write("'%s'" % value)


class PGSQLDumpBoolean(DumpBoolean):

    def __call__(self):
        if self.value is True:
            self.format("TRUE")
        if self.value is False:
            self.format("FALSE")


class PGSQLDumpFloat(DumpFloat):

    def __call__(self):
        if str(self.value) in ['inf', '-inf', 'nan']:
            raise DumpError("invalid float value",
                                 self.phrase.mark)
        self.format("%r::FLOAT8" % self.value)


class PGSQLDumpDate(DumpByDomain):

    adapts(DateDomain)

    def __call__(self):
        self.format("{value:literal}::DATE", value=str(self.value))


class PGSQLDumpToDate(DumpToDomain):

    adapts(Domain, DateDomain)

    def __call__(self):
        self.format("CAST({base} AS DATE)", base=self.base)


class PGSQLDumpIsTotallyEqual(DumpIsTotallyEqual):

    def __call__(self):
        self.format("({lop} IS {polarity:not}DISTINCT FROM {rop})",
                    self.arguments, polarity=-self.signature.polarity)


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


