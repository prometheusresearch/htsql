#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from htsql.core.domain import IntegerDomain
from htsql.core.tr.dump import (FormatLiteral, DumpFloat, DumpDecimal, DumpDate,
                                DumpTime, DumpDateTime, DumpToDecimal,
                                DumpToFloat, DumpToString, DumpSortDirection)
from htsql.core.tr.fn.dump import (DumpLike, DumpDateIncrement,
                                   DumpDateDecrement, DumpDateDifference,
                                   DumpMakeDate, DumpExtractYear,
                                   DumpExtractMonth, DumpExtractDay,
                                   DumpExtractHour, DumpExtractMinute, DumpSum)


class PGSQLFormatLiteral(FormatLiteral):

    def __call__(self):
        value = self.value.replace(u"'", u"''")
        if u"\\" in value:
            value = value.replace(u"\\", u"\\\\")
            self.stream.write(u"E'%s'" % value)
        else:
            self.stream.write(u"'%s'" % value)


class PGSQLDumpSortDirection(DumpSortDirection):

    def __call__(self):
        self.format("{base} {direction:switch{ASC|DESC}}",
                    self.arguments, self.signature)
        if self.phrase.is_nullable:
            self.format(" NULLS {direction:switch{FIRST|LAST}}",
                        self.signature)


class PGSQLDumpFloat(DumpFloat):

    def __call__(self):
        if self.value >= 0.0:
            self.write(u"%s::FLOAT8" % repr(self.value))
        else:
            self.write(u"'%s'::FLOAT8" % repr(self.value))


class PGSQLDumpDecimal(DumpDecimal):

    def __call__(self):
        if not self.value.is_signed():
            self.write(u"%s::NUMERIC" % self.value)
        else:
            self.write(u"'%s'::NUMERIC" % self.value)


class PGSQLDumpDate(DumpDate):

    def __call__(self):
        self.format("{value:literal}::DATE", value=unicode(self.value))


class PGSQLDumpTime(DumpTime):

    def __call__(self):
        self.format("{value:literal}::TIME", value=unicode(self.value))


class PGSQLDumpDateTime(DumpDateTime):

    def __call__(self):
        if self.value.tzinfo is None:
            self.format("{value:literal}::TIMESTAMP",
                        value=unicode(self.value))
        else:
            self.format("{value:literal}::TIMESTAMPTZ",
                        value=unicode(self.value))


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


class PGSQLDumpExtractHour(DumpExtractHour):

    template = "CAST(EXTRACT(HOUR FROM {op}) AS INTEGER)"


class PGSQLDumpExtractMinute(DumpExtractMinute):

    template = "CAST(EXTRACT(MINUTE FROM {op}) AS INTEGER)"


class PGSQLDumpLike(DumpLike):

    def __call__(self):
        self.format("({lop} {polarity:not}ILIKE {rop})",
                    self.phrase, self.signature)


class PGSQLDumpSum(DumpSum):

    def __call__(self):
        if isinstance(self.phrase.domain, IntegerDomain):
            self.format("CAST(SUM({op}) AS BIGINT)", self.phrase)
        else:
            return super(PGSQLDumpSum, self).__call__()


