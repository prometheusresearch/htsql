#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from htsql.core.adapter import adapt
from htsql.core.domain import BooleanDomain, StringDomain
from htsql.core.tr.frame import LiteralPhrase
from htsql.core.tr.dump import (DumpBoolean, DumpDecimal, DumpDate,
                                DumpTime, DumpDateTime,
                                DumpToFloat, DumpToDecimal, DumpToString,
                                DumpToDate, DumpToTime, DumpToDateTime,
                                DumpIsTotallyEqual)
from htsql.core.tr.fn.dump import (DumpRoundTo, DumpTrunc, DumpTruncTo,
                                   DumpLength, DumpSubstring, DumpTrim,
                                   DumpDateIncrement, DumpDateTimeIncrement,
                                   DumpDateDecrement, DumpDateTimeDecrement,
                                   DumpDateDifference, DumpMakeDate,
                                   DumpMakeDateTime, DumpCombineDateTime,
                                   DumpExtractYear, DumpExtractMonth,
                                   DumpExtractDay, DumpExtractHour,
                                   DumpExtractMinute, DumpExtractSecond,
                                   DumpToday, DumpNow, DumpFunction)
from .signature import IsAnySig
from htsql.core.tr.error import SerializeError


class SQLiteDumpBoolean(DumpBoolean):

    def __call__(self):
        if self.value is None:
            self.write(u"NULL")
        if self.value is True:
            self.write(u"1")
        if self.value is False:
            self.write(u"0")


class SQLiteDumpDecimal(DumpDecimal):

    def __call__(self):
        raise SerializeError("decimal data type is not supported",
                             self.phrase.mark)


class SQLiteDumpDate(DumpDate):

    def __call__(self):
        self.format("{value:literal}", value=unicode(self.value))


class SQLiteDumpTime(DumpTime):

    def __call__(self):
        value = self.value.replace(tzinfo=None)
        self.format("{value:literal}", value=unicode(value))


class SQLiteDumpDateTime(DumpDateTime):

    def __call__(self):
        value = self.value.replace(tzinfo=None)
        self.format("{value:literal}", value=unicode(value))


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

    adapt(BooleanDomain, StringDomain)

    def __call__(self):
        self.format("(CASE WHEN {base} THEN 'true'"
                    " WHEN NOT {base} THEN 'false' END)", base=self.base)


class SQLiteDumpToDate(DumpToDate):

    def __call__(self):
        self.format("DATE({base})", base=self.base)


class SQLiteDumpToTime(DumpToTime):

    def __call__(self):
        self.format("TIME({base})", base=self.base)


class SQLiteDumpToDateTime(DumpToDateTime):

    def __call__(self):
        self.format("DATETIME({base})", base=self.base)


class SQLiteDumpIsTotallyEqual(DumpIsTotallyEqual):

    def __call__(self):
        self.format("({polarity:not}CASE WHEN ({lop} = {rop}) OR"
                    " ({lop} IS NULL AND {rop} IS NULL)"
                    " THEN 1 ELSE 0 END)",
                    self.arguments, self.signature)


class SQLiteDumpRoundTo(DumpRoundTo):

    def __call__(self):
        if (isinstance(self.phrase.precision, LiteralPhrase)
                and self.phrase.precision.value is not None):
            scale = self.phrase.precision.value
            if scale >= 0:
                self.format("ROUND({op}, {scale:pass})",
                            self.arguments, scale=unicode(scale))
            else:
                power = 10**(-scale)
                self.format("(ROUND({op} / {power:pass}.0) * {power:pass}.0)",
                            self.arguments, power=unicode(power))
        else:
            self.format("(ROUND({op} * POWER(10, {precision}))"
                        " / POWER(10, {precision}))",
                        self.arguments, self.signature)


class SQLiteDumpTrunc(DumpTrunc):

    template = "ROUND({op} - (CASE WHEN {op} >= 0 THEN 0.5 ELSE -0.5 END))"


class SQLiteDumpTruncTo(DumpTruncTo):

    template = ("(ROUND({op} * POWER(10, {precision})"
                " - (CASE WHEN {op} >= 0 THEN 0.5 ELSE -0.5 END))"
                " / POWER(10, {precision}))")


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


class SQLiteDumpToday(DumpToday):

    template = "DATE('now', 'localtime')"


class SQLiteDumpNow(DumpNow):

    template = "DATETIME('now', 'localtime')"


class SQLiteDumpDateIncrement(DumpDateIncrement):

    template = "DATE(JULIANDAY({lop}) + {rop})"


class SQLiteDumpDateTimeIncrement(DumpDateTimeIncrement):

    template = "DATETIME(JULIANDAY({lop}) + {rop})"


class SQLiteDumpDateDecrement(DumpDateDecrement):

    template = "DATE(JULIANDAY({lop}) - {rop})"


class SQLiteDumpDateTimeDecrement(DumpDateTimeDecrement):

    template = "DATETIME(JULIANDAY({lop}) - {rop})"


class SQLiteDumpDateDifference(DumpDateDifference):

    template = "CAST(JULIANDAY({lop}) - JULIANDAY({rop}) AS INTEGER)"


class SQLiteDumpMakeDate(DumpMakeDate):

    template = ("DATE('0001-01-01', ({year} - 1) || ' years',"
                " ({month} - 1) || ' months', ({day} - 1) || ' days')")


class SQLiteDumpMakeDateTime(DumpMakeDateTime):

    def __call__(self):
        template = ("DATETIME('0001-01-01', ({year} - 1) || ' years',"
                    " ({month} - 1) || ' months', ({day} - 1) || ' days'")
        if self.phrase.hour is not None:
            template += ", {hour} || ' hours'"
        if self.phrase.minute is not None:
            template += ", {minute} || ' minutes'"
        if self.phrase.second is not None:
            template += ", {second} || ' seconds'"
        template += ")"
        self.format(template, self.arguments)


class SQLiteDumpCombineDateTime(DumpCombineDateTime):

    template = "({date} || ' ' || {time})"


class SQLiteDumpExtractYear(DumpExtractYear):

    template = "CAST(STRFTIME('%Y', {op}) AS INTEGER)"


class SQLiteDumpExtractMonth(DumpExtractMonth):

    template = "CAST(STRFTIME('%m', {op}) AS INTEGER)"


class SQLiteDumpExtractDay(DumpExtractDay):

    template = "CAST(STRFTIME('%d', {op}) AS INTEGER)"


class SQLiteDumpExtractHour(DumpExtractHour):

    template = "CAST(STRFTIME('%H', {op}) AS INTEGER)"


class SQLiteDumpExtractMinute(DumpExtractMinute):

    template = "CAST(STRFTIME('%M', {op}) AS INTEGER)"


class SQLiteDumpExtractSecond(DumpExtractSecond):

    template = "CAST(STRFTIME('%f', {op}) AS REAL)"


class SQLiteDumpIsAny(DumpFunction):

    adapt(IsAnySig)
    template = "({lop} {polarity:not}IN {rop})"


