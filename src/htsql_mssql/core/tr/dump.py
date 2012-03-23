#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from htsql.core.adapter import adapt
from htsql.core.domain import (BooleanDomain, StringDomain, IntegerDomain,
                               DecimalDomain, DateDomain, TimeDomain,
                               DateTimeDomain)
from htsql.core.tr.error import SerializeError
from htsql.core.tr.frame import ColumnPhrase, ReferencePhrase, LiteralPhrase
from htsql.core.tr.dump import (FormatName, DumpBranch, DumpBySignature,
                                DumpFromPredicate, DumpToPredicate,
                                DumpIsTotallyEqual, DumpBoolean, DumpInteger,
                                DumpDecimal, DumpFloat, DumpDate, DumpTime,
                                DumpDateTime, DumpToInteger,
                                DumpToFloat, DumpToDecimal, DumpToString,
                                DumpToDate, DumpToTime, DumpToDateTime)
from htsql.core.tr.fn.dump import (DumpRound, DumpRoundTo, DumpTrunc,
                                   DumpTruncTo, DumpLength, DumpConcatenate,
                                   DumpSubstring, DumpTrim, DumpToday, DumpNow,
                                   DumpExtractYear, DumpExtractMonth,
                                   DumpExtractDay, DumpExtractHour,
                                   DumpExtractMinute, DumpExtractSecond,
                                   DumpMakeDate, DumpMakeDateTime,
                                   DumpDateIncrement, DumpDateDecrement,
                                   DumpDateTimeIncrement,
                                   DumpDateTimeDecrement, DumpDateDifference)
import math


class MSSQLFormatName(FormatName):

    def __call__(self):
        self.stream.write(u"[%s]" % self.value.replace(u"]", u"]]"))


class MSSQLDumpBranch(DumpBranch):

    def dump_select(self):
        aliases = self.state.select_aliases_by_tag[self.frame.tag]
        self.write(u"SELECT ")
        self.indent()
        if self.frame.limit is not None:
            self.write(u"TOP "+unicode(self.frame.limit))
            self.newline()
        for index, phrase in enumerate(self.frame.select):
            alias = None
            if self.state.hook.with_aliases:
                alias = aliases[index]
                if isinstance(phrase, ColumnPhrase):
                    if alias == phrase.column.name:
                        alias = None
                if isinstance(phrase, ReferencePhrase):
                    target_alias = (self.state.select_aliases_by_tag
                                            [phrase.tag][phrase.index])
                    if alias == target_alias:
                        alias = None
            if alias is not None:
                self.format("{selection} AS {alias:name}",
                            selection=phrase, alias=alias)
            else:
                self.format("{selection}",
                            selection=phrase)
            if index < len(self.frame.select)-1:
                self.write(u",")
                self.newline()
        self.dedent()

    def dump_limit(self):
        assert self.frame.offset is None


class MSSQLDumpFromPredicate(DumpFromPredicate):

    def __call__(self):
        if self.phrase.is_nullable:
            self.format("(CASE WHEN {op} THEN 1 WHEN NOT {op} THEN 0 END)",
                        self.arguments)
        else:
            self.format("(CASE WHEN {op} THEN 1 ELSE 0 END)",
                        self.arguments)


class MSSQLDumpToPredicate(DumpToPredicate):

    def __call__(self):
        self.format("({op} <> 0)", self.arguments)


class MSSQLDumpBoolean(DumpBoolean):

    def __call__(self):
        if self.value is True:
            self.write(u"1")
        if self.value is False:
            self.write(u"0")


class MSSQLDumpInteger(DumpInteger):

    def __call__(self):
        if not (-2**63 <= self.value < 2**63):
            raise SerializeError("invalid integer value",
                                 self.phrase.mark)
        if abs(self.value) < 2**31:
            if self.value >= 0:
                self.write(unicode(self.value))
            else:
                self.write(u"(%s)" % self.value)
        else:
            self.write(u"CAST(%s AS BIGINT)" % self.value)


class MSSQLDumpFloat(DumpFloat):

    def __call__(self):
        assert not math.isinf(self.value) and not math.isnan(self.value)
        value = repr(self.value)
        if 'e' not in value and 'E' not in value:
            value = value+'e0'
        if value[0] == '-':
            value = "(%s)" % value
        self.write(unicode(value))


class MSSQLDumpDecimal(DumpDecimal):

    def __call__(self):
        assert self.value.is_finite()
        value = str(self.value)
        if 'E' in value:
            value = "CAST(%s AS DECIMAL(38,19))" % value
        elif '.' not in value:
            value = "%s." % value
        if value[0] == '-':
            value = "(%s)" % value
        self.write(unicode(value))


class MSSQLDumpDate(DumpDate):

    def __call__(self):
        self.format("CAST({value:literal} AS DATETIME)",
                    value=unicode(self.value))


class MSSQLDumpTime(DumpTime):

    def __call__(self):
        value = (self.value.hour*3600 + self.value.minute*60 +
                 self.value.second + self.value.microsecond/1000000.0) / 86400.0
        value = repr(value)
        if 'e' not in value and 'E' not in value:
            value = value+'e0'
        self.write(unicode(value))


class MSSQLDumpDateTime(DumpDateTime):

    def __call__(self):
        value = self.value.replace(tzinfo=None)
        if not value.microsecond:
            value = unicode(value)
        else:
            value = unicode(value)[:-3]
        self.format("CAST({value:literal} AS DATETIME)", value=value)


class MSSQLDumpToInteger(DumpToInteger):

    def __call__(self):
        self.format("CAST({base} AS INT)", base=self.base)


class MSSQLDumpToFloat(DumpToFloat):

    def __call__(self):
        self.format("CAST({base} AS FLOAT)", base=self.base)


class MSSQLDumpToDecimal(DumpToDecimal):

    def __call__(self):
        self.format("CAST({base} AS DECIMAL(38,19))", base=self.base)


class MSSQLDumpIntegerToDecimal(MSSQLDumpToDecimal):

    adapt(IntegerDomain, DecimalDomain)

    def __call__(self):
        self.format("CAST({base} AS DECIMAL(38))", base=self.base)


class MSSQLDumpToString(DumpToString):

    def __call__(self):
        self.format("CAST({base} AS VARCHAR(MAX))", base=self.base)


class MSSQLDumpBooleanToString(DumpToString):

    adapt(BooleanDomain, StringDomain)

    def __call__(self):
        if self.base.is_nullable:
            self.format("(CASE WHEN {base} <> 0 THEN 'true'"
                        " WHEN NOT {base} = 0 THEN 'false' END)",
                        base=self.base)
        else:
            self.format("(CASE WHEN {base} <> 0 THEN 'true' ELSE 'false' END)",
                        base=self.base)


class MSSQLDumpDateToString(DumpToString):

    adapt(DateDomain, StringDomain)

    def __call__(self):
        self.format("SUBSTRING(CONVERT(VARCHAR, {base}, 21), 1, 10)",
                    base=self.base)


class MSSQLDumpTimeToString(DumpToString):

    adapt(TimeDomain, StringDomain)

    def __call__(self):
        self.format("SUBSTRING(CONVERT(VARCHAR, CAST({base} AS DATETIME), 21),"
                    " 12, 12)", base=self.base)


class MSSQLDumpDateTimeToString(DumpToString):

    adapt(DateTimeDomain, StringDomain)

    def __call__(self):
        self.format("CONVERT(VARCHAR, {base}, 21)", base=self.base)


class MSSQLDumpStringToDate(DumpToDate):

    adapt(StringDomain, DateDomain)

    def __call__(self):
        self.format("CAST(FLOOR(CAST(CAST({base} AS DATETIME) AS FLOAT))"
                    " AS DATETIME)", base=self.base)


class MSSQLDumpDateTimeToDate(DumpToDate):

    adapt(DateTimeDomain, DateDomain)

    def __call__(self):
        self.format("CAST(FLOOR(CAST({base} AS FLOAT)) AS DATETIME)",
                    base=self.base)


class MSSQLDumpStringToTime(DumpToTime):

    adapt(StringDomain, TimeDomain)

    def __call__(self):
        self.format("CAST(CAST('1900-01-01 ' + {base} AS DATETIME) AS FLOAT)",
                    base=self.base)


class MSSQLDumpDateTimeToTime(DumpToTime):

    adapt(DateTimeDomain, TimeDomain)

    def __call__(self):
        self.format("(CAST({base} AS FLOAT) - FLOOR(CAST({base} AS FLOAT)))",
                    base=self.base)


class MSSQLDumpStringToDateTime(DumpToDateTime):

    adapt(StringDomain, DateTimeDomain)

    def __call__(self):
        self.format("CAST({base} AS DATETIME)", base=self.base)


class MSSQLDumpDateToDateTime(DumpToDateTime):

    adapt(DateDomain, DateTimeDomain)

    def __call__(self):
        self.format("{base}", base=self.base)


class MSSQLDumpIsTotallyEqual(DumpIsTotallyEqual):

    def __call__(self):
        self.format("((CASE WHEN ({lop} = {rop}) OR"
                    " ({lop} IS NULL AND {rop} IS NULL)"
                    " THEN 1 ELSE 0 END) {polarity:switch{<>|=}} 0)",
                    self.arguments, self.signature)


class MSSQLDumpRound(DumpRound):

    def __call__(self):
        if isinstance(self.phrase.domain, DecimalDomain):
            self.format("CAST(ROUND({op}, 0) AS DECIMAL(38))", self.arguments)
        else:
            self.format("ROUND({op}, 0)", self.arguments)


class MSSQLDumpRoundTo(DumpRoundTo):

    def __call__(self):
        scale = None
        if (isinstance(self.phrase.precision, LiteralPhrase) and
            self.phrase.precision.value is not None):
            scale = self.phrase.precision.value
            if scale < 0:
                scale = 0
        if scale is not None:
            self.format("CAST(ROUND({op}, {precision})"
                        " AS DECIMAL(38,{scale:pass}))",
                        self.arguments, self.signature, scale=unicode(scale))
        else:
            self.format("ROUND({op}, {precision})",
                        self.arguments, self.signature)


class MSSQLDumpTrunc(DumpTrunc):

    def __call__(self):
        if isinstance(self.phrase.domain, DecimalDomain):
            self.format("CAST(ROUND({op} -"
                        " (CASE WHEN {op} >= 0 THEN 0.5 ELSE -0.5 END), 0)"
                        " AS DECIMAL(38))", self.arguments)
        else:
            self.format("ROUND({op} -"
                        " (CASE WHEN {op} >= 0 THEN 0.5 ELSE -0.5 END), 0)",
                        self.arguments)


class MSSQLDumpTruncTo(DumpTruncTo):

    def __call__(self):
        scale = None
        if (isinstance(self.phrase.precision, LiteralPhrase) and
            self.phrase.precision.value is not None):
            scale = self.phrase.precision.value
            if scale < 0:
                scale = 0
        if scale is not None:
            self.format("CAST(ROUND({op} -"
                        " (CASE WHEN {op} >= 0 THEN 0.5 ELSE -0.5 END) /"
                        " CAST(POWER(10e0, {precision}) AS DECIMAL(38,19)),"
                        " {precision})"
                        " AS DECIMAL(38,{scale:pass}))",
                        self.arguments, self.signature, scale=unicode(scale))
        else:
            self.format("ROUND({op} -"
                        " (CASE WHEN {op} >= 0 THEN 0.5 ELSE -0.5 END) /"
                        " CAST(POWER(10e0, {precision}) AS DECIMAL(38,19)),"
                        " {precision})", self.arguments, self.signature)


class MSSQLDumpLength(DumpLength):

    template = "LEN({op})"


class MSSQLDumpConcatenate(DumpConcatenate):

    template = "({lop} + {rop})"


class MSSQLDumpSubstring(DumpSubstring):

    def __call__(self):
        if self.phrase.length is None:
            self.format("SUBSTRING({op}, {start}, LEN({op}))", self.phrase)
        else:
            self.format("SUBSTRING({op}, {start}, {length})", self.phrase)


class MSSQLDumpTrim(DumpTrim):

    def __call__(self):
        if self.signature.is_left and not self.signature.is_right:
            self.format("LTRIM({op})", self.arguments)
        elif not self.signature.is_left and self.signature.is_right:
            self.format("RTRIM({op})", self.arguments)
        else:
            self.format("LTRIM(RTRIM({op}))", self.arguments)


class MSSQLDumpToday(DumpToday):

    template = "CAST(FLOOR(CAST(GETDATE() AS FLOAT)) AS DATETIME)"


class MSSQLDumpNow(DumpNow):

    template = "GETDATE()"


class MSSQLDumpExtractYear(DumpExtractYear):

    template = "DATEPART(YEAR, {op})"


class MSSQLDumpExtractMonth(DumpExtractMonth):

    template = "DATEPART(MONTH, {op})"


class MSSQLDumpExtractDay(DumpExtractDay):

    template = "DATEPART(DAY, {op})"


class MSSQLDumpExtractHour(DumpExtractHour):

    template = "DATEPART(HOUR, {op})"


class MSSQLDumpExtractMinute(DumpExtractMinute):

    template = "DATEPART(MINUTE, {op})"


class MSSQLDumpExtractSecond(DumpExtractSecond):

    template = ("(DATEPART(SECOND, {op}) +"
                " DATEPART(MILLISECOND, {op}) / 1000e0)")


class MSSQLDumpMakeDate(DumpMakeDate):

    template = ("DATEADD(DAY, {day} - 1,"
                " DATEADD(MONTH, {month} - 1,"
                " DATEADD(YEAR, {year} - 2001,"
                " CAST('2001-01-01' AS DATETIME))))")


class MSSQLDumpMakeDateTime(DumpMakeDateTime):

    def __call__(self):
        template = ("DATEADD(DAY, {day} - 1,"
                    " DATEADD(MONTH, {month} - 1,"
                    " DATEADD(YEAR, {year} - 2001,"
                    " CAST('2001-01-01' AS DATETIME))))")
        if self.phrase.hour is not None:
            template = "DATEADD(HOUR, {hour}, %s)" % template
        if self.phrase.minute is not None:
            template = "DATEADD(MINUTE, {minute}, %s)" % template
        if self.phrase.second is not None:
            template = "DATEADD(MILLISECOND, 1000 * {second}, %s)" % template
        self.format(template, self.arguments)


class MSSQLDumpDateIncrement(DumpDateIncrement):

    template = "({lop} + {rop})"


class MSSQLDumpDateTimeIncrement(DumpDateTimeIncrement):

    template = "({lop} + {rop})"


class MSSQLDumpDateDecrement(DumpDateDecrement):

    template = "({lop} - {rop})"


class MSSQLDumpDateTimeDecrement(DumpDateTimeDecrement):

    template = "({lop} - {rop})"


class MSSQLDumpDateDifference(DumpDateDifference):

    template = "DATEDIFF(DAY, {rop}, {lop})"


