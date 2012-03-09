#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql.core.tr.fn.dump`
============================
"""


from ...adapter import adapt, adapt_none
from ..dump import DumpBySignature
from .signature import (AddSig, ConcatenateSig, DateIncrementSig,
                        DateTimeIncrementSig, SubtractSig, DateDecrementSig,
                        DateTimeDecrementSig, DateDifferenceSig,
                        MultiplySig, DivideSig, IfSig, SwitchSig,
                        ReversePolaritySig, RoundSig, RoundToSig, TruncSig,
                        TruncToSig, LengthSig, LikeSig, ReplaceSig,
                        SubstringSig, UpperSig, LowerSig, TrimSig, TodaySig,
                        NowSig, MakeDateSig, MakeDateTimeSig,
                        CombineDateTimeSig, ExtractYearSig, ExtractMonthSig,
                        ExtractDaySig, ExtractHourSig, ExtractMinuteSig,
                        ExtractSecondSig, ExistsSig, CountSig, MinMaxSig,
                        SumSig, AvgSig)


class DumpFunction(DumpBySignature):

    adapt_none()
    template = None

    def __call__(self):
        if self.template is None:
            super(DumpFunction, self).__call__()
        else:
            self.format(self.template, self.arguments, self.signature)


class DumpAdd(DumpFunction):

    adapt(AddSig)
    template = "({lop} + {rop})"


class DumpSubtract(DumpFunction):

    adapt(SubtractSig)
    template = "({lop} - {rop})"


class DumpMultiply(DumpFunction):

    adapt(MultiplySig)
    template = "({lop} * {rop})"


class DumpDivide(DumpFunction):

    adapt(DivideSig)
    template = "({lop} / {rop})"


class DumpDateIncrement(DumpFunction):

    adapt(DateIncrementSig)
    template = "CAST({lop} + {rop} * INTERVAL '1' DAY AS DATE)"


class DumpDateTimeIncrement(DumpFunction):

    adapt(DateTimeIncrementSig)
    template = "({lop} + {rop} * INTERVAL '1' DAY)"


class DumpDateDecrement(DumpFunction):

    adapt(DateDecrementSig)
    template = "CAST({lop} - {rop} * INTERVAL '1' DAY AS DATE)"


class DumpDateTimeDecrement(DumpFunction):

    adapt(DateTimeDecrementSig)
    template = "({lop} - {rop} * INTERVAL '1' DAY)"


class DumpDateDifference(DumpFunction):

    adapt(DateDifferenceSig)
    template = "EXTRACT(DAY FROM {lop} - {rop})"


class DumpConcatenate(DumpFunction):

    adapt(ConcatenateSig)
    template = "({lop} || {rop})"


class DumpIf(DumpFunction):

    adapt(IfSig)

    def __call__(self):
        self.format("(CASE")
        for predicate, consequent in zip(self.phrase.predicates,
                                         self.phrase.consequents):
            self.format(" WHEN {predicate} THEN {consequent}",
                        predicate=predicate, consequent=consequent)
        if self.phrase.alternative is not None:
            self.format(" ELSE {alternative}",
                        alternative=self.phrase.alternative)
        self.format(" END)")


class DumpSwitch(DumpFunction):

    adapt(SwitchSig)

    def __call__(self):
        self.format("(CASE {variable}",
                    variable=self.phrase.variable)
        for variant, consequent in zip(self.phrase.variants,
                                       self.phrase.consequents):
            self.format(" WHEN {variant} THEN {consequent}",
                        variant=variant, consequent=consequent)
        if self.phrase.alternative is not None:
            self.format(" ELSE {alternative}",
                        alternative=self.phrase.alternative)
        self.format(" END)")


class DumpReversePolarity(DumpFunction):

    adapt(ReversePolaritySig)
    template = "(- {op})"


class DumpRound(DumpFunction):

    adapt(RoundSig)
    template = "ROUND({op})"


class DumpRoundTo(DumpFunction):

    adapt(RoundToSig)
    template = "ROUND({op}, {precision})"


class DumpTrunc(DumpFunction):

    adapt(TruncSig)
    template = "TRUNC({op})"


class DumpTruncTo(DumpFunction):

    adapt(TruncToSig)
    template = "TRUNC({op}, {precision})"


class DumpLength(DumpFunction):

    adapt(LengthSig)
    template = "CHARACTER_LENGTH({op})"


class DumpLike(DumpFunction):

    adapt(LikeSig)

    def __call__(self):
        self.format("({lop} {polarity:not}LIKE {rop} ESCAPE {escape:literal})",
                    self.arguments, self.signature, escape=u"\\")


class DumpReplace(DumpFunction):

    adapt(ReplaceSig)
    template = "REPLACE({op}, {old}, {new})"


class DumpSubstring(DumpFunction):

    adapt(SubstringSig)

    def __call__(self):
        if self.phrase.length is not None:
            self.format("SUBSTRING({op} FROM {start} FOR {length})",
                        self.phrase)
        else:
            self.format("SUBSTRING({op} FROM {start})", self.arguments)


class DumpUpper(DumpFunction):

    adapt(UpperSig)
    template = "UPPER({op})"


class DumpLower(DumpFunction):

    adapt(LowerSig)
    template = "LOWER({op})"


class DumpTrim(DumpFunction):

    adapt(TrimSig)

    def __call__(self):
        if self.signature.is_left and not self.signature.is_right:
            self.format("TRIM(LEADING FROM {op})", self.arguments)
        elif not self.signature.is_left and self.signature.is_right:
            self.format("TRIM(TRAILING FROM {op})", self.arguments)
        else:
            self.format("TRIM({op})", self.arguments)


class DumpToday(DumpFunction):

    adapt(TodaySig)
    template = "CURRENT_DATE"


class DumpNow(DumpFunction):

    adapt(NowSig)
    template = "LOCALTIMESTAMP"


class DumpMakeDate(DumpFunction):

    adapt(MakeDateSig)
    template = ("CAST(DATE '2001-01-01' + ({year} - 2001) * INTERVAL '1' YEAR"
                " + ({month} - 1) * INTERVAL '1' MONTH"
                " + ({day} - 1) * INTERVAL '1' DAY AS DATE)")


class DumpMakeDateTime(DumpFunction):

    adapt(MakeDateTimeSig)

    def __call__(self):
        template = ("(TIMESTAMP '2001-01-01 00:00:00'"
                    " + ({year} - 2001) * INTERVAL '1' YEAR"
                    " + ({month} - 1) * INTERVAL '1' MONTH"
                    " + ({day} - 1) * INTERVAL '1' DAY")
        if self.phrase.hour is not None:
            template += " + {hour} * INTERVAL '1' HOUR"
        if self.phrase.minute is not None:
            template += " + {minute} * INTERVAL '1' MINUTE"
        if self.phrase.second is not None:
            template += " + {second} * INTERVAL '1' SECOND"
        template += ")"
        self.format(template, self.arguments)


class DumpCombineDateTime(DumpFunction):

    adapt(CombineDateTimeSig)
    template = "({date} + {time})"


class DumpExtractYear(DumpFunction):

    adapt(ExtractYearSig)
    template = "EXTRACT(YEAR FROM {op})"


class DumpExtractMonth(DumpFunction):

    adapt(ExtractMonthSig)
    template = "EXTRACT(MONTH FROM {op})"


class DumpExtractDay(DumpFunction):

    adapt(ExtractDaySig)
    template = "EXTRACT(DAY FROM {op})"


class DumpExtractHour(DumpFunction):

    adapt(ExtractHourSig)
    template = "EXTRACT(HOUR FROM {op})"


class DumpExtractMinute(DumpFunction):

    adapt(ExtractMinuteSig)
    template = "EXTRACT(MINUTE FROM {op})"


class DumpExtractSecond(DumpFunction):

    adapt(ExtractSecondSig)
    template = "EXTRACT(SECOND FROM {op})"


class DumpExists(DumpFunction):

    adapt(ExistsSig)
    template = "EXISTS{op}"


class DumpCount(DumpFunction):

    adapt(CountSig)
    template = "COUNT({op})"


class DumpMinMax(DumpFunction):

    adapt(MinMaxSig)

    def __call__(self):
        if self.signature.polarity > 0:
            self.format("MIN({op})", self.arguments)
        else:
            self.format("MAX({op})", self.arguments)


class DumpSum(DumpFunction):

    adapt(SumSig)
    template = "SUM({op})"


class DumpAvg(DumpFunction):

    adapt(AvgSig)
    template = "AVG({op})"


