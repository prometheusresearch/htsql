#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.fn.dump`
=======================
"""


from ...adapter import adapts
from ..dump import DumpBySignature
from .signature import (NumericAddSig, ConcatenateSig, DateIncrementSig,
                        NumericSubtractSig, DateDecrementSig,
                        DateDifferenceSig, NumericMultiplySig,
                        NumericDivideSig, IfSig, SwitchSig,
                        AmongSig, CompareSig, NumericReversePolaritySig,
                        RoundSig, RoundToSig, StringLengthSig,
                        WrapExistsSig, TakeCountSig, TakeMinSig, TakeMaxSig,
                        TakeSumSig, TakeAvgSig)


class DumpFunction(DumpBySignature):

    template = None

    def __call__(self):
        if self.template is None:
            raise NotImplementedError()
        self.state.format(self.template, self.arguments, self.signature)


class DumpNumericAdd(DumpFunction):

    adapts(NumericAddSig)
    template = "({lop} + {rop})"


class DumpNumericSubtract(DumpFunction):

    adapts(NumericSubtractSig)
    template = "({lop} - {rop})"


class DumpNumericMultiply(DumpFunction):

    adapts(NumericMultiplySig)
    template = "({lop} * {rop})"


class DumpNumericDivide(DumpFunction):

    adapts(NumericDivideSig)
    template = "({lop} / {rop})"


class DumpConcatenate(DumpFunction):

    adapts(ConcatenateSig)
    template = "({lop} || {rop})"


class DumpIf(DumpFunction):

    adapts(IfSig)

    def __call__(self):
        self.state.format("(CASE")
        for predicate, consequent in zip(self.phrase.predicates,
                                         self.phrase.consequents):
            self.state.format(" WHEN {predicate} THEN {consequent}",
                              predicate=predicate, consequent=consequent)
        if self.phrase.alternative is not None:
            self.state.format(" ELSE {alternative}",
                              alternative=self.phrase.alternative)
        self.state.format(" END)")


class DumpSwitch(DumpFunction):

    adapts(SwitchSig)

    def __call__(self):
        self.state.format("(CASE {variable}",
                          variable=self.phrase.variable)
        for variant, consequent in zip(self.phrase.variants,
                                       self.phrase.consequents):
            self.state.format(" WHEN {variant} THEN {consequent}",
                              variant=variant, consequent=consequent)
        if self.phrase.alternative is not None:
            self.state.format(" ELSE {alternative}",
                              alternative=self.phrase.alternative)
        self.state.format(" END)")


class DumpAmong(DumpFunction):

    adapts(AmongSig)
    template = "({lop} {polarity:polarity}IN ({rops:join{, }}))"


class DumpCompare(DumpFunction):

    adapts(CompareSig)
    template = "({lop} {relation:asis} {rop})"


class DumpReversePolarity(DumpFunction):

    adapts(NumericReversePolaritySig)
    template = "(- {op})"


class DumpRound(DumpFunction):

    adapts(RoundSig)
    template = "ROUND({op})"


class DumpRoundTo(DumpFunction):

    adapts(RoundToSig)
    template = "ROUND({op}, {precision})"


class DumpStringLength(DumpFunction):

    adapts(StringLengthSig)
    template = "CHARACTER_LENGTH({op})"


class DumpWrapExists(DumpFunction):

    adapts(WrapExistsSig)
    template = "EXISTS{op}"


class DumpTakeCount(DumpFunction):

    adapts(TakeCountSig)
    template = "COUNT({op})"


class DumpTakeMin(DumpFunction):

    adapts(TakeMinSig)
    template = "MIN({op})"


class DumpTakeMax(DumpFunction):

    adapts(TakeMaxSig)
    template = "MAX({op})"


class DumpTakeSum(DumpFunction):

    adapts(TakeSumSig)
    template = "SUM({op})"


class DumpTakeAvg(DumpFunction):

    adapts(TakeAvgSig)
    template = "AVG({op})"


