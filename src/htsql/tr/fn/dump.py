#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.fn.dump`
=======================
"""


from ...adapter import adapts, adapts_none
from ..dump import DumpBySignature
from .signature import (AddSig, ConcatenateSig, DateIncrementSig,
                        SubtractSig, DateDecrementSig, DateDifferenceSig,
                        MultiplySig, DivideSig, IfSig, SwitchSig,
                        ReversePolaritySig,
                        RoundSig, RoundToSig, LengthSig,
                        ExistsSig, CountSig, MinMaxSig, SumSig, AvgSig)


class DumpFunction(DumpBySignature):

    adapts_none()
    template = None

    def __call__(self):
        if self.template is None:
            super(DumpFunction, self).__call__()
        else:
            self.format(self.template, self.arguments, self.signature)


class DumpAdd(DumpFunction):

    adapts(AddSig)
    template = "({lop} + {rop})"


class DumpSubtract(DumpFunction):

    adapts(SubtractSig)
    template = "({lop} - {rop})"


class DumpMultiply(DumpFunction):

    adapts(MultiplySig)
    template = "({lop} * {rop})"


class DumpDivide(DumpFunction):

    adapts(DivideSig)
    template = "({lop} / {rop})"


class DumpConcatenate(DumpFunction):

    adapts(ConcatenateSig)
    template = "({lop} || {rop})"


class DumpIf(DumpFunction):

    adapts(IfSig)

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

    adapts(SwitchSig)

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

    adapts(ReversePolaritySig)
    template = "(- {op})"


class DumpRound(DumpFunction):

    adapts(RoundSig)
    template = "ROUND({op})"


class DumpRoundTo(DumpFunction):

    adapts(RoundToSig)
    template = "ROUND({op}, {precision})"


class DumpLength(DumpFunction):

    adapts(LengthSig)
    template = "CHARACTER_LENGTH({op})"


class DumpExists(DumpFunction):

    adapts(ExistsSig)
    template = "EXISTS{op}"


class DumpCount(DumpFunction):

    adapts(CountSig)
    template = "COUNT({op})"


class DumpMinMax(DumpFunction):

    adapts(MinMaxSig)

    def __call__(self):
        if self.signature.polarity > 0:
            self.format("MIN({op})", self.arguments)
        else:
            self.format("MAX({op})", self.arguments)


class DumpSum(DumpFunction):

    adapts(SumSig)
    template = "SUM({op})"


class DumpAvg(DumpFunction):

    adapts(AvgSig)
    template = "AVG({op})"


