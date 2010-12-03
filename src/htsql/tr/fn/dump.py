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
from .signature import (AddSig, ConcatenateSig, DateIncrementSig,
                        SubtractSig, DateDecrementSig, DateDifferenceSig,
                        MultiplySig, DivideSig, IfSig, SwitchSig,
                        ReversePolaritySig,
                        RoundSig, RoundToSig, LengthSig,
                        WrapExistsSig, TakeCountSig, TakeMinSig, TakeMaxSig,
                        TakeSumSig, TakeAvgSig)


class DumpFunction(DumpBySignature):

    template = None

    def __call__(self):
        if self.template is None:
            print self.phrase, self.phrase.signature
            raise NotImplementedError()
        self.state.format(self.template, self.arguments, self.signature)


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


