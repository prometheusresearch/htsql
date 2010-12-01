#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.fn.serialize`
============================
"""


from ...adapter import adapts
from ..serialize import SerializeBySignature
from .signature import (NumericAddSig, ConcatenateSig, DateIncrementSig,
                        NumericSubtractSig, DateDecrementSig,
                        DateDifferenceSig, NumericMultiplySig,
                        NumericDivideSig, IfSig, SwitchSig,
                        AmongSig, CompareSig, NumericReversePolaritySig,
                        RoundSig, RoundToSig, StringLengthSig,
                        WrapExistsSig, TakeCountSig, TakeMinSig, TakeMaxSig,
                        TakeSumSig, TakeAvgSig)


class SerializeFunction(SerializeBySignature):

    template = None

    def __call__(self):
        if self.template is None:
            raise NotImplementedError()
        self.state.format(self.template, self.arguments, self.signature)


class SerializeNumericAdd(SerializeFunction):

    adapts(NumericAddSig)
    template = "({lop} + {rop})"


class SerializeNumericSubtract(SerializeFunction):

    adapts(NumericSubtractSig)
    template = "({lop} - {rop})"


class SerializeNumericMultiply(SerializeFunction):

    adapts(NumericMultiplySig)
    template = "({lop} * {rop})"


class SerializeNumericDivide(SerializeFunction):

    adapts(NumericDivideSig)
    template = "({lop} / {rop})"


class SerializeConcatenate(SerializeFunction):

    adapts(ConcatenateSig)
    template = "({lop} || {rop})"


class SerializeIf(SerializeFunction):

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


class SerializeSwitch(SerializeFunction):

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


class SerializeAmong(SerializeFunction):

    adapts(AmongSig)
    template = "({lop} {polarity:polarity}IN ({rops:join{, }}))"


class SerializeCompare(SerializeFunction):

    adapts(CompareSig)
    template = "({lop} {relation:asis} {rop})"


class SerializeReversePolarity(SerializeFunction):

    adapts(NumericReversePolaritySig)
    template = "(- {op})"


class SerializeRound(SerializeFunction):

    adapts(RoundSig)
    template = "ROUND({op})"


class SerializeRoundTo(SerializeFunction):

    adapts(RoundToSig)
    template = "ROUND({op}, {precision})"


class SerializeStringLength(SerializeFunction):

    adapts(StringLengthSig)
    template = "CHARACTER_LENGTH({op})"


class SerializeWrapExists(SerializeFunction):

    adapts(WrapExistsSig)
    template = "EXISTS{op}"


class SerializeTakeCount(SerializeFunction):

    adapts(TakeCountSig)
    template = "COUNT({op})"


class SerializeTakeMin(SerializeFunction):

    adapts(TakeMinSig)
    template = "MIN({op})"


class SerializeTakeMax(SerializeFunction):

    adapts(TakeMaxSig)
    template = "MAX({op})"


class SerializeTakeSum(SerializeFunction):

    adapts(TakeSumSig)
    template = "SUM({op})"


class SerializeTakeAvg(SerializeFunction):

    adapts(TakeAvgSig)
    template = "AVG({op})"


