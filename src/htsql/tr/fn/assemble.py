#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.fn.assemble`
===========================
"""


from ...adapter import adapts, adapts_none
from ..assemble import EvaluateBySignature
from ..frame import FormulaPhrase
from .signature import (ConcatenateSig, ExistsSig, CountSig, ContainsSig,
                        LikeSig, IfSig, SwitchSig)


class EvaluateFunction(EvaluateBySignature):

    adapts_none()

    is_null_regular = True
    is_nullable = True
    is_predicate = False

    def __call__(self):
        arguments = self.arguments.map(self.state.evaluate)
        if self.is_null_regular:
            is_nullable = any(cell.is_nullable for cell in arguments.cells())
        else:
            is_nullable = self.is_nullable
        phrase = FormulaPhrase(self.signature,
                               self.domain,
                               is_nullable,
                               self.code,
                               **arguments)
        if self.is_predicate:
            phrase = self.state.from_predicate(phrase)
        return phrase


class EvaluateWrapExists(EvaluateFunction):

    adapts(ExistsSig)
    is_null_regular = False
    is_nullable = False
    is_predicate = True


class EvaluateTakeCount(EvaluateFunction):

    adapts(CountSig)
    is_null_regular = False
    is_nullable = False


class EvaluateConcatenate(EvaluateFunction):

    adapts(ConcatenateSig)
    is_null_regular = False
    is_nullable = False


class EvaluateContains(EvaluateFunction):

    adapts(ContainsSig)
    is_predicate = True


class EvalutateLike(EvaluateFunction):

    adapts(LikeSig)
    is_predicate = True


class EvaluateIf(EvaluateFunction):

    adapts(IfSig)

    def __call__(self):
        arguments = self.arguments.map(self.state.evaluate)
        predicates = arguments['predicates']
        consequents = arguments['consequents']
        alternative = arguments['alternative']
        predicates = [self.state.to_predicate(cell) for cell in predicates]
        is_nullable = any(cell.is_nullable for cell in consequents)
        if alternative is None or alternative.is_nullable:
            is_nullable = True
        phrase = FormulaPhrase(self.signature, self.domain, is_nullable,
                               self.code, predicates=predicates,
                               consequents=consequents,
                               alternative=alternative)
        return phrase


class EvaluateSwitch(EvaluateFunction):

    adapts(SwitchSig)

    def __call__(self):
        arguments = self.arguments.map(self.state.evaluate)
        consequents = arguments['consequents']
        alternative = arguments['alternative']
        is_nullable = any(cell.is_nullable for cell in consequents)
        if alternative is None or alternative.is_nullable:
            is_nullable = True
        phrase = FormulaPhrase(self.signature, self.domain, is_nullable,
                               self.code, **arguments)
        return phrase




