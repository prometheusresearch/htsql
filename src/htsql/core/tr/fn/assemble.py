#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql.core.tr.fn.assemble`
================================
"""


from ...adapter import adapt, adapt_none
from ..assemble import EvaluateBySignature
from ..frame import FormulaPhrase
from .signature import (ConcatenateSig, ExistsSig, CountSig, ContainsSig,
                        LikeSig, IfSig, SwitchSig)


class EvaluateFunction(EvaluateBySignature):

    adapt_none()

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
        yield phrase


class EvaluateWrapExists(EvaluateFunction):

    adapt(ExistsSig)
    is_null_regular = False
    is_nullable = False
    is_predicate = True


class EvaluateTakeCount(EvaluateFunction):

    adapt(CountSig)
    is_null_regular = False
    is_nullable = False


class EvaluateConcatenate(EvaluateFunction):

    adapt(ConcatenateSig)
    is_null_regular = False
    is_nullable = False


class EvaluateContains(EvaluateFunction):

    adapt(ContainsSig)
    is_predicate = True


class EvalutateLike(EvaluateFunction):

    adapt(LikeSig)
    is_predicate = True


class EvaluateIf(EvaluateFunction):

    adapt(IfSig)

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
        yield phrase


class EvaluateSwitch(EvaluateFunction):

    adapt(SwitchSig)

    def __call__(self):
        arguments = self.arguments.map(self.state.evaluate)
        consequents = arguments['consequents']
        alternative = arguments['alternative']
        is_nullable = any(cell.is_nullable for cell in consequents)
        if alternative is None or alternative.is_nullable:
            is_nullable = True
        phrase = FormulaPhrase(self.signature, self.domain, is_nullable,
                               self.code, **arguments)
        yield phrase


