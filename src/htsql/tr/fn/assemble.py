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
from .signature import ConcatenateSig, ExistsSig, CountSig


class EvaluateFunction(EvaluateBySignature):

    adapts_none()

    is_null_regular = True
    is_nullable = True

    def __call__(self):
        arguments = self.arguments.map(self.state.evaluate)
        if self.is_null_regular:
            is_nullable = any(cell.is_nullable for cell in arguments.cells())
        else:
            is_nullable = self.is_nullable
        return FormulaPhrase(self.signature,
                             self.domain,
                             is_nullable,
                             self.code,
                             **arguments)


class EvaluateWrapExists(EvaluateFunction):

    adapts(ExistsSig)
    is_null_regular = False
    is_nullable = False


class EvaluateTakeCount(EvaluateFunction):

    adapts(CountSig)
    is_null_regular = False
    is_nullable = False


class EvaluateConcatenate(EvaluateFunction):

    adapts(ConcatenateSig)
    is_null_regular = False
    is_nullable = False


