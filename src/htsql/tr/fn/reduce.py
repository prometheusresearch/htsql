#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.fn.reduce`
=========================
"""


from ...adapter import adapts
from ..reduce import ReduceBySignature
from ..frame import LiteralPhrase, FormulaPhrase
from ..signature import IfNullSig
from .signature import KeepPolaritySig, ConcatenateSig


class ReduceKeepPolaritySig(ReduceBySignature):

    adapts(KeepPolaritySig)

    def __call__(self):
        return self.state.reduce(self.phrase.op)


class ReduceConcatenate(ReduceBySignature):

    adapts(ConcatenateSig)

    def __call__(self):
        empty = LiteralPhrase('', self.phrase.domain, self.phrase.expression)
        lop = self.phrase.lop
        if lop.is_nullable:
            lop = FormulaPhrase(IfNullSig(), lop.domain, False,
                                lop.expression, lop=lop, rop=empty)
        rop = self.phrase.rop
        if rop.is_nullable:
            rop = FormulaPhrase(IfNullSig(), rop.domain, False,
                                rop.expression, lop=rop, rop=empty)
        return FormulaPhrase(self.phrase.signature, self.phrase.domain,
                             False, self.phrase.expression,
                             lop=self.state.reduce(lop),
                             rop=self.state.reduce(rop))


