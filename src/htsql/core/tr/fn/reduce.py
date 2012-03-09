#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql.core.tr.fn.reduce`
==============================
"""


from ...adapter import adapt
from ..reduce import ReduceBySignature
from ..frame import LiteralPhrase, FormulaPhrase
from ..signature import IfNullSig
from .signature import KeepPolaritySig, ReversePolaritySig, ConcatenateSig


class ReduceKeepPolarity(ReduceBySignature):

    adapt(KeepPolaritySig)

    def __call__(self):
        return self.state.reduce(self.phrase.op)


class ReduceReversePolarity(ReduceBySignature):

    adapt(ReversePolaritySig)

    def __call__(self):
        op = self.state.reduce(self.phrase.op)
        if isinstance(op, LiteralPhrase):
            if op.value is None:
                return op
            return op.clone(value=-op.value, expression=self.phrase.expression)
        return self.phrase.clone(op=op)


class ReduceConcatenate(ReduceBySignature):

    adapt(ConcatenateSig)

    def __call__(self):
        empty = LiteralPhrase(u'', self.phrase.domain, self.phrase.expression)
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


