#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.fn.reduce`
=========================
"""


from ...adapter import adapts
from ..reduce import ReduceBySignature
from ..frame import (IsNullPhrase, NullIfPhrase, IfNullPhrase,
                     LiteralPhrase, FunctionPhrase)
from .signature import (IsNullSig, NullIfSig, IfNullSig,
                        NumericKeepPolaritySig, ConcatenateSig)


class ReduceIsNull(ReduceBySignature):

    adapts(IsNullSig)

    def __call__(self):
        phrase = IsNullPhrase(self.phrase.op, self.phrase.expression)
        return self.state.reduce(phrase)


class ReduceNullIfSig(ReduceBySignature):

    adapts(NullIfSig)

    def __call__(self):
        phrase = self.phrase.lop
        for rop in self.phrase.rops:
            phrase = NullIfPhrase(phrase, rop, self.phrase.domain,
                                  self.phrase.expression)
        return self.state.reduce(phrase)


class ReduceIfNullSig(ReduceBySignature):

    adapts(IfNullSig)

    def __call__(self):
        phrase = self.phrase.lop
        for rop in self.phrase.rops:
            phrase = IfNullPhrase(phrase, rop, self.phrase.domain,
                                  self.phrase.expression)
        return self.state.reduce(phrase)


class ReduceKeepPolaritySig(ReduceBySignature):

    adapts(NumericKeepPolaritySig)

    def __call__(self):
        return self.state.reduce(self.phrase.op)


class ReduceConcatenate(ReduceBySignature):

    adapts(ConcatenateSig)

    def __call__(self):
        empty = LiteralPhrase('', self.phrase.domain, self.phrase.expression)
        lop = self.phrase.lop
        if lop.is_nullable:
            lop = IfNullPhrase(lop, empty, lop.domain, lop.expression)
        rop = self.phrase.rop
        if rop.is_nullable:
            rop = IfNullPhrase(rop, empty, rop.domain, rop.expression)
        return FunctionPhrase(self.phrase.signature, self.phrase.domain,
                              False, self.phrase.expression,
                              lop=self.state.reduce(lop),
                              rop=self.state.reduce(rop))


