#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


from ...adapter import adapts, adapts_none
from ..code import LiteralCode, FormulaCode
from ..rewrite import RewriteBySignature
from .signature import SubstringSig


class RewriteFunction(RewriteBySignature):

    adapts_none()
    is_null_regular = False

    def __call__(self):
        arguments = self.arguments.map(self.state.rewrite)
        if self.is_null_regular:
            for cell in arguments.cells():
                if isinstance(cell, LiteralCode) and cell.value is None:
                    return LiteralCode(None, self.domain, self.code.binding)
        return FormulaCode(self.signature, self.domain,
                           self.code.binding, **arguments)


class RewriteSubstring(RewriteFunction):

    adapts(SubstringSig)
    is_null_regular = True


