#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ...adapter import adapt, adapt_none
from ..flow import LiteralCode, FormulaCode
from ..rewrite import RewriteBySignature
from .signature import SubstringSig


class RewriteFunction(RewriteBySignature):

    adapt_none()
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

    adapt(SubstringSig)
    is_null_regular = True


