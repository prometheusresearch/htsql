#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from htsql.core.tr.space import LiteralCode, FormulaCode
from htsql.core.tr.fn.signature import ReplaceSig, ConcatenateSig, LikeSig
from htsql.core.tr.fn.encode import EncodeContains


class MSSQLEncodeContains(EncodeContains):

    def __call__(self):
        lop = self.state.encode(self.flow.lop)
        rop = self.state.encode(self.flow.rop)
        if isinstance(rop, LiteralCode):
            if rop.value is not None:
                value = ("%" + rop.value.replace("\\", "\\\\")
                                         .replace("[", "\\[")
                                         .replace("]", "\\]")
                                         .replace("%", "\\%")
                                         .replace("_", "\\_") + "%")
                rop = rop.clone(value=value)
        else:
            backslash_literal = LiteralCode("\\", rop.domain, self.flow)
            xbackslash_literal = LiteralCode("\\\\", rop.domain, self.flow)
            lbracket_literal = LiteralCode("[", rop.domain, self.flow)
            xlbracket_literal = LiteralCode("\\[", rop.domain, self.flow)
            rbracket_literal = LiteralCode("]", rop.domain, self.flow)
            xrbracket_literal = LiteralCode("\\]", rop.domain, self.flow)
            percent_literal = LiteralCode("%", rop.domain, self.flow)
            xpercent_literal = LiteralCode("\\%", rop.domain, self.flow)
            underscore_literal = LiteralCode("_", rop.domain, self.flow)
            xunderscore_literal = LiteralCode("\\_", rop.domain, self.flow)
            rop = FormulaCode(ReplaceSig(), rop.domain, self.flow,
                              op=rop, old=backslash_literal,
                              new=xbackslash_literal)
            rop = FormulaCode(ReplaceSig(), rop.domain, self.flow,
                              op=rop, old=lbracket_literal,
                              new=xlbracket_literal)
            rop = FormulaCode(ReplaceSig(), rop.domain, self.flow,
                              op=rop, old=rbracket_literal,
                              new=xrbracket_literal)
            rop = FormulaCode(ReplaceSig(), rop.domain, self.flow,
                              op=rop, old=percent_literal,
                              new=xpercent_literal)
            rop = FormulaCode(ReplaceSig(), rop.domain, self.flow,
                              op=rop, old=underscore_literal,
                              new=xunderscore_literal)
            rop = FormulaCode(ConcatenateSig(), rop.domain, self.flow,
                              lop=percent_literal, rop=rop)
            rop = FormulaCode(ConcatenateSig(), rop.domain, self.flow,
                              lop=rop, rop=percent_literal)
        return FormulaCode(self.signature.clone_to(LikeSig),
                           self.domain, self.flow, lop=lop, rop=rop)


