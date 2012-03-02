#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from htsql.core.tr.flow import LiteralCode, FormulaCode
from htsql.core.tr.fn.signature import ReplaceSig, ConcatenateSig, LikeSig
from htsql.core.tr.fn.encode import EncodeContains


class MSSQLEncodeContains(EncodeContains):

    def __call__(self):
        lop = self.state.encode(self.binding.lop)
        rop = self.state.encode(self.binding.rop)
        if isinstance(rop, LiteralCode):
            if rop.value is not None:
                value = (u"%" + rop.value.replace(u"\\", u"\\\\")
                                         .replace(u"[", u"\\[")
                                         .replace(u"]", u"\\]")
                                         .replace(u"%", u"\\%")
                                         .replace(u"_", u"\\_") + u"%")
                rop = rop.clone(value=value)
        else:
            backslash_literal = LiteralCode(u"\\", rop.domain, self.binding)
            xbackslash_literal = LiteralCode(u"\\\\", rop.domain, self.binding)
            lbracket_literal = LiteralCode(u"[", rop.domain, self.binding)
            xlbracket_literal = LiteralCode(u"\\[", rop.domain, self.binding)
            rbracket_literal = LiteralCode(u"]", rop.domain, self.binding)
            xrbracket_literal = LiteralCode(u"\\]", rop.domain, self.binding)
            percent_literal = LiteralCode(u"%", rop.domain, self.binding)
            xpercent_literal = LiteralCode(u"\\%", rop.domain, self.binding)
            underscore_literal = LiteralCode(u"_", rop.domain, self.binding)
            xunderscore_literal = LiteralCode(u"\\_", rop.domain, self.binding)
            rop = FormulaCode(ReplaceSig(), rop.domain, self.binding,
                              op=rop, old=backslash_literal,
                              new=xbackslash_literal)
            rop = FormulaCode(ReplaceSig(), rop.domain, self.binding,
                              op=rop, old=lbracket_literal,
                              new=xlbracket_literal)
            rop = FormulaCode(ReplaceSig(), rop.domain, self.binding,
                              op=rop, old=rbracket_literal,
                              new=xrbracket_literal)
            rop = FormulaCode(ReplaceSig(), rop.domain, self.binding,
                              op=rop, old=percent_literal,
                              new=xpercent_literal)
            rop = FormulaCode(ReplaceSig(), rop.domain, self.binding,
                              op=rop, old=underscore_literal,
                              new=xunderscore_literal)
            rop = FormulaCode(ConcatenateSig(), rop.domain, self.binding,
                              lop=percent_literal, rop=rop)
            rop = FormulaCode(ConcatenateSig(), rop.domain, self.binding,
                              lop=rop, rop=percent_literal)
        return FormulaCode(self.signature.clone_to(LikeSig),
                           self.domain, self.binding, lop=lop, rop=rop)


