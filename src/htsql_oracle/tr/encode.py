#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


from htsql.adapter import adapts
from htsql.domain import StringDomain
from htsql.tr.code import LiteralCode, FormulaCode
from htsql.tr.encode import EncodeLiteral
from htsql.tr.fn.encode import EncodeFunction
from htsql.tr.signature import IfNullSig
from htsql.tr.fn.signature import LengthSig


class OracleEncodeLength(EncodeFunction):

    adapts(LengthSig)

    def __call__(self):
        code = super(OracleEncodeLength, self).__call__()
        zero = LiteralCode(0, code.domain, code.binding)
        return FormulaCode(IfNullSig(), code.domain, code.binding,
                           lop=code, rop=zero)


