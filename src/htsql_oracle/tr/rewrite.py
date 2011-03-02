#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


from htsql.adapter import adapts
from htsql.domain import StringDomain
from htsql.tr.code import LiteralCode
from htsql.tr.rewrite import Rewrite


class OracleRewriteLiteral(Rewrite):

    adapts(LiteralCode)

    def __call__(self):
        if isinstance(self.code.domain, StringDomain) and self.code.value == "":
            return self.code.clone(value=None)
        return self.code


