#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from htsql.core.adapter import adapt
from htsql.core.domain import StringDomain
from htsql.core.tr.flow import LiteralCode
from htsql.core.tr.rewrite import Rewrite


class OracleRewriteLiteral(Rewrite):

    adapt(LiteralCode)

    def __call__(self):
        if isinstance(self.code.domain, StringDomain) and self.code.value == "":
            return self.code.clone(value=None)
        return self.code


