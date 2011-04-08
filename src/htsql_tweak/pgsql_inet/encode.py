#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


from htsql.adapter import adapts
from htsql.domain import IntegerDomain
from htsql.tr.encode import Convert
from htsql.tr.code import CastCode
from .domain import INetDomain


class ConvertToINet(Convert):

    adapts(IntegerDomain, INetDomain)

    def __call__(self):
        return CastCode(self.state.encode(self.base), self.domain,
                        self.binding)


