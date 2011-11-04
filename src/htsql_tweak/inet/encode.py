#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.adapter import adapts, adapts_many
from htsql.domain import IntegerDomain, StringDomain
from htsql.tr.encode import Convert
from htsql.tr.flow import CastCode
from .domain import INetDomain


class ConvertINet(Convert):

    adapts_many((IntegerDomain, INetDomain),
                (StringDomain, INetDomain),
                (INetDomain, IntegerDomain),
                (INetDomain, StringDomain))

    def __call__(self):
        return CastCode(self.state.encode(self.base), self.domain,
                        self.binding)


class ConvertINetToINet(Convert):

    adapts(INetDomain, INetDomain)

    def __call__(self):
        return self.state.encode(self.base)


