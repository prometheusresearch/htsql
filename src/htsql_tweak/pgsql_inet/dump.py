#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.adapter import adapts
from htsql.domain import IntegerDomain
from htsql.tr.dump import DumpByDomain, DumpToDomain
from .domain import INetDomain


class DumpInet(DumpByDomain):

    adapts(INetDomain)

    def __call__(self):
        self.format("{value:literal}::INET", value=self.value)


class DumpIntegerToINet(DumpToDomain):

    adapts(IntegerDomain, INetDomain)

    def __call__(self):
        self.format("('0.0.0.0'::INET + {base})", base=self.base)


