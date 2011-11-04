#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.adapter import adapts
from htsql.domain import Domain, IntegerDomain, StringDomain
from htsql.tr.dump import DumpByDomain, DumpToDomain
from ..domain import INetDomain


class DumpInet(DumpByDomain):

    adapts(INetDomain)

    def __call__(self):
        self.format("{value:literal}::INET", value=self.value)


class DumpToINet(DumpToDomain):

    adapts(Domain, INetDomain)

    def __call__(self):
        self.format("CAST({base} AS INET)", base=self.base)


class DumpIntegerToINet(DumpToDomain):

    adapts(IntegerDomain, INetDomain)

    def __call__(self):
        self.format("('0.0.0.0'::INET + {base})", base=self.base)


class DumpINetToInteger(DumpToDomain):

    adapts(INetDomain, IntegerDomain)

    def __call__(self):
        self.format("({base} - '0.0.0.0'::INET)", base=self.base)


class DumpINetToString(DumpToDomain):

    adapts(INetDomain, StringDomain)

    def __call__(self):
        self.format("HOST({base})", base=self.base)


