#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from htsql.core.adapter import adapt
from htsql.core.domain import Domain, IntegerDomain, StringDomain
from htsql.core.tr.dump import DumpByDomain, DumpToDomain
from htsql.tweak.inet.domain import INetDomain


class DumpInet(DumpByDomain):

    adapt(INetDomain)

    def __call__(self):
        self.format("{value:literal}::INET", value=self.value)


class DumpToINet(DumpToDomain):

    adapt(Domain, INetDomain)

    def __call__(self):
        self.format("CAST({base} AS INET)", base=self.base)


class DumpIntegerToINet(DumpToDomain):

    adapt(IntegerDomain, INetDomain)

    def __call__(self):
        self.format("('0.0.0.0'::INET + {base})", base=self.base)


class DumpINetToInteger(DumpToDomain):

    adapt(INetDomain, IntegerDomain)

    def __call__(self):
        self.format("({base} - '0.0.0.0'::INET)", base=self.base)


class DumpINetToString(DumpToDomain):

    adapt(INetDomain, StringDomain)

    def __call__(self):
        self.format("HOST({base})", base=self.base)


