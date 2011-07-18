#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.util import maybe
from htsql.domain import Domain
from htsql_engine.pgsql.domain import PGDomain
import socket


class INetDomain(Domain):

    def parse(self, data):
        assert isinstance(data, maybe(str))
        if data is None:
            return None
        try:
            data = socket.inet_ntoa(socket.inet_aton(data))
        except socket.error, exc:
            raise ValueError("invalid inet value")
        return data

    def dump(self, value):
        assert isinstance(data, maybe(str))
        return value


class PGINetDomain(PGDomain, INetDomain):
    pass


