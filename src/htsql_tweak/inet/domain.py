#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.util import maybe
from htsql.domain import Domain
import socket


class INetDomain(Domain):

    def parse(self, data):
        assert isinstance(data, maybe(unicode))
        if data is None:
            return None
        data = data.encode('utf-8')
        try:
            data = socket.inet_ntoa(socket.inet_aton(data))
        except socket.error:
            raise ValueError("invalid IPv4 address")
        return data.decode('utf-8')

    def dump(self, value):
        assert isinstance(value, maybe(unicode))
        return value


