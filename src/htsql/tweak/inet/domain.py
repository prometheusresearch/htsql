#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ...core.util import maybe, oneof
from ...core.domain import Domain
import socket


class INetDomain(Domain):

    family = 'inet'

    def parse(self, data):
        assert isinstance(data, maybe(str))
        if data is None:
            return None
        try:
            data = socket.inet_ntoa(socket.inet_aton(data))
        except socket.error:
            raise ValueError("invalid IPv4 address")
        return data

    def dump(self, value):
        assert isinstance(value, maybe(str))
        return str(value)


