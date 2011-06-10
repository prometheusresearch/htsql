#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.adapter import adapts_many
from htsql.domain import UntypedDomain
from htsql.tr.coerce import BinaryCoerce
from .domain import INetDomain


class BinaryCoerceINet(BinaryCoerce):

    adapts_many((INetDomain, INetDomain),
                (INetDomain, UntypedDomain),
                (UntypedDomain, INetDomain))

    def __call__(self):
        return INetDomain()


