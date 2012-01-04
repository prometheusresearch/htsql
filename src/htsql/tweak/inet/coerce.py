#
# Copyright (c) 2006-2012, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from ...core.adapter import adapts_many
from ...core.domain import UntypedDomain
from ...core.tr.coerce import BinaryCoerce
from .domain import INetDomain


class BinaryCoerceINet(BinaryCoerce):

    adapts_many((INetDomain, INetDomain),
                (INetDomain, UntypedDomain),
                (UntypedDomain, INetDomain))

    def __call__(self):
        return INetDomain()


