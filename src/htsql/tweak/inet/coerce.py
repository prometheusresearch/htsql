#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ...core.adapter import adapt_many
from ...core.domain import UntypedDomain
from ...core.tr.coerce import BinaryCoerce
from .domain import INetDomain


class BinaryCoerceINet(BinaryCoerce):

    adapt_many((INetDomain, INetDomain),
               (INetDomain, UntypedDomain),
               (UntypedDomain, INetDomain))

    def __call__(self):
        return INetDomain()


