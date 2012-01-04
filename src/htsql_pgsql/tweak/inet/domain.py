#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from ...core.domain import PGDomain
from htsql.tweak.inet.domain import INetDomain


class PGINetDomain(PGDomain, INetDomain):
    pass


