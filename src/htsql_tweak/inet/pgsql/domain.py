#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql_engine.pgsql.domain import PGDomain
from ..domain import INetDomain


class PGINetDomain(PGDomain, INetDomain):
    pass


