#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.adapter import named
from htsql_engine.pgsql.introspect import IntrospectPGSQLDomain
from .domain import PGINetDomain


class IntrospectPGSQLINetDomain(IntrospectPGSQLDomain):

    named(('pg_catalog', 'inet'))

    def __call__(self):
        return PGINetDomain(self.schema_name, self.name)


