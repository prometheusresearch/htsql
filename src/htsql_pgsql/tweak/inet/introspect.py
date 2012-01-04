#
# Copyright (c) 2006-2012, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.core.adapter import named
from ...core.introspect import IntrospectPGSQLDomain
from .domain import PGINetDomain


class IntrospectPGSQLINetDomain(IntrospectPGSQLDomain):

    named(('pg_catalog', 'inet'))

    def __call__(self):
        return PGINetDomain(self.schema_name, self.name)


