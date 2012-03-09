#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from htsql.core.adapter import call
from ...core.introspect import IntrospectPGSQLDomain
from htsql.tweak.inet.domain import INetDomain


class IntrospectPGSQLINetDomain(IntrospectPGSQLDomain):

    call(('pg_catalog', 'inet'))

    def __call__(self):
        return INetDomain()


