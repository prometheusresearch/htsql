#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from htsql_sqlite.core.introspect import IntrospectSQLite
from .connect import build_names


class IntrospectFileDBCleanup(IntrospectSQLite):

    def __call__(self):
        catalog = super(IntrospectFileDBCleanup, self).__call__()
        table_names = set(name for name, file in build_names())
        for schema in catalog.schemas:
            for table in list(schema.tables):
                if table.name not in table_names:
                    table.remove()
        return catalog


