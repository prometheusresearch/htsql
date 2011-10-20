#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.context import context
from htsql.adapter import weigh
from htsql.introspect import Introspect


class OverrideIntrospect(Introspect):

    weigh(1.0)

    def __call__(self):
        addon = context.app.tweak.override
        catalog = super(OverrideIntrospect, self).__call__()

        if addon.include_schema:
            for schema in reversed(list(catalog.schemas)):
                if not any(pattern.matches(schema)
                           for pattern in addon.include_schema):
                    schema.remove()

        if addon.exclude_schema:
            for schema in reversed(list(catalog.schemas)):
                if any(pattern.matches(schema)
                       for pattern in addon.exclude_schema):
                    schema.remove()

        if addon.include_table:
            for schema in catalog.schemas:
                for table in reversed(list(schema.tables)):
                    if not any(pattern.matches(table)
                               for pattern in addon.include_table):
                        table.remove()

        if addon.exclude_table:
            for schema in catalog.schemas:
                for table in reversed(list(schema.tables)):
                    if any(pattern.matches(table)
                           for pattern in addon.exclude_table):
                        table.remove()

        if addon.include_column:
            for schema in catalog.schemas:
                for table in schema.tables:
                    for column in reversed(list(table.columns)):
                        if not any(pattern.matches(column)
                                   for pattern in addon.include_column):
                            column.remove()

        if addon.exclude_column:
            for schema in catalog.schemas:
                for table in schema.tables:
                    for column in reversed(list(table.columns)):
                        if any(pattern.matches(column)
                               for pattern in addon.exclude_column):
                            column.remove()

        return catalog


