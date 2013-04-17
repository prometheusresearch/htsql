#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


"""
:mod:`htsql.core.introspect`
============================

This module declares the database introspector adapter.
"""


from .adapter import Utility, rank
from .cache import once


class Introspect(Utility):
    """
    Declares the introspection interface.

    An introspector analyzes the database meta-data and generates
    an HTSQL catalog.
    """

    def __call__(self):
        """
        Returns an HTSQL catalog.
        """
        # Override in implementations.
        raise NotImplementedError()


class IntrospectCleanup(Introspect):

    rank(10.0)

    def __call__(self):
        catalog = super(IntrospectCleanup, self).__call__()

        for schema in reversed(list(catalog.schemas)):
            for table in reversed(list(schema.tables)):
                if not table.columns:
                    table.remove()
            if not schema.tables:
                schema.remove()

        for schema in catalog.schemas:
            for table in schema.tables:
                seen = {}
                for unique_key in list(table.unique_keys):
                    key = tuple(unique_key.origin_columns)
                    if key in seen:
                        other_key = seen[key]
                        if (unique_key.is_primary or
                            (not unique_key.is_partial and
                                other_key.is_partial)):
                            other_key.remove()
                            seen[key] = unique_key
                        else:
                            unique_key.remove()
                    else:
                        seen[key] = unique_key
                seen = {}
                for foreign_key in list(table.foreign_keys):
                    key = (tuple(foreign_key.origin_columns),
                           foreign_key.target,
                           tuple(foreign_key.target_columns))
                    if key in seen:
                        other_key = seen[key]
                        if (not foreign_key.is_partial and
                                other_key.is_partial):
                            other_key.remove()
                            seen[key] = foreign_key
                        else:
                            foreign_key.remove()
                    else:
                        seen[key] = foreign_key

        return catalog


@once
def introspect():
    catalog = Introspect.__invoke__()
    catalog.freeze()
    return catalog


