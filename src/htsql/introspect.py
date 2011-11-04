#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql.introspect`
=======================

This module declares the database introspector adapter.
"""


from .context import context
from .adapter import Utility, weigh
from .cache import once
import threading


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

    weigh(10.0)

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
                duplicates = {}
                for unique_key in list(table.unique_keys):
                    key = tuple(unique_key.origin_columns)
                    if key in duplicates:
                        other_key = duplicates[key]
                        if (unique_key.is_primary or
                            (not unique_key.is_partial and
                                other_key.is_partial)):
                            other_key.remove()
                            duplicates[key] = unique_key
                        else:
                            unique_key.remove()
                    else:
                        duplicates[key] = unique_key
                duplicates = {}
                for foreign_key in list(table.foreign_keys):
                    key = (tuple(foreign_key.origin_columns),
                           foreign_key.target,
                           tuple(foreign_key.target_columns))
                    if key in duplicates:
                        other_key = duplicates[key]
                        if (not foreign_key.is_partial and
                                other_key.is_partial):
                            other_key.remove()
                            duplicates[key] = foreign_key
                        else:
                            foreign_key.remove()
                    else:
                        duplicates[key] = foreign_key

        return catalog


@once
def introspect():
    introspect = Introspect()
    catalog = introspect()
    catalog.freeze()
    return catalog


