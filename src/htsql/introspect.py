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
from .adapter import Utility
import threading


class CatalogCache(object):

    def __init__(self):
        self.lock = threading.Lock()
        self.catalog = None

    def update(self, catalog):
        self.catalog = catalog


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


def introspect():
    catalog_cache = context.app.htsql.catalog_cache
    if catalog_cache.catalog is None:
        with catalog_cache.lock:
            if catalog_cache.catalog is None:
                introspect = Introspect()
                catalog = introspect()
                catalog.freeze()
                catalog_cache.update(catalog)
    return catalog_cache.catalog


