#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.introspect`
=======================

This module declares the database introspector adapter.

This module exports a global variable:

`introspect_adapters`
    List of adapters declared in this module.
"""


from .adapter import Utility, find_adapters


class Introspect(Utility):
    """
    Declares the introspection interface.

    An introspector analyzes the database meta-data and generates
    an HTSQL catalog.

    Usage::

        introspect = Introspect()
        catalog = introspect()

    Note that most implementations loads the meta-data information
    when the adapter is constructed so subsequent calls of
    :meth:`__call__` will always produce essentially the same catalog.
    To re-load the meta-data from the database, create a new instance
    of the :class:`Introspect` adapter.
    """

    def __call__(self):
        """
        Returns an HTSQL catalog.
        """
        # Override in implementations.
        raise NotImplementedError()


introspect_adapters = find_adapters()


