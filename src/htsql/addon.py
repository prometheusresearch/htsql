#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.addon`
==================

This module declares HTSQL addons.
"""


class Addon(object):
    """
    Implements an addon for HTSQL applications.

    This is an abstract class; to add a new addon, create a subclass
    of :class:`Addon` and override the following class attributes:

    `adapters` (a list of :class:`htsql.adapter.Adapter` instances)
        Adapters exported by the addon.
    """

    # TODO: add support for addon parameters and global variables.

    adapters = []


