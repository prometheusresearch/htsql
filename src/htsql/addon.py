#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
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
    of :class:`Addon`.
    """

    # TODO: add support for addon parameters and global variables.


