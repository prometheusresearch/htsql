#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql.export`
===================

This module exports the `htsql.core` addon.
"""


from .util import autoimport
from .addon import Addon


autoimport('htsql')


class HTSQL_CORE(Addon):
    """
    Declares the `htsql.core` addon.
    """


