#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_pgsql.export`
=========================

This module exports the `engine.pgsql` addon.
"""


from htsql.addon import Addon
from .connect import connect_adapters
from .split_sql import split_sql_adapters


class ENGINE_PGSQL(Addon):
    """
    Declares the `engine.pgsql` addon.
    """

    # List of adapters exported by the addon.
    adapters = (connect_adapters +
                split_sql_adapters)


