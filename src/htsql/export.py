#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
This module exports the `htsql.core` addon.
"""


from addon import Addon
from wsgi import wsgi_adapters
from connect import connect_adapters
from split_sql import split_sql_adapters


class HTSQL_CORE(Addon):
    """
    Declares the `htsql.core` addon.
    """

    # List of adapters exported by the addon.
    adapters = (wsgi_adapters +
                connect_adapters +
                split_sql_adapters)


