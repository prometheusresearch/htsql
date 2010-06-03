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


class HTSQL_CORE(Addon):
    """
    Declares the `htsql.core` addon.
    """

    adapters = wsgi_adapters


