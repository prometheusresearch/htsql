#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
This module exports the `engine.pgsql` addon.
"""


from htsql.addon import Addon


class ENGINE_PGSQL(Addon):
    """
    Declares the `engine.pgsql` addon.
    """

    adapters = []


