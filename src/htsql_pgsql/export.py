#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_pgsql.export`
=========================

This module exports the `engine.pgsql` addon.
"""


from htsql.util import autoimport
from htsql.addon import Addon


autoimport('htsql_pgsql')


class ENGINE_PGSQL(Addon):
    """
    Declares the `engine.pgsql` addon.
    """


