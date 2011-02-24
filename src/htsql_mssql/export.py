#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_mssql.export`
=========================

This module exports the `engine.mssql` addon.
"""


from htsql.util import autoimport
from htsql.addon import Addon


autoimport('htsql_mssql')


class ENGINE_MSSQL(Addon):
    """
    Declares the `engine.mssql` addon.
    """


