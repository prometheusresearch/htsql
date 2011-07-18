#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql_engine.mssql.export`
================================

This module exports the `engine.mssql` addon.
"""


from htsql.util import autoimport
from htsql.addon import Addon


autoimport('htsql_engine.mssql')


class ENGINE_MSSQL(Addon):
    """
    Declares the `engine.mssql` addon.
    """


