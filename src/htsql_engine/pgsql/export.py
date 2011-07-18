#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql_engine.pgsql.export`
================================

This module exports the `engine.pgsql` addon.
"""


from htsql.util import autoimport
from htsql.addon import Addon


autoimport('htsql_engine.pgsql')


class ENGINE_PGSQL(Addon):
    """
    Declares the `engine.pgsql` addon.
    """


