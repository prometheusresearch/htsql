#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql_engine.mysql.export`
================================

This module exports the `engine.mysql` addon.
"""


from htsql.util import autoimport
from htsql.addon import Addon


autoimport('htsql_engine.mysql')


class ENGINE_MYSQL(Addon):
    """
    Declares the `engine.mysql` addon.
    """


