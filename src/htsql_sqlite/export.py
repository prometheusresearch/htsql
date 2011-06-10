#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql_sqlite.export`
==========================

This module exports the `engine.sqlite` addon.
"""


from htsql.util import autoimport
from htsql.addon import Addon


autoimport('htsql_sqlite')


class ENGINE_SQLITE(Addon):
    """
    Declares the `engine.sqlite` addon.
    """


