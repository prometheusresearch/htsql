#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql_engine.oracle.export`
=================================

This module exports the `engine.oracle` addon.
"""


from htsql.util import autoimport
from htsql.addon import Addon


autoimport('htsql_engine.oracle')


class ENGINE_ORACLE(Addon):
    """
    Declares the `engine.oracle` addon.
    """


