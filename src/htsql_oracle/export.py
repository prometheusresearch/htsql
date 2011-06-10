#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql_oracle.export`
==========================

This module exports the `engine.oracle` addon.
"""


from htsql.util import autoimport
from htsql.addon import Addon


autoimport('htsql_oracle')


class ENGINE_ORACLE(Addon):
    """
    Declares the `engine.oracle` addon.
    """


