#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
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


