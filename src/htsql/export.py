#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.export`
===================

This module exports the `htsql.core` addon.
"""


from .util import autoimport
from .addon import Addon


autoimport('htsql')


class HTSQL_CORE(Addon):
    """
    Declares the `htsql.core` addon.
    """


