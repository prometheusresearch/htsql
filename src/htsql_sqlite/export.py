#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
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


