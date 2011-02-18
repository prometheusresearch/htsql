#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_mysql.export`
=========================

This module exports the `engine.mysql` addon.
"""


from htsql.util import autoimport
from htsql.addon import Addon


autoimport('htsql_mysql')


class ENGINE_MYSQL(Addon):
    """
    Declares the `engine.mysql` addon.
    """


