#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_pgsql.export`
=========================

This module exports the `engine.pgsql` addon.
"""


from htsql.addon import Addon
import htsql_pgsql.connect
import htsql_pgsql.split_sql
import htsql_pgsql.introspect
import htsql_pgsql.tr.serialize


class ENGINE_PGSQL(Addon):
    """
    Declares the `engine.pgsql` addon.
    """


