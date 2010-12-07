#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_sqlite.export`
==========================

This module exports the `engine.sqlite` addon.
"""


from htsql.addon import Addon
import htsql_sqlite.connect
import htsql_sqlite.split_sql
import htsql_sqlite.introspect
import htsql_sqlite.tr.bind
import htsql_sqlite.tr.dump


class ENGINE_SQLITE(Addon):
    """
    Declares the `engine.sqlite` addon.
    """


