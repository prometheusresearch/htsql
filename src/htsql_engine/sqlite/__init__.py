#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql_engine.sqlite`
==========================

This package provides HTSQL for SQLite.
"""


from . import connect, domain, introspect, split_sql, tr
from htsql.addon import Addon


class EngineSQLiteAddon(Addon):

    name = 'engine.sqlite'
    packages = ['.', '.tr']


