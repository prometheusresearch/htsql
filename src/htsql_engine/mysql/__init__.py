#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql_engine.mysql`
=========================

This package provides HTSQL for MySQL.
"""


from . import connect, domain, introspect, split_sql, tr
from htsql.addon import Addon


class EngineMySQLAddon(Addon):

    name = 'engine.mysql'
    packages = ['.', '.tr']


