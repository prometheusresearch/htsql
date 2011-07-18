#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql_engine.mssql`
=========================

This package provides HTSQL for MS SQL Server.
"""



from . import connect, domain, introspect, split_sql, tr
from htsql.addon import Addon


class EngineMSSQLAddon(Addon):

    name = 'engine.mssql'
    packages = ['.', '.tr']


