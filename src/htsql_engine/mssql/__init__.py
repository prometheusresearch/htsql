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
    hint = """implements HTSQL for Microsoft SQL Server"""
    help = """
    This extension implements HTSQL for Microsoft SQL Server.
    Currently MS SQL Server 2005 and 2008 are supported.

    This extension is loaded automatically when the engine of
    the database URI is set to `mssql`.
    """
    packages = ['.', '.tr']

    def __init__(self, app, attributes):
        if app.htsql.db.engine != 'mssql':
            raise ImportError("mssql engine is expected")
        super(EngineMSSQLAddon, self).__init__(app, attributes)


