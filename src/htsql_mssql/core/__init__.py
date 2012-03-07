#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from . import connect, introspect, split_sql, tr
from htsql.core.addon import Addon


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


