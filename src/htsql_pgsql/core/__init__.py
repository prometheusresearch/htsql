#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from . import connect, introspect, split_sql, tr
from htsql.core.addon import Addon


class EnginePGSQLAddon(Addon):

    name = 'engine.pgsql'
    hint = """implements HTSQL for PostgreSQL"""
    help = """
    This extension implements HTSQL for PostgreSQL database server.
    PostgreSQL version 8.4 or later is required.

    This extension is loaded automatically when the engine of the
    database URI is set to `pgsql`.
    """
    packages = ['.', '.tr']

    def __init__(self, app, attributes):
        if app.htsql.db.engine != 'pgsql':
            raise ImportError("pgsql engine is expected")
        super(EnginePGSQLAddon, self).__init__(app, attributes)


