#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from . import connect, introspect, split_sql, tr
from htsql.core.addon import Addon


class EngineSQLiteAddon(Addon):

    name = 'engine.sqlite'
    hint = """implements HTSQL for SQLite"""
    help = """
    This extension implements HTSQL for SQLite.

    This extension is loaded automatically when the engine of the
    database URI is set to `sqlite`.
    """
    packages = ['.', '.tr']

    def __init__(self, app, attributes):
        if app.htsql.db.engine != 'sqlite':
            raise ImportError("sqlite engine is expected")
        super(EngineSQLiteAddon, self).__init__(app, attributes)


