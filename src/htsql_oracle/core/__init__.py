#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from . import connect, introspect, split_sql, tr
from htsql.core.addon import Addon


class EngineOracleAddon(Addon):

    name = 'engine.oracle'
    hint = """implements HTSQL for Oracle"""
    help = """
    This extension implements HTSQL for Oracle database server.

    This extension is loaded automatically when the engine of
    the database URI is set to `oracle`.
    """
    packages = ['.', '.tr']

    def __init__(self, app, attributes):
        if app.htsql.db.engine != 'oracle':
            raise ImportError("oracle engine is expected")
        super(EngineOracleAddon, self).__init__(app, attributes)


