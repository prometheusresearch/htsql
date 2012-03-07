#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from . import connect, introspect, split_sql, tr
from htsql.core.addon import Addon


class EngineMySQLAddon(Addon):

    name = 'engine.mysql'
    hint = """implements HTSQL for MySQL"""
    help = """
    This extension implements HTSQL for MySQL.  MySQL version 5.1 or
    later is required.

    This extension does not work well with MyISAM engine, InnoDB engine
    is preferred.

    This extension is loaded automatically when the engine of
    the database URI is set to `mysql`.
    """
    packages = ['.', '.tr']

    def __init__(self, app, attributes):
        if app.htsql.db.engine != 'mysql':
            raise ImportError("mysql engine is expected")
        super(EngineMySQLAddon, self).__init__(app, attributes)


