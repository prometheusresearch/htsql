#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql_engine.pgsql`
=========================

This package provides HTSQL for PostgreSQL.
"""


from . import connect, domain, introspect, split_sql, tr
from htsql.addon import Addon


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


