#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql_engine.oracle`
==========================

This package provides HTSQL for Oracle.
"""


from . import connect, domain, introspect, split_sql, tr
from htsql.addon import Addon


class EngineOracleAddon(Addon):

    name = 'engine.oracle'
    packages = ['.', '.tr']

    def __init__(self, app, attributes):
        if app.htsql.db.engine != 'oracle':
            raise ImportError("oracle engine is expected")
        super(EngineOracleAddon, self).__init__(app, attributes)


