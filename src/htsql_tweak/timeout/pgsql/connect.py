#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.context import context
from htsql_engine.pgsql.connect import ConnectPGSQL


class TimeoutConnectPGSQL(ConnectPGSQL):

    def open(self):
        connection = super(TimeoutConnectPGSQL, self).open()
        timeout = context.app.tweak.timeout.timeout
        if timeout is not None:
            cursor = connection.cursor()
            cursor.execute("""
                SET SESSION STATEMENT_TIMEOUT TO %s
            """ % (timeout*1000))
        return connection


