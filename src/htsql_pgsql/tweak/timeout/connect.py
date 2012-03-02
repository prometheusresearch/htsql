#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from htsql.core.context import context
from htsql_pgsql.core.connect import ConnectPGSQL


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


