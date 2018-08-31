#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from htsql.core.adapter import adapt
from htsql.core.domain import TextDomain, EnumDomain
from htsql.core.connect import (Connect, UnscrambleError, Unscramble,
        Scramble)
from htsql.core.context import context
import psycopg2, psycopg2.extensions


class ConnectPGSQL(Connect):
    """
    Implementation of the connection adapter for PostgreSQL.
    """

    def open(self):
        # Prepare and execute the `psycopg2.connect()` call.
        addon = context.app.htsql
        parameters = {}
        parameters['database'] = addon.db.database
        if addon.db.host is not None:
            parameters['host'] = addon.db.host
        if addon.db.port is not None:
            parameters['port'] = addon.db.port
        if addon.db.username is not None:
            parameters['user'] = addon.db.username
        if addon.db.password is not None:
            parameters['password'] = addon.db.password
        if addon.password is not None:
            parameters['password'] = addon.password
        connection = psycopg2.connect(**parameters)

        # All queries are UTF-8 encoded strings regardless of the database
        # encoding.
        connection.set_client_encoding('UTF8')

        # Make TEXT values return as `unicode` objects.
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODE,
                                          connection)
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY,
                                          connection)

        # Enable autocommit.
        if self.with_autocommit:
            connection.set_isolation_level(
                    psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        return connection


class UnscramblePGSQLError(UnscrambleError):

    def __call__(self):
        # If we got a DBAPI exception, extract the error message.
        if isinstance(self.error, psycopg2.Error):
            return str(self.error)

        # Otherwise, let the superclass return `None`.
        return super(UnscramblePGSQLError, self).__call__()


