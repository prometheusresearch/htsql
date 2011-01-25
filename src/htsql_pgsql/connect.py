#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_pgsql.connect`
==========================

This module implements the connection adapter for PostgreSQL.
"""


from htsql.connect import Connect, DBError
from htsql.context import context
import psycopg2, psycopg2.extensions


class PGSQLError(DBError):
    """
    Raised when a database error occurred.
    """


class ConnectPGSQL(Connect):
    """
    Implementation of the connection adapter for PostgreSQL.
    """

    def open_connection(self, with_autocommit=False):
        # Prepare and execute the `pgsql2.connect()` call.
        db = context.app.db
        parameters = {}
        parameters['database'] = db.database
        if db.host is not None:
            parameters['host'] = db.host
        if db.port is not None:
            parameters['port'] = db.port
        if db.username is not None:
            parameters['user'] = db.username
        if db.password is not None:
            parameters['password'] = db.password
        connection = psycopg2.connect(**parameters)

        # Enable autocommit.
        if with_autocommit:
            connection.set_isolation_level(
                    psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        return connection

    def normalize_error(self, exception):
        # If we got a DBAPI exception, generate our own error.
        if isinstance(exception, psycopg2.Error):
            message = str(exception)
            error = PGSQLError(message, exception)
            return error

        # Otherwise, let the superclass return `None`.
        return super(ConnectPGSQL, self).normalize_error(exception)


