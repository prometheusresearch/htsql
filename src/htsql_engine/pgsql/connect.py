#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql_engine.pgsql.connect`
=================================

This module implements the connection adapter for PostgreSQL.
"""


from htsql.adapter import adapts
from htsql.domain import StringDomain, EnumDomain
from htsql.connect import Connect, DBError, NormalizeError, Normalize
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

    def open(self):
        # Prepare and execute the `psycopg2.connect()` call.
        db = context.app.htsql.db
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


class NormalizePGSQLError(NormalizeError):

    def __call__(self):
        # If we got a DBAPI exception, generate our own error.
        if isinstance(self.error, psycopg2.Error):
            message = str(self.error)
            error = PGSQLError(message)
            return error

        # Otherwise, let the superclass return `None`.
        return super(NormalizePGSQLError, self).__call__()


class NormalizePGSQLString(Normalize):

    adapts(StringDomain)

    @staticmethod
    def convert(value):
        if isinstance(value, str):
            value = value.decode('utf-8')
        return value


class NormalizePGSQLEnum(Normalize):

    adapts(EnumDomain)

    @staticmethod
    def convert(value):
        if isinstance(value, str):
            value = value.decode('utf-8')
        return value


