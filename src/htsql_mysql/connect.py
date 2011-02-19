#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_mysql.connect`
==========================

This module implements the connection adapter for MySQL.
"""


from htsql.connect import Connect, Normalize, DBError
from htsql.adapter import adapts
from htsql.context import context
from htsql.domain import BooleanDomain, StringDomain, EnumDomain
import MySQLdb, MySQLdb.connections


class Cursor(MySQLdb.connections.Connection.default_cursor):

    _defer_warnings = True


class MySQLError(DBError):
    """
    Raised when a database error occurred.
    """


class ConnectMySQL(Connect):
    """
    Implementation of the connection adapter for SQLite.
    """

    def open_connection(self, with_autocommit=False):
        # Note: `with_autocommit` is ignored.
        db = context.app.db
        parameters = {}
        parameters['db'] = db.database
        if db.host is not None:
            parameters['host'] = db.host
        if db.port is not None:
            parameters['port'] = db.port
        if db.username is not None:
            parameters['user'] = db.username
        if db.password is not None:
            parameters['passwd'] = db.password
        parameters['charset'] = 'utf8'
        parameters['cursorclass'] = Cursor
        connection = MySQLdb.connect(**parameters)
        return connection

    def normalize_error(self, exception):
        # If we got a DBAPI exception, generate our error out of it.
        if isinstance(exception, MySQLdb.Error):
            message = str(exception)
            error = MySQLError(message, exception)
            return error

        # Otherwise, let the superclass return `None`.
        return super(ConnectMySQL, self).normalize_error(exception)


class NormalizeMySQLBoolean(Normalize):

    adapts(BooleanDomain)

    def __call__(self, value):
        if value is None:
            return None
        return (value != 0)


class NormalizeMySQLString(Normalize):

    adapts(StringDomain)

    def __call__(self, value):
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        return value


class NormalizeMySQLEnum(Normalize):

    adapts(EnumDomain)

    def __call__(self, value):
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        if value not in self.domain.labels:
            value = None
        return value



