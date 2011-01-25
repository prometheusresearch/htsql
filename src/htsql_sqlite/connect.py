#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_sqlite.connect`
===========================

This module implements the connection adapter for SQLite.
"""


from htsql.connect import Connect, Normalize, DBError
from htsql.adapter import adapts
from htsql.context import context
from htsql.domain import BooleanDomain, StringDomain, DateDomain
# In Python 2.6, the `sqlite3` module is built-in, but
# for Python 2.5, we need to import a third-party module.
try:
    import sqlite3
except ImportError:
    from pysqlite2 import dbapi2 as sqlite3


class SQLiteError(DBError):
    """
    Raised when a database error occurred.
    """


class ConnectSQLite(Connect):
    """
    Implementation of the connection adapter for SQLite.
    """

    def open_connection(self, with_autocommit=False):
        # FIXME: should we complain if the database address or
        # authentications parameters are not `None`?
        # Get the path to the database file.
        db = context.app.db
        # Generate and return the DBAPI connection.
        if with_autocommit:
            connection = sqlite3.connect(db.database, isolation_level=None)
        else:
            connection = sqlite3.connect(db.database)
        return connection

    def normalize_error(self, exception):
        # If we got a DBAPI exception, generate our error out of it.
        if isinstance(exception, sqlite3.Error):
            message = str(exception)
            error = SQLiteError(message, exception)
            return error

        # Otherwise, let the superclass return `None`.
        return super(ConnectSQLite, self).normalize_error(exception)


class NormalizeSQLiteBoolean(Normalize):

    adapts(BooleanDomain)

    def __call__(self, value):
        if value is None:
            return None
        return (value != 0)


class NormalizeSQLiteString(Normalize):

    adapts(StringDomain)

    def __call__(self, value):
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        return value


class NormalizeSQLiteDate(Normalize):

    adapts(DateDomain)

    def __call__(self, value):
        if isinstance(value, (str, unicode)):
            converter = sqlite3.converters['DATE']
            value = converter(value)
        return value


