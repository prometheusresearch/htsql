#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_sqlite.connect`
===========================

This module implements the connection adapter for SQLite.

This module exports a global variable:

`connect_adapters`
    List of adapters declared in this module.
"""


from htsql.connect import Connect, Normalize, DBError
from htsql.adapter import adapts, find_adapters
from htsql.context import context
from htsql.domain import StringDomain
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
        return super(SQLiteConnect, self).normalize_error(exception)


class NormalizeSQLiteString(Normalize):

    adapts(StringDomain)

    def __call__(self, value):
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        return value


connect_adapters = find_adapters()


