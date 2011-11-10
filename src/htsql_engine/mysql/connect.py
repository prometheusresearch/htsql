#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql_engine.mysql.connect`
=================================

This module implements the connection adapter for MySQL.
"""


from htsql.connect import Connect, Normalize, NormalizeError, DBError
from htsql.adapter import adapts
from htsql.context import context
from htsql.domain import BooleanDomain, StringDomain, EnumDomain, TimeDomain
import MySQLdb, MySQLdb.connections
import datetime


class Cursor(MySQLdb.connections.Connection.default_cursor):

    _defer_warnings = True


class MySQLError(DBError):
    """
    Raised when a database error occurred.
    """


class ConnectMySQL(Connect):

    def open(self):
        # Note: `with_autocommit` is ignored.
        db = context.app.htsql.db
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
        parameters['use_unicode'] = True
        parameters['charset'] = 'utf8'
        parameters['cursorclass'] = Cursor
        connection = MySQLdb.connect(**parameters)
        return connection


class NormalizeMySQLError(NormalizeError):

    def __call__(self):
        # If we got a DBAPI exception, generate our error out of it.
        if isinstance(self.error, MySQLdb.Error):
            message = str(self.error)
            error = MySQLError(message)
            return error

        # Otherwise, let the superclass return `None`.
        return super(NormalizeMySQLError, self).__call__()


class NormalizeMySQLBoolean(Normalize):

    adapts(BooleanDomain)

    @staticmethod
    def convert(value):
        if value is None:
            return None
        return (value != 0)


class NormalizeMySQLString(Normalize):

    adapts(StringDomain)

    @staticmethod
    def convert(value):
        if isinstance(value, str):
            value = value.decode('utf-8')
        return value


class NormalizeMySQLEnum(Normalize):

    adapts(EnumDomain)

    def convert(self, value):
        if isinstance(value, str):
            value = value.decode('utf-8')
        if value not in self.domain.labels:
            value = None
        return value


class NormalizeMySQLTime(Normalize):

    adapts(TimeDomain)

    @staticmethod
    def convert(value):
        if isinstance(value, datetime.timedelta):
            if value.days != 0:
                value = None
            else:
                value = (datetime.datetime(2001,1,1) + value).time()
        return value


