#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql_engine.mssql.connect`
=================================

This module implements the connection adapter for MS SQL Server.
"""


from htsql.connect import Connect, Normalize, NormalizeError, DBError
from htsql.adapter import adapts
from htsql.context import context
from htsql.domain import BooleanDomain, StringDomain, DateDomain, TimeDomain
import datetime
import pymssql


class MSSQLError(DBError):
    """
    Raised when a database error occurred.
    """


class ConnectMSSQL(Connect):

    def open(self):
        db = context.app.htsql.db
        parameters = {}
        parameters['database'] = db.database
        if db.host is not None:
            if db.port is not None:
                parameters['host'] = "%s:%s" % (db.host, db.port)
            else:
                parameters['host'] = db.host
        if db.username is not None:
            parameters['user'] = db.username
        if db.password is not None:
            parameters['password'] = db.password
        parameters['charset'] = 'utf8'
        connection = pymssql.connect(**parameters)
        if self.with_autocommit:
            connection.autocommit(True)
        return connection


class NormalizeMSSQLError(NormalizeError):

    def __call__(self):
        # If we got a DBAPI exception, generate our error out of it.
        if isinstance(self.error, pymssql.Error):
            message = str(self.error)
            error = MSSQLError(message)
            return error

        # Otherwise, let the superclass return `None`.
        return super(NormalizeMSSQLError, self).__call__()


class NormalizeMSSQLBoolean(Normalize):

    adapts(BooleanDomain)

    @staticmethod
    def convert(value):
        if value is None:
            return None
        return (value != 0)


class NormalizeMSSQLString(Normalize):

    adapts(StringDomain)

    @staticmethod
    def convert(value):
        if isinstance(value, str):
            value = value.decode('utf-8')
        return value


class NormalizeMSSQLDate(Normalize):

    adapts(DateDomain)

    @staticmethod
    def convert(value):
        if isinstance(value, datetime.datetime):
            assert not value.time()
            value = value.date()
        return value


class NormalizeMSSQLTime(Normalize):

    adapts(TimeDomain)

    @staticmethod
    def convert(value):
        if isinstance(value, float):
            assert 0.0 <= value < 1.0
            value = int(86400000000*value) * datetime.timedelta(0,0,1)
            assert not value.days
            value = (datetime.datetime(2001,1,1) + value).time()
        return value


