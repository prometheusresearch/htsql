#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_mssql.connect`
==========================

This module implements the connection adapter for MS SQL Server.
"""


from htsql.connect import Connect, Normalize, DBError
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
    """
    Implementation of the connection adapter for MS SQL Server.
    """

    def open_connection(self, with_autocommit=False):
        db = context.app.db
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
        if with_autocommit:
            connection.autocommit(True)
        return connection

    def normalize_error(self, exception):
        # If we got a DBAPI exception, generate our error out of it.
        if isinstance(exception, pymssql.Error):
            message = str(exception)
            error = MSSQLError(message, exception)
            return error

        # Otherwise, let the superclass return `None`.
        return super(ConnectMSSQL, self).normalize_error(exception)


class NormalizeMSSQLBoolean(Normalize):

    adapts(BooleanDomain)

    def __call__(self, value):
        if value is None:
            return None
        return (value != 0)


class NormalizeMSSQLString(Normalize):

    adapts(StringDomain)

    def __call__(self, value):
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        return value


class NormalizeMSSQLDate(Normalize):

    adapts(DateDomain)

    def __call__(self, value):
        if isinstance(value, datetime.datetime):
            assert not value.time()
            value = value.date()
        return value


class NormalizeMSSQLTime(Normalize):

    adapts(TimeDomain)

    def __call__(self, value):
        if isinstance(value, float):
            assert 0.0 <= value < 1.0
            value = int(86400000000*value) * datetime.timedelta(0,0,1)
            assert not value.days
            value = (datetime.datetime(2001,1,1) + value).time()
        return value


