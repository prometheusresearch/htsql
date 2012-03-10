#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from htsql.core.connect import Connect, Normalize, NormalizeError, DBError
from htsql.core.adapter import adapt
from htsql.core.context import context
from htsql.core.domain import (BooleanDomain, StringDomain, DateDomain,
                               TimeDomain)
import datetime
import pymssql


class MSSQLError(DBError):
    """
    Raised when a database error occurred.
    """


class ConnectMSSQL(Connect):

    def open(self):
        addon = context.app.htsql
        parameters = {}
        parameters['database'] = addon.db.database
        if addon.db.host is not None:
            if addon.db.port is not None:
                parameters['host'] = "%s:%s" % (addon.db.host, addon.db.port)
            else:
                parameters['host'] = addon.db.host
        if addon.db.username is not None:
            parameters['user'] = addon.db.username
        if addon.db.password is not None:
            parameters['password'] = addon.db.password
        if addon.password is not None:
            parameters['password'] = addon.password
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

    adapt(BooleanDomain)

    @staticmethod
    def convert(value):
        if value is None:
            return None
        return (value != 0)


class NormalizeMSSQLString(Normalize):

    adapt(StringDomain)

    @staticmethod
    def convert(value):
        if isinstance(value, str):
            value = value.decode('utf-8')
        return value


class NormalizeMSSQLDate(Normalize):

    adapt(DateDomain)

    @staticmethod
    def convert(value):
        if isinstance(value, datetime.datetime):
            assert not value.time()
            value = value.date()
        return value


class NormalizeMSSQLTime(Normalize):

    adapt(TimeDomain)

    @staticmethod
    def convert(value):
        if isinstance(value, float):
            assert 0.0 <= value < 1.0
            value = int(86400000000*value) * datetime.timedelta(0,0,1)
            assert not value.days
            value = (datetime.datetime(2001,1,1) + value).time()
        return value


