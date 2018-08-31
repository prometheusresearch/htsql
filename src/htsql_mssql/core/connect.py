#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from htsql.core.connect import Connect, Scramble, Unscramble, UnscrambleError
from htsql.core.adapter import adapt
from htsql.core.context import context
from htsql.core.domain import (BooleanDomain, TextDomain, DateDomain,
        TimeDomain)
import datetime
import pymssql


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


class UnscrambleMSSQLError(UnscrambleError):

    def __call__(self):
        # If we got a DBAPI exception, extract the error message.
        if isinstance(self.error, pymssql.Error):
            return str(self.error)

        # Otherwise, let the superclass return `None`.
        return super(UnscrambleMSSQLError, self).__call__()


class UnscrambleMSSQLBoolean(Unscramble):

    adapt(BooleanDomain)

    @staticmethod
    def convert(value):
        if value is None:
            return None
        return (value != 0)


class UnscrambleMSSQLDate(Unscramble):

    adapt(DateDomain)

    @staticmethod
    def convert(value):
        if isinstance(value, datetime.datetime):
            assert not value.time()
            value = value.date()
        return value


class UnscrambleMSSQLTime(Unscramble):

    adapt(TimeDomain)

    @staticmethod
    def convert(value):
        if isinstance(value, float):
            assert 0.0 <= value < 1.0
            value = int(86400000000*value) * datetime.timedelta(0,0,1)
            assert not value.days
            value = (datetime.datetime(2001,1,1) + value).time()
        return value


