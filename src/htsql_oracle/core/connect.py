#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from htsql.core.connect import Connect, Scramble, Unscramble, UnscrambleError
from htsql.core.adapter import adapt
from htsql.core.context import context
from htsql.core.error import Error
from htsql.core.domain import (BooleanDomain, DecimalDomain, TextDomain,
        DateDomain, TimeDomain)
import datetime
import decimal
import cx_Oracle


class ConnectOracle(Connect):
    """
    Implementation of the connection adapter for Oracle.
    """

    @classmethod
    def outconverter(cls, value):
        value = value.replace(',', '.')
        if '.' in value:
            return decimal.Decimal(value)
        return int(value)

    @classmethod
    def outputtypehandler(cls, cursor, name, defaultType,
                          size, precision, scale):
        if defaultType == cx_Oracle.NUMBER:
            return cursor.var(str, 100, cursor.arraysize,
                              outconverter=cls.outconverter)
        if defaultType in (cx_Oracle.STRING, cx_Oracle.FIXED_CHAR):
            return cursor.var(str, size, cursor.arraysize)

    def open(self):
        addon = context.app.htsql
        parameters = {}
        parameters['user'] = addon.db.username or ''
        parameters['password'] = addon.db.password or ''
        if addon.password is not None:
            parameters['password'] = addon.password
        if addon.db.host is not None:
            host = addon.db.host
            port = addon.db.port or 1521
            sid = addon.db.database
            dsn = cx_Oracle.makedsn(host, port, sid)
        else:
            dsn = addon.db.database
        parameters['dsn'] = dsn
        connection = cx_Oracle.connect(**parameters)
        if self.with_autocommit:
            connection.autocommit = True
        connection.outputtypehandler = self.outputtypehandler
        cursor = connection.cursor()
        cursor.execute("ALTER SESSION SET NLS_SORT = BINARY_CI")
        cursor.execute("ALTER SESSION SET NLS_COMP = LINGUISTIC")
        return connection


class UnscrambleOracleError(UnscrambleError):

    def __call__(self):
        # If we got a DBAPI exception, extract the error message.
        if isinstance(self.error, cx_Oracle.Error):
            return str(self.error)

        # Otherwise, let the superclass return `None`.
        return super(UnscrambleOracleError, self).__call__()


class UnscrambleOracleBoolean(Unscramble):

    adapt(BooleanDomain)

    @staticmethod
    def convert(value):
        if value is None:
            return None
        return (value != 0)


class UnscrambleOracleDecimal(Unscramble):

    adapt(DecimalDomain)

    @staticmethod
    def convert(value):
        if value is None:
            return None
        return decimal.Decimal(value)


class UnscrambleOracleText(Unscramble):

    adapt(TextDomain)

    @staticmethod
    def convert(value):
        if isinstance(value, cx_Oracle.LOB):
            try:
                value = value.read()
            except cx_Oracle.Error as exc:
                message = str(exc)
                raise Error(message, exc)
        return value


class UnscrambleOracleDate(Unscramble):

    adapt(DateDomain)

    @staticmethod
    def convert(value):
        if isinstance(value, datetime.datetime):
            assert not value.time()
            value = value.date()
        return value


class UnscrambleOracleTime(Unscramble):

    adapt(TimeDomain)

    @staticmethod
    def convert(value):
        if isinstance(value, datetime.timedelta):
            assert not value.days
            value = (datetime.datetime(2001,1,1) + value).time()
        return value


