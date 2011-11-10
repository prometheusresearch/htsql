#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql_engine.oracle.connect`
==================================

This module implements the connection adapter for Oracle.
"""


from htsql.connect import Connect, Normalize, NormalizeError, DBError
from htsql.adapter import adapts
from htsql.context import context
from htsql.domain import (BooleanDomain, DecimalDomain, StringDomain,
                          DateDomain, TimeDomain)
import datetime
import decimal
import cx_Oracle


class OracleError(DBError):
    """
    Raised when a database error occurred.
    """


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
            return cursor.var(unicode, size, cursor.arraysize)

    def open(self):
        db = context.app.htsql.db
        parameters = {}
        parameters['user'] = db.username or ''
        parameters['password'] = db.password or ''
        if db.host is not None:
            host = db.host
            port = db.port or 1521
            sid = db.database
            dsn = cx_Oracle.makedsn(host, port, sid)
        else:
            dsn = db.database
        parameters['dsn'] = dsn
        connection = cx_Oracle.connect(**parameters)
        if self.with_autocommit:
            connection.autocommit = True
        connection.outputtypehandler = self.outputtypehandler
        cursor = connection.cursor()
        cursor.execute("ALTER SESSION SET NLS_SORT = BINARY_CI")
        cursor.execute("ALTER SESSION SET NLS_COMP = LINGUISTIC")
        return connection


class NormalizeOracleError(NormalizeError):

    def __call__(self):
        # If we got a DBAPI exception, generate our error out of it.
        if isinstance(self.error, cx_Oracle.Error):
            message = str(self.error)
            error = OracleError(message)
            return error

        # Otherwise, let the superclass return `None`.
        return super(NormalizeOracleError, self).__call__()


class NormalizeOracleBoolean(Normalize):

    adapts(BooleanDomain)

    @staticmethod
    def convert(value):
        if value is None:
            return None
        return (value != 0)


class NormalizeOracleDecimal(Normalize):

    adapts(DecimalDomain)

    @staticmethod
    def convert(value):
        if value is None:
            return None
        return decimal.Decimal(value)


class NormalizeOracleString(Normalize):

    adapts(StringDomain)

    @staticmethod
    def convert(value):
        if isinstance(value, cx_Oracle.LOB):
            try:
                value = value.read()
            except cx_Oracle.Error, exc:
                message = str(exc)
                raise OracleError(message, exc)
        if isinstance(value, str):
            value = value.decode('utf-8')
        return value


class NormalizeOracleDate(Normalize):

    adapts(DateDomain)

    @staticmethod
    def convert(value):
        if isinstance(value, datetime.datetime):
            assert not value.time()
            value = value.date()
        return value


class NormalizeOracleTime(Normalize):

    adapts(TimeDomain)

    @staticmethod
    def convert(value):
        if isinstance(value, datetime.timedelta):
            assert not value.days
            value = (datetime.datetime(2001,1,1) + value).time()
        return value


