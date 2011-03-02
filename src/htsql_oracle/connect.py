#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_oracle.connect`
===========================

This module implements the connection adapter for Oracle.
"""


from htsql.connect import Connect, Normalize, DBError
from htsql.adapter import adapts
from htsql.context import context
from htsql.domain import BooleanDomain, DecimalDomain, StringDomain, DateDomain
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

    def open_connection(self, with_autocommit=False):
        db = context.app.db
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
        if with_autocommit:
            connection.autocommit = True
        connection.outputtypehandler = self.outputtypehandler
        cursor = connection.cursor()
        cursor.execute("ALTER SESSION SET NLS_SORT = BINARY_CI");
        cursor.execute("ALTER SESSION SET NLS_COMP = LINGUISTIC");
        return connection

    def normalize_error(self, exception):
        # If we got a DBAPI exception, generate our error out of it.
        if isinstance(exception, cx_Oracle.Error):
            message = str(exception)
            error = OracleError(message, exception)
            return error

        # Otherwise, let the superclass return `None`.
        return super(ConnectOracle, self).normalize_error(exception)


class NormalizeOracleBoolean(Normalize):

    adapts(BooleanDomain)

    def __call__(self, value):
        if value is None:
            return None
        return (value != 0)


class NormalizeOracleDecimal(Normalize):

    adapts(DecimalDomain)

    def __call__(self, value):
        if value is None:
            return None
        return decimal.Decimal(value)


class NormalizeOracleString(Normalize):

    adapts(StringDomain)

    def __call__(self, value):
        if isinstance(value, cx_Oracle.LOB):
            try:
                value = value.read()
            except cx_Oracle.Error, exc:
                message = str(exc)
                raise OracleError(message, exc)
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        return value


class NormalizeOracleDate(Normalize):

    adapts(DateDomain)

    def __call__(self, value):
        if isinstance(value, datetime.datetime):
            value = value.date()
        return value


