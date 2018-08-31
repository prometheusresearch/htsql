#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from htsql.core.connect import Connect, Scramble, Unscramble, UnscrambleError
from htsql.core.adapter import adapt
from htsql.core.error import Error
from htsql.core.context import context
from htsql.core.domain import (BooleanDomain, TextDomain, IntegerDomain,
        DecimalDomain, FloatDomain, DateDomain, TimeDomain, DateTimeDomain)
import sqlite3
import datetime
import os.path
import decimal
import math


def sqlite3_sqrt(x):
    try:
        return math.sqrt(float(x))
    except:
        return None


def sqlite3_power(x, y):
    try:
        return float(x) ** float(y)
    except:
        return None


class ConnectSQLite(Connect):
    """
    Implementation of the connection adapter for SQLite.
    """

    def open(self):
        # FIXME: should we complain if the database address or
        # authentications parameters are not `None`?
        # Get the path to the database file.
        db = context.app.htsql.db
        # Check if the database file exists.
        if not ((db.database.startswith(":") and db.database.endswith(":")) or
                os.path.exists(db.database)):
            raise Error("file does not exist: %s" % db.database)
        # Generate and return the DBAPI connection.
        connection = sqlite3.connect(db.database)
        self.create_functions(connection)
        if self.with_autocommit:
            connection.isolation_level = None
        return connection

    def create_functions(self, connection):
        connection.create_function('POWER', 2, sqlite3_power)
        connection.create_function('SQRT', 1, sqlite3_sqrt)


class UnscrambleSQLiteError(UnscrambleError):

    def __call__(self):
        # If we got a DBAPI exception, extract the error message.
        if isinstance(self.error, sqlite3.Error):
            return str(self.error)
        # Otherwise, let the superclass return `None`.
        return super(UnscrambleSQLiteError, self).__call__()


# FIXME: validate numeric values.


class UnscrambleSQLiteBoolean(Unscramble):

    adapt(BooleanDomain)

    @staticmethod
    def convert(value):
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if not isinstance(value, int):
            raise Error("Expected a Boolean value, got", repr(value))
        return (value != 0)


class UnscrambleSQLiteInteger(Unscramble):

    adapt(IntegerDomain)

    @staticmethod
    def convert(value):
        if value is None:
            return None
        if isinstance(value, int):
            return value
        raise Error("Expected an integer value, got", repr(value))


class UnscrambleSQLiteDecimal(Unscramble):

    adapt(DecimalDomain)

    @staticmethod
    def convert(value):
        if value is None:
            return None
        if isinstance(value, float):
            return decimal.Decimal(str(value))
        raise Error("Expected a decimal value, got", repr(value))


class UnscrambleSQLiteFloat(Unscramble):

    adapt(FloatDomain)

    @staticmethod
    def convert(value):
        if value is None:
            return None
        if isinstance(value, float):
            return value
        raise Error("Expected a float value, got", repr(value))


class UnscrambleSQLiteText(Unscramble):

    adapt(TextDomain)

    @staticmethod
    def convert(value):
        if value is None:
            return None
        if not isinstance(value, str):
            raise Error("Expected a text value, got", repr(value))
        return value


class UnscrambleSQLiteDate(Unscramble):

    adapt(DateDomain)

    @staticmethod
    def convert(value):
        if value is None:
            return None
        if isinstance(value, datetime.date):
            return value
        if not isinstance(value, str):
            raise Error("Expected a date value, got", repr(value))
        converter = sqlite3.converters['DATE']
        value = converter(value.encode('utf-8'))
        return value


class UnscrambleSQLiteTime(Unscramble):

    adapt(TimeDomain)

    @staticmethod
    def convert(value):
        if value is None:
            return None
        if isinstance(value, datetime.time):
            return value
        if not isinstance(value, str):
            raise Error("Expected a time value, got", repr(value))
        # FIXME: verify that the value is in valid format.
        hour, minute, second = value.split(':')
        hour = int(hour)
        minute = int(minute)
        if '.' in second:
            second, microsecond = second.split('.')
            second = int(second)
            microsecond = int(microsecond)
        else:
            second = int(second)
            microsecond = 0
        value = datetime.time(hour, minute, second, microsecond)
        return value


class UnscrambleSQLiteDateTime(Unscramble):

    adapt(DateTimeDomain)

    @staticmethod
    def convert(value):
        if value is None:
            return None
        if isinstance(value, datetime.datetime):
            return value
        if not isinstance(value, str):
            raise Error("Expected a timestamp value, got", repr(value))
        converter = sqlite3.converters['TIMESTAMP']
        value = converter(value.encode('utf-8'))
        return value


