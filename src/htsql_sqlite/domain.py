#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_sqlite.domain`
=========================

This module defines SQLite-specific data types.
"""


from htsql.domain import (Domain, BooleanDomain, IntegerDomain, FloatDomain,
                          StringDomain, DateDomain, OpaqueDomain)


class SQLiteDomain(Domain):

    def __init__(self, name, **attributes):
        super(SQLiteDomain, self).__init__(**attributes)
        self.name = name

    def __str__(self):
        return self.name


class SQLiteBooleanDomain(SQLiteDomain, BooleanDomain):
    pass


class SQLiteIntegerDomain(SQLiteDomain, IntegerDomain):
    pass


class SQLiteFloatDomain(SQLiteDomain, FloatDomain):
    pass


class SQLiteTextDomain(SQLiteDomain, StringDomain):
    pass


class SQLiteDateDomain(SQLiteDomain, DateDomain):
    pass


class SQLiteOpaqueDomain(SQLiteDomain, OpaqueDomain):
    pass


