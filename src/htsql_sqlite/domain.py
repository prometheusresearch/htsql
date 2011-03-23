#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_sqlite.domain`
==========================

This module defines SQLite-specific data types.
"""


from htsql.domain import (Domain, BooleanDomain, IntegerDomain, FloatDomain,
                          StringDomain, DateDomain, TimeDomain, DateTimeDomain,
                          OpaqueDomain)


class SQLiteDomain(Domain):
    """
    Represents an SQLite data type.

    This is an abstract mixin class; see subclasses for concrete data types.

    `name` (a string)
        The name of the type.
    """

    def __init__(self, name, **attributes):
        # Sanity check on the arguments.
        assert isinstance(name, str)

        # Pass the attributes to the concrete domain constructor.
        super(SQLiteDomain, self).__init__(**attributes)
        self.name = name

    def __str__(self):
        return self.name

    def __eq__(self, other):
        # The generic domain comparison checks if the types of the domains
        # and all their attributes are equal.  Since SQLite domains grow
        # an extra attribute `name`, we need to update the comparison
        # implementation.
        return (super(SQLiteDomain, self).__eq__(other) and
                self.name == other.name)


class SQLiteBooleanDomain(SQLiteDomain, BooleanDomain):
    """
    Represents a pseudo-Boolean type for SQLite.

    In SQL, Boolean values are expressed as integers; ``0`` is the FALSE value,
    any non-zero integer is a TRUE value.
    """


class SQLiteIntegerDomain(SQLiteDomain, IntegerDomain):
    """
    Represents an SQLite ``INTEGER`` data type.
    """


class SQLiteFloatDomain(SQLiteDomain, FloatDomain):
    """
    Represents an SQLite ``REAL`` data type.
    """


class SQLiteTextDomain(SQLiteDomain, StringDomain):
    """
    Represents an SQLite ``TEXT`` data type.
    """


class SQLiteDateDomain(SQLiteDomain, DateDomain):
    """
    Represents a pseudo-date type for SQLite.

    In SQL, date values are expressed as ``TEXT`` values
    of the form ``YYYY-MM-DD``.
    """


class SQLiteTimeDomain(SQLiteDomain, TimeDomain):
    pass


class SQLiteDateTimeDomain(SQLiteDomain, DateTimeDomain):
    pass


class SQLiteOpaqueDomain(SQLiteDomain, OpaqueDomain):
    """
    Represents an unsupported SQLite data type.
    """


