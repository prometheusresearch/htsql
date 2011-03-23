#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_mssql.domain`
=========================

This module defines MS SQL Server-specific data types.
"""


from htsql.domain import (Domain, BooleanDomain, IntegerDomain, DecimalDomain,
                          FloatDomain, StringDomain, DateDomain, TimeDomain,
                          DateTimeDomain, OpaqueDomain)


class MSSQLDomain(Domain):
    """
    Represents a native data type of MS SQL Server.

    This is an abstract mixin class; see subclasses for concrete data types.

    `schema_name` (a string)
        The name of the type schema.

    `name` (a string)
        The name of the type.
    """

    def __init__(self, schema_name, name, **attributes):
        # Sanity check on the arguments.
        assert isinstance(schema_name, str)
        assert isinstance(name, str)

        # Pass the attributes to the concrete domain constructor.
        super(MSSQLDomain, self).__init__(**attributes)
        self.schema_name = schema_name
        self.name = name

    def __str__(self):
        return "%s.%s" % (self.schema_name, self.name)

    def __eq__(self, other):
        # The generic domain comparison checks if the types of the domains
        # and all their attributes are equal.  Since we added extra attributes,
        # we need to update the implementation.
        return (super(MSSQLDomain, self).__eq__(other) and
                self.schema_name == other.schema_name and
                self.name == other.name)


class MSSQLBooleanDomain(MSSQLDomain, BooleanDomain):
    """
    Represents a ``BIT`` type of MS SQL Server.

    Boolean values are expressed as integers; ``0`` is the FALSE
    value, ``1`` is the TRUE value.
    """


class MSSQLIntegerDomain(MSSQLDomain, IntegerDomain):
    """
    Represents ``SMALLINT``, ``INT``, and ``BIGINT`` data types.
    """


class MSSQLDecimalDomain(MSSQLDomain, DecimalDomain):
    """
    Represents ``DECIMAL`` and ``NUMERIC`` data types.
    """


class MSSQLFloatDomain(MSSQLDomain, FloatDomain):
    """
    Represents ``REAL`` and ``FLOAT`` data types.
    """


class MSSQLStringDomain(MSSQLDomain, StringDomain):
    """
    Represents ``CHAR``, ``VARCHAR``, ``NCHAR``, ``NVARCHAR`` data types.
    """


class MSSQLDateDomain(MSSQLDomain, DateDomain):
    """
    Represents ``DATETIME`` and ``SMALLDATETIME`` data types.
    """


class MSSQLTimeDomain(MSSQLDomain, TimeDomain):
    pass


class MSSQLDateTimeDomain(MSSQLDomain, DateTimeDomain):
    pass


class MSSQLOpaqueDomain(MSSQLDomain, OpaqueDomain):
    """
    Represents an unsupported MS SQL Server data type.
    """


