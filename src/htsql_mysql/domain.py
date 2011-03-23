#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_mysql.domain`
=========================

This module defines MySQL-specific data types.
"""


from htsql.domain import (Domain, BooleanDomain, IntegerDomain, DecimalDomain,
                          FloatDomain, StringDomain, EnumDomain, DateDomain,
                          TimeDomain, DateTimeDomain, OpaqueDomain)


class MySQLDomain(Domain):
    """
    Represents a MySQL data type.

    This is an abstract mixin class; see subclasses for concrete data types.

    `name` (a string)
        The name of the type.
    """

    def __init__(self, name, **attributes):
        # Sanity check on the arguments.
        assert isinstance(name, str)

        # Pass the attributes to the concrete domain constructor.
        super(MySQLDomain, self).__init__(**attributes)
        self.name = name

    def __str__(self):
        return self.name

    def __eq__(self, other):
        # The generic domain comparison checks if the types of the domains
        # and all their attributes are equal.  Since MySQL domains grow
        # an extra attribute `name`, we need to update the comparison
        # implementation.
        return (super(MySQLDomain, self).__eq__(other) and
                self.name == other.name)


class MySQLBooleanDomain(MySQLDomain, BooleanDomain):
    """
    Represents a pseudo-Boolean type for MySQL.

    In MySQL, Boolean values are expressed as integers; ``0`` is the FALSE
    value, any non-zero integer is a TRUE value.
    """


class MySQLIntegerDomain(MySQLDomain, IntegerDomain):
    """
    Represents MySQL ``TINYINT``, ``SMALLINT``, ``INT``, and ``BIGINT``
    data types.
    """


class MySQLDecimalDomain(MySQLDomain, DecimalDomain):
    """
    Represents a MySQL ``DECIMAL`` data type.
    """


class MySQLFloatDomain(MySQLDomain, FloatDomain):
    """
    Represents MySQL ``FLOAT`` and ``DOUBLE`` data types.
    """


class MySQLStringDomain(MySQLDomain, StringDomain):
    """
    Represents MySQL ``CHAR``, ``VARCHAR``, ``TINYTEXT``, ``TEXT``,
    and ``LONGTEXT`` data types.
    """


class MySQLEnumDomain(MySQLDomain, EnumDomain):
    """
    Represents a MySQL ``ENUM`` data type.
    """


class MySQLDateDomain(MySQLDomain, DateDomain):
    """
    Represents a MySQL ``DATE`` data type.
    """


class MySQLTimeDomain(MySQLDomain, TimeDomain):
    pass


class MySQLDateTimeDomain(MySQLDomain, DateTimeDomain):
    pass


class MySQLOpaqueDomain(MySQLDomain, OpaqueDomain):
    """
    Represents an unsupported MySQL data type.
    """


