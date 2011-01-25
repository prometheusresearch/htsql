#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_pgsql.domain`
=========================

This module defines PostgreSQL-specific data types.
"""


from htsql.domain import (Domain, BooleanDomain, IntegerDomain, FloatDomain,
                          DecimalDomain, StringDomain, EnumDomain, DateDomain,
                          OpaqueDomain)


class PGDomain(Domain):
    """
    Represents a PostgreSQL data type.

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
        super(PGDomain, self).__init__(**attributes)
        self.schema_name = schema_name
        self.name = name

    def __str__(self):
        return "%s.%s" % (self.schema_name, self.name)

    def __eq__(self, other):
        # The generic domain comparison checks if the types of the domains
        # and all their attributes are equal.  For PostgreSQL domains,
        # we add an extra check to verify that their names are equal too.
        return (super(PGDomain, self).__eq__(other) and
                self.schema_name == other.schema_name and
                self.name == other.name)


class PGBooleanDomain(PGDomain, BooleanDomain):
    """
    Represents the ``BOOLEAN`` data type.
    """


class PGIntegerDomain(PGDomain, IntegerDomain):
    """
    Represents the ``SMALLINT``, ``INTEGER`` and ``BIGINT`` data types.
    """


class PGFloatDomain(PGDomain, FloatDomain):
    """
    Represents the ``REAL`` and ``DOUBLE PRECISION`` data types.
    """


class PGDecimalDomain(PGDomain, DecimalDomain):
    """
    Represents the ``NUMERIC`` data type.
    """


class PGCharDomain(PGDomain, StringDomain):
    """
    Represents the ``CHAR`` data type.
    """


class PGVarCharDomain(PGDomain, StringDomain):
    """
    Represents the ``VARCHAR`` data type.
    """


class PGTextDomain(PGDomain, StringDomain):
    """
    Represents the ``TEXT`` data type.
    """


class PGEnumDomain(PGDomain, EnumDomain):
    """
    Represents the ``ENUM`` data types.
    """


class PGDateDomain(PGDomain, DateDomain):
    """
    Represents the ``DATE`` data type.
    """


class PGOpaqueDomain(PGDomain, OpaqueDomain):
    """
    Represents an unsupported PostgreSQL data type.
    """


