#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_oracle.domain`
==========================

This module defines Oracle-specific data types.
"""


from htsql.domain import (Domain, BooleanDomain, IntegerDomain, DecimalDomain,
                          FloatDomain, StringDomain, DateDomain, TimeDomain,
                          DateTimeDomain, OpaqueDomain)


class OracleDomain(Domain):
    """
    Represents an Oracle data type.

    This is an abstract mixin class; see subclasses for concrete data types.

    `name` (a string)
        The name of the type.
    """

    def __init__(self, name, **attributes):
        # Sanity check on the arguments.
        assert isinstance(name, str)

        # Pass the attributes to the concrete domain constructor.
        super(OracleDomain, self).__init__(**attributes)
        self.name = name

    def __str__(self):
        return self.name

    def __eq__(self, other):
        # The generic domain comparison checks if the types of the domains
        # and all their attributes are equal.  Since Oracle domains grow
        # an extra attribute `name`, we need to update the comparison
        # implementation.
        return (super(OracleDomain, self).__eq__(other) and
                self.name == other.name)


class OracleBooleanDomain(OracleDomain, BooleanDomain):
    """
    Represents a pseudo-Boolean type for Oracle.

    In Oracle, we express Boolean values as integers; ``0`` is the FALSE
    value, any non-zero integer is a TRUE value.
    """


class OracleIntegerDomain(OracleDomain, IntegerDomain):
    """
    Represents the Oracle ``INTEGER`` (actually, ``NUMBER(38)``) data type.
    """


class OracleDecimalDomain(OracleDomain, DecimalDomain):
    """
    Represents an Oracle ``NUMBER`` data type.
    """


class OracleFloatDomain(OracleDomain, FloatDomain):
    """
    Represents Oracle ``BINARY_FLOAT`` and ``BINARY_DOUBLE`` data types.
    """


class OracleStringDomain(OracleDomain, StringDomain):
    """
    Represents Oracle ``CHAR``, ``NCHAR``, ``VARCHAR2``, ``NVARCHAR2``,
    ``CLOB``, ``NCLOB``, and ``LONG`` data types.
    """


class OracleDateDomain(OracleDomain, DateDomain):
    """
    Represents an Oracle ``DATE`` data type.
    """


class OracleTimeDomain(OracleDomain, TimeDomain):
    pass


class OracleDateTimeDomain(OracleDomain, DateTimeDomain):
    pass


class OracleOpaqueDomain(OracleDomain, OpaqueDomain):
    """
    Represents an unsupported Oracle data type.
    """


