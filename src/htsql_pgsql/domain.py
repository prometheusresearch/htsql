#
# Copyright (c) 2006-2010, Prometheus Research, LLC
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

    def __init__(self, schema_name, name, **attributes):
        super(PGDomain, self).__init__(**attributes)
        self.schema_name = schema_name
        self.name = name

    def __str__(self):
        return "%s.%s" % (self.schema_name, self.name)


class PGBooleanDomain(PGDomain, BooleanDomain):
    pass


class PGIntegerDomain(PGDomain, IntegerDomain):
    pass


class PGFloatDomain(PGDomain, FloatDomain):
    pass


class PGDecimalDomain(PGDomain, DecimalDomain):
    pass


class PGCharDomain(PGDomain, StringDomain):
    pass


class PGVarCharDomain(PGDomain, StringDomain):
    pass


class PGTextDomain(PGDomain, StringDomain):
    pass


class PGEnumDomain(PGDomain, EnumDomain):
    pass


class PGDateDomain(PGDomain, DateDomain):
    pass


class PGOpaqueDomain(PGDomain, OpaqueDomain):
    pass


