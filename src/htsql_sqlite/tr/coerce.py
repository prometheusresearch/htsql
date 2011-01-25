#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_sqlite.tr.coerce`
=============================
"""


from htsql.adapter import adapts
from htsql.domain import DecimalDomain, FloatDomain
from htsql.tr.coerce import UnaryCoerce


class SQLiteUnaryCoerceDecimal(UnaryCoerce):

    adapts(DecimalDomain)

    def __call__(self):
        return FloatDomain()


