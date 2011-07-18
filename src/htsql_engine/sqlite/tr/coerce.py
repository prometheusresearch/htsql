#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql_engine.sqlite.tr.coerce`
====================================
"""


from htsql.adapter import adapts
from htsql.domain import DecimalDomain, FloatDomain
from htsql.tr.coerce import UnaryCoerce


class SQLiteUnaryCoerceDecimal(UnaryCoerce):

    adapts(DecimalDomain)

    def __call__(self):
        return FloatDomain()


