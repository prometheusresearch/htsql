#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from htsql.core.adapter import adapts
from htsql.core.domain import DecimalDomain, FloatDomain
from htsql.core.tr.coerce import UnaryCoerce


class SQLiteUnaryCoerceDecimal(UnaryCoerce):

    adapts(DecimalDomain)

    def __call__(self):
        return FloatDomain()


