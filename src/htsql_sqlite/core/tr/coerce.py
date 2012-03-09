#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from htsql.core.adapter import adapt
from htsql.core.domain import DecimalDomain, FloatDomain
from htsql.core.tr.coerce import UnaryCoerce


class SQLiteUnaryCoerceDecimal(UnaryCoerce):

    adapt(DecimalDomain)

    def __call__(self):
        return FloatDomain()


