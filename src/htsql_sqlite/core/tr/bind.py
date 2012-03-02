#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from htsql.core.domain import IntegerDomain, FloatDomain
from htsql.core.tr.fn.bind import (correlates, CorrelateDecimalRoundTo,
                                   CorrelateDecimalTruncTo,
                                   CorrelateDecimalAvg)
from htsql.core.tr.fn.signature import RoundToSig, TruncToSig


class SQLiteCorrelateDecimalAvg(CorrelateDecimalAvg):

    domains = [FloatDomain()]
    codomain = FloatDomain()


class SQLiteCorrelateFloatRoundTo(CorrelateDecimalRoundTo):

    correlates(RoundToSig, (FloatDomain, IntegerDomain))


class SQLiteCorrelateFloatTruncTo(CorrelateDecimalTruncTo):

    correlates(TruncToSig, (FloatDomain, IntegerDomain))


