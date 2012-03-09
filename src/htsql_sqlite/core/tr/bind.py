#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from htsql.core.domain import IntegerDomain, FloatDomain
from htsql.core.tr.fn.bind import (match, CorrelateDecimalRoundTo,
                                   CorrelateDecimalTruncTo,
                                   CorrelateDecimalAvg)
from htsql.core.tr.fn.signature import RoundToSig, TruncToSig


class SQLiteCorrelateDecimalAvg(CorrelateDecimalAvg):

    domains = [FloatDomain()]
    codomain = FloatDomain()


class SQLiteCorrelateFloatRoundTo(CorrelateDecimalRoundTo):

    match(RoundToSig, (FloatDomain, IntegerDomain))


class SQLiteCorrelateFloatTruncTo(CorrelateDecimalTruncTo):

    match(TruncToSig, (FloatDomain, IntegerDomain))


