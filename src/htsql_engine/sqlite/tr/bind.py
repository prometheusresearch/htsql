# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
# See `LICENSE` for license information, `AUTHORS` for the list of authors.


"""
:mod:`htsql_engine.sqlite.tr.bind`
==================================

This module adapts HTSQL binder for SQLite.
"""


from htsql.domain import IntegerDomain, FloatDomain
from htsql.tr.fn.bind import (correlates, CorrelateDecimalRoundTo,
                              CorrelateDecimalTruncTo,
                              CorrelateDecimalAvg)
from htsql.tr.fn.signature import RoundToSig, TruncToSig


class SQLiteCorrelateDecimalAvg(CorrelateDecimalAvg):

    domains = [FloatDomain()]
    codomain = FloatDomain()


class SQLiteCorrelateFloatRoundTo(CorrelateDecimalRoundTo):

    correlates(RoundToSig, (FloatDomain, IntegerDomain))


class SQLiteCorrelateFloatTruncTo(CorrelateDecimalTruncTo):

    correlates(TruncToSig, (FloatDomain, IntegerDomain))


