# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_sqlite.tr.bind`
===========================

This module adapts HTSQL binder for SQLite.
"""


from htsql.domain import IntegerDomain, FloatDomain
from htsql.tr.fn.bind import (correlates, CorrelateDecimalRoundTo,
                              CorrelateDecimalAvg)
from htsql.tr.fn.signature import RoundToSig


class SQLiteCorrelateDecimalAvg(CorrelateDecimalAvg):

    domains = [FloatDomain()]
    codomain = FloatDomain()


class SQLiteCorrelateFloatRoundTo(CorrelateDecimalRoundTo):

    correlates(RoundToSig, (FloatDomain, IntegerDomain))


