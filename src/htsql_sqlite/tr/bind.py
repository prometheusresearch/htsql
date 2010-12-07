# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_sqlite.tr.bind`
===========================

This module adapts HTSQL binder for SQLite.
"""


from htsql.domain import FloatDomain
from htsql.tr.fn.bind import CorrelateDecimalAvg


class SQLiteCorrelateDecimalAvg(CorrelateDecimalAvg):

    domains = [FloatDomain()]
    codomain = FloatDomain()


