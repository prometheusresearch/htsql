#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql.core.tr.plan`
=========================

This module declares a SQL execution plan.
"""


from ..util import maybe, listof, Printable
from ..domain import Profile, Domain


class Statement(Printable):

    def __init__(self, sql, domains, substatements):
        assert isinstance(sql, unicode)
        assert isinstance(domains, listof(Domain))
        assert isinstance(substatements, listof(Statement))
        self.sql = sql
        self.domains = domains
        self.substatements = substatements


class Plan(Printable):

    def __init__(self, profile, statement, compose):
        assert isinstance(profile, Profile)
        assert isinstance(statement, maybe(Statement))
        self.profile = profile
        self.statement = statement
        self.compose = compose


