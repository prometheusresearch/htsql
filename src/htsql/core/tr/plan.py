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

    def __init__(self, sql, domains):
        assert isinstance(sql, unicode)
        assert isinstance(domains, listof(Domain))
        self.sql = sql
        self.domains = domains


class Plan(Printable):

    def __init__(self, profile, statement, compose):
        assert isinstance(profile, Profile)
        assert isinstance(statement, maybe(Statement))
        self.profile = profile
        self.statement = statement
        self.compose = compose

    def __unicode__(self):
        return (u"<%s>" % self.statement.sql
                if self.statement is not None else u"<>")

    def __str__(self):
        return unicode(self).encode('utf-8')


