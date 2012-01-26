#
# Copyright (c) 2006-2012, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
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
        assert compose is not None
        self.profile = profile
        self.statement = statement
        self.compose = compose

    def __unicode__(self):
        return (u"<%s>" % self.statement.sql
                if self.statement is not None else u"<>")

    def __str__(self):
        return unicode(self).encode('utf-8')


class ComposeNone(object):

    def __call__(self, rows):
        return None


class ComposeRecord(object):

    def __init__(self, record_class, null_index, compose_fields):
        self.record_class = record_class
        self.null_index = null_index
        self.compose_fields = compose_fields

    def __call__(self, row):
        if self.null_index is not None and row[self.null_index] is None:
            return None
        else:
            fields = [compose_field(row)
                      for compose_field in self.compose_fields]
            return self.record_class(*fields)


class ComposeValue(object):

    def __init__(self, index):
        self.index = index

    def __call__(self, row):
        return row[self.index]


