#
# Copyright (c) 2006-2012, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql.core.tr.plan`
=========================

This module declares a SQL execution plan.
"""


from ..util import maybe, Printable
from ..domain import Profile


class Plan(Printable):
    """
    Represents a SQL execution plan.

    `sql` (a Unicode string or ``None``)
        The SQL statement to execute.
    """

    def __init__(self, sql, profile):
        assert isinstance(sql, maybe(unicode))
        assert isinstance(profile, Profile)
        self.sql = sql
        self.profile = profile

    def __unicode__(self):
        return (u"<%s>" % self.sql if self.sql is not None else u"<>")

    def __str__(self):
        return unicode(self).encode('utf-8')


