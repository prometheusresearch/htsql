#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql.tr.plan`
====================

This module declares a SQL execution plan.
"""


from ..util import maybe, Printable
from .frame import QueryFrame


class Plan(Printable):
    """
    Represents a SQL execution plan.

    `sql` (a Unicode string or ``None``)
        The SQL statement to execute.

    `frame` (:class:`htsql.tr.frame.QueryFrame`)
        The query frame corresponding to the plan.
    """

    def __init__(self, sql, frame):
        assert isinstance(sql, maybe(unicode))
        assert isinstance(frame, QueryFrame)
        self.sql = sql
        self.frame = frame
        # Extract nodes that gave rise to the generated SQL.
        self.term = frame.term
        self.code = frame.expression
        self.binding = frame.binding
        self.syntax = frame.syntax
        self.mark = frame.mark

    def __unicode__(self):
        return (u"<%s>" % self.sql if self.sql is not None else u"<>")

    def __str__(self):
        return unicode(self).encode('utf-8')


