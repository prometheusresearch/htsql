#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.plan`
====================

This module implements a plan node.
"""


from ..util import maybe, Printable
from ..mark import Mark
from .frame import QueryFrame


class Plan(Printable):

    def __init__(self, frame, sql, mark):
        assert isinstance(frame, QueryFrame)
        assert isinstance(sql, maybe(str))
        assert isinstance(mark, Mark)
        self.frame = frame
        self.term = frame.term
        self.code = frame.expression
        self.binding = frame.binding
        self.syntax = frame.syntax
        self.sql = sql
        self.mark = mark

    def __str__(self):
        return ("<%s>" % self.sql if self.sql is not None else "<>")


