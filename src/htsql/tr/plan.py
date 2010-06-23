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


from ..util import maybe, Node
from ..mark import Mark
from .frame import QueryFrame


class Plan(Node):

    def __init__(self, frame, sql, mark):
        assert isinstance(frame, QueryFrame)
        assert isinstance(sql, maybe(str))
        assert isinstance(mark, Mark)
        self.frame = frame
        self.sketch = frame.sketch
        self.term = frame.term
        self.code = frame.code
        self.binding = frame.binding
        self.syntax = frame.syntax
        self.sql = sql
        self.mark = mark


