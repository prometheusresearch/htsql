#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.term`
====================

This module declares term nodes.
"""


from ..util import Node, listof, dictof, oneof, tupleof, maybe
from ..mark import Mark
from ..entity import TableEntity
from .code import Space, Code, Unit, QueryCode


LEFT = 0
RIGHT = 1
FORWARD = 0


class Term(Node):

    is_nullary = False
    is_unary = False
    is_binary = False

    def __init__(self, children, mark):
        assert isinstance(children, listof(Term))
        assert isinstance(mark, Mark)
        self.children = children
        self.mark = mark


class NullaryTerm(Term):

    is_nullary = True

    def __init__(self, space, baseline, routes, mark):
        assert isinstance(space, Space)
        assert isinstance(baseline, Space) and baseline.axes() == baseline
        assert isinstance(routes, dictof(oneof(Space, Unit), listof(int)))
        super(NullaryTerm, self).__init__([], mark)
        self.space = space
        self.baseline = baseline
        self.routes =  routes


class UnaryTerm(Term):

    is_unary = True

    def __init__(self, child, space, baseline, routes, mark):
        assert isinstance(child, Term)
        assert isinstance(space, Space)
        assert isinstance(baseline, Space) and baseline.axes() == baseline
        assert isinstance(routes, dictof(oneof(Space, Unit), listof(object)))
        super(UnaryTerm, self).__init__([child], mark)
        self.child = child
        self.space = space
        self.baseline = baseline
        self.routes = routes


class BinaryTerm(Term):

    is_binary = True

    def __init__(self, left_child, right_child,
                 space, baseline, routes, mark):
        assert isinstance(left_child, Term)
        assert isinstance(right_child, Term)
        assert isinstance(space, Space)
        assert isinstance(baseline, Space) and baseline.axes() == baseline
        assert isinstance(routes, dictof(oneof(Space, Unit), listof(object)))
        super(BinaryTerm, self).__init__([left_child, right_child], mark)
        self.left_child = left_child
        self.right_child = right_child
        self.space = space
        self.baseline = baseline
        self.routes = routes


class TableTerm(NullaryTerm):

    def __init__(self, table, space, baseline, routes, mark):
        assert isinstance(table, TableEntity)
        assert space.table is table
        super(TableTerm, self).__init__(space, baseline, routes, mark)
        self.table = table


class ScalarTerm(NullaryTerm):

    def __init__(self, space, baseline, routes, mark):
        assert space.table is None
        super(ScalarTerm, self).__init__(space, baseline, routes, mark)


class FilterTerm(UnaryTerm):

    def __init__(self, child, filter, space, baseline, routes, mark):
        assert isinstance(filter, Expression)
        assert isinstance(filter.domain, BooleanDomain)
        super(FilterTerm, self).__init__(term, space, baseline,
                                         routes, mark)
        self.filter = filter


class JoinTerm(BinaryTerm):

    def __init__(self, left_child, right_child, ties, is_inner,
                 space, baseline, routes, mark):
        assert isinstance(ties, listof(Tie))
        assert isinstance(is_inner, bool)
        super(JoinTerm, self).__init__(left_child, right_child,
                                       space, baseline, routes, mark)
        self.ties = ties
        self.is_inner = is_inner


class CorrelationTerm(BinaryTerm):

    def __init__(self, left_child, right_child, ties,
                 space, baseline, routes, mark):
        assert isinstance(ties, listof(Tie))
        super(CorrelationTerm, self).__init__(left_child, right_child,
                                              space, baseline, routes, mark)
        self.ties = ties


class ProjectionTerm(UnaryTerm):

    def __init__(self, child, ties, space, baseline, routes, mark):
        assert isinstance(ties, listof(Ties))
        super(ProjectionTerm).__init__(child, space, baseline, routes, mark)
        self.ties = ties


class OrderingTerm(UnaryTerm):

    def __init__(self, child, order, limit, offset,
                 space, baseline, routes, mark):
        assert isinstance(order, listof(tupleof(Code, int)))
        assert isinstance(limit, maybe(int))
        assert isinstance(offset, maybe(int))
        super(OrderingTerm, self).__init__(child, space, baseline,
                                           routes, mark)
        self.order = order
        self.limit = limit
        self.offset = offset


class WrapperTerm(UnaryTerm):
    pass


class SegmentTerm(UnaryTerm):

    def __init__(self, child, select, space, baseline, routes, mark):
        assert isinstance(select, listof(Code))
        super(SegmentTerm, self).__init__(child, space, baseline, routes, mark)
        self.select = select


class QueryTerm(Term):

    def __init__(self, code, segment, mark):
        assert isinstance(code, QueryCode)
        assert isinstance(segment, maybe(SegmentTerm))
        children = []
        if segment is not None:
            children.append(segment)
        super(QueryTerm, self).__init__(children, mark)
        self.code = code
        self.binding = code.binding
        self.syntax = code.syntax
        self.segment = segment


class Tie(Node):

    is_parallel = False
    is_series = False


class ParallelTie(Tie):

    is_parallel = True

    def __init__(self, space):
        assert isinstance(space, Space) and space.is_axis
        self.space = space


class SeriesTie(Tie):

    is_series = True

    def __init__(self, space, is_reverse=False):
        assert isinstance(space, Space) and space.is_axis
        assert isinstance(is_reverse, bool)
        self.space = space
        self.is_reverse = is_reverse


