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
from ..domain import BooleanDomain
from .code import Expression, Space, Code, Unit, QueryExpression


class Term(Node):

    def __init__(self, expression):
        assert isinstance(expression, Expression)
        self.expression = expression
        self.binding = expression.binding
        self.syntax = expression.syntax
        self.mark = expression.mark

    def __str__(self):
        return str(self.expression)


class RoutingTerm(Term):

    is_nullary = False
    is_unary = False
    is_binary = False

    def __init__(self, id, kids, space, routes):
        assert isinstance(id, int)
        assert isinstance(kids, listof(RoutingTerm))
        assert isinstance(space, Space)
        assert isinstance(routes, dictof(oneof(Space, Unit), int))
        assert space in routes
        backbone = space.inflate()
        assert backbone in routes
        baseline = backbone
        while baseline.base in routes:
            baseline = baseline.base
        super(RoutingTerm, self).__init__(space)
        self.id = id
        self.kids = kids
        self.space = space
        self.routes = routes
        self.backbone = backbone
        self.baseline = baseline

    def __str__(self):
        # Display:
        #   <baseline> -> <space>
        return "%s -> %s" % (self.baseline, self.space)


class NullaryTerm(RoutingTerm):

    is_nullary = True

    def __init__(self, id, space, routes):
        super(NullaryTerm, self).__init__(id, [], space, routes)


class UnaryTerm(RoutingTerm):

    is_unary = True

    def __init__(self, id, kid, space, routes):
        super(UnaryTerm, self).__init__(id, [kid], space, routes)
        self.kid = kid


class BinaryTerm(RoutingTerm):

    is_binary = True

    def __init__(self, id, lkid, rkid, space, routes):
        super(BinaryTerm, self).__init__(id, [lkid, rkid], space, routes)
        self.lkid = lkid
        self.rkid = rkid


class ScalarTerm(NullaryTerm):

    def __init__(self, id, space, routes):
        assert space.table is None
        super(ScalarTerm, self).__init__(id, space, routes)


class TableTerm(NullaryTerm):

    def __init__(self, id, space, routes):
        assert space.table is not None
        super(TableTerm, self).__init__(id, space, routes)
        self.table = space.table


class FilterTerm(UnaryTerm):

    def __init__(self, id, kid, filter, space, routes):
        assert (isinstance(filter, Code) and
                isinstance(filter.domain, BooleanDomain))
        super(FilterTerm, self).__init__(id, kid, space, routes)
        self.filter = filter


class JoinTerm(BinaryTerm):

    def __init__(self, id, lkid, rkid, ties, is_inner, space, routes):
        assert isinstance(ties, listof(Tie))
        assert isinstance(is_inner, bool)
        super(JoinTerm, self).__init__(id, lkid, rkid, space, routes)
        self.ties = ties
        self.is_inner = is_inner


class CorrelationTerm(BinaryTerm):

    def __init__(self, id, lkid, rkid, ties, space, routes):
        assert isinstance(ties, listof(Tie))
        super(CorrelationTerm, self).__init__(id, lkid, rkid, space, routes)
        self.ties = ties


class ProjectionTerm(UnaryTerm):

    def __init__(self, id, kid, ties, space, routes):
        assert isinstance(ties, listof(Tie))
        super(ProjectionTerm, self).__init__(id, kid, space, routes)
        self.ties = ties


class OrderTerm(UnaryTerm):

    def __init__(self, id, kid, order, limit, offset, space, routes):
        assert isinstance(order, listof(tupleof(Code, int)))
        assert isinstance(limit, maybe(int))
        assert isinstance(offset, maybe(int))
        super(OrderTerm, self).__init__(id, kid, space, routes)
        self.order = order
        self.limit = limit
        self.offset = offset


class WrapperTerm(UnaryTerm):
    pass


class SegmentTerm(UnaryTerm):

    def __init__(self, id, kid, elements, space, routes):
        assert isinstance(elements, listof(Code))
        super(SegmentTerm, self).__init__(id, kid, space, routes)
        self.elements = elements


class QueryTerm(Term):

    def __init__(self, segment, expression):
        assert isinstance(segment, maybe(SegmentTerm))
        assert isinstance(expression, QueryExpression)
        super(QueryTerm, self).__init__(expression)
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

    def __init__(self, space, is_backward=False):
        assert isinstance(space, Space) and space.is_axis
        assert isinstance(is_backward, bool)
        self.space = space
        self.is_reverse = is_backward


