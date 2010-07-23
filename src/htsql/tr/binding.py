#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.binding`
=======================

This module declares binding nodes.
"""


from ..entity import CatalogEntity, TableEntity, ColumnEntity, Join
from ..domain import Domain, VoidDomain, BooleanDomain, TupleDomain
from .syntax import Syntax
from ..util import maybe, listof, tupleof, Node


class Binding(Node):

    def __init__(self, parent, domain, syntax):
        assert isinstance(parent, Binding)
        assert isinstance(domain, Domain)
        assert isinstance(syntax, Syntax)

        self.parent = parent
        if parent is self:
            self.root = self
        else:
            self.root = parent.root
        self.domain = domain
        self.syntax = syntax
        self.mark = syntax.mark


class RootBinding(Binding):

    def __init__(self, catalog, syntax):
        assert isinstance(catalog, CatalogEntity)
        super(RootBinding, self).__init__(self, VoidDomain(), syntax)
        self.catalog = catalog


class QueryBinding(Binding):

    def __init__(self, parent, segment, syntax):
        assert isinstance(segment, maybe(SegmentBinding))
        super(QueryBinding, self).__init__(parent, VoidDomain(), syntax)
        self.segment = segment


class SegmentBinding(Binding):

    def __init__(self, parent, base, elements, syntax):
        assert isinstance(base, Binding)
        assert isinstance(elements, listof(Binding))
        super(SegmentBinding, self).__init__(parent, VoidDomain(), syntax)
        self.base = base
        self.elements = elements


class TableBinding(Binding):

    def __init__(self, parent, table, syntax):
        assert isinstance(table, TableEntity)
        super(TableBinding, self).__init__(parent, TupleDomain(), syntax)
        self.table = table


class FreeTableBinding(TableBinding):
    pass


class JoinedTableBinding(TableBinding):

    def __init__(self, parent, table, joins, syntax):
        assert isinstance(joins, listof(Join)) and len(joins) > 0
        super(JoinedTableBinding, self).__init__(parent, table, syntax)
        self.joins = joins


class ColumnBinding(Binding):

    def __init__(self, parent, column, syntax):
        assert isinstance(column, ColumnEntity)
        super(ColumnBinding, self).__init__(parent, column.domain, syntax)
        self.column = column


class LiteralBinding(Binding):

    def __init__(self, parent, value, domain, syntax):
        super(LiteralBinding, self).__init__(parent, domain, syntax)
        self.value = value


class SieveBinding(Binding):

    def __init__(self, parent, filter, syntax):
        assert isinstance(filter, Binding)
        super(SieveBinding, self).__init__(parent, parent.domain, syntax)
        self.filter = filter


class EqualityBinding(Binding):

    def __init__(self, parent, left, right, syntax):
        assert isinstance(left, Binding)
        assert isinstance(right, Binding)
        domain = BooleanDomain()
        super(EqualityBinding, self).__init__(parent, domain, syntax)
        self.left = left
        self.right = right


class InequalityBinding(Binding):

    def __init__(self, parent, left, right, syntax):
        assert isinstance(left, Binding)
        assert isinstance(right, Binding)
        domain = BooleanDomain()
        super(InequalityBinding, self).__init__(parent, domain, syntax)
        self.left = left
        self.right = right


class ConjunctionBinding(Binding):

    def __init__(self, parent, terms, syntax):
        assert isinstance(terms, listof(Binding))
        domain = BooleanDomain()
        super(ConjunctionBinding, self).__init__(parent, domain, syntax)
        self.terms = terms


class DisjunctionBinding(Binding):

    def __init__(self, parent, terms, syntax):
        assert isinstance(terms, listof(Binding))
        domain = BooleanDomain()
        super(DisjunctionBinding, self).__init__(parent, domain, syntax)
        self.terms = terms


class NegationBinding(Binding):

    def __init__(self, parent, term, syntax):
        assert isinstance(term, Binding)
        domain = BooleanDomain()
        super(NegationBinding, self).__init__(parent, domain, syntax)
        self.term = term


class CastBinding(Binding):

    def __init__(self, parent, binding, domain, syntax):
        super(CastBinding, self).__init__(parent, domain, syntax)
        self.binding = binding


class OrderedBinding(Binding):

    def __init__(self, parent, order, limit, offset, syntax):
        assert isinstance(order, listof(tupleof(Binding, int)))
        assert isinstance(limit, maybe(int))
        assert isinstance(offset, maybe(int))
        super(OrderedBinding, self).__init__(parent, parent.domain, syntax)
        self.order = order
        self.limit = limit
        self.offset = offset


class TupleBinding(Binding):

    def __init__(self, binding):
        super(TupleBinding, self).__init__(binding.parent, BooleanDomain(),
                                           binding.syntax)
        self.binding = binding


class FunctionBinding(Binding):

    def __init__(self, parent, domain, syntax, **arguments):
        super(FunctionBinding, self).__init__(parent, domain, syntax)
        self.arguments = arguments
        for key in arguments:
            setattr(self, key, arguments[key])


