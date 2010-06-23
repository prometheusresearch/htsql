#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.code`
====================

This module declares space and code nodes.
"""


from ..util import maybe, listof, tupleof, Node
from ..mark import Mark
from ..entity import TableEntity, ColumnEntity, Join
from ..domain import Domain, BooleanDomain, VoidDomain
from .binding import Binding, QueryBinding


class Code(Node):

    def __init__(self, mark, hash=None):
        assert isinstance(mark, Mark)
        if hash is None:
            hash = id(self)
        self.mark = mark
        self.hash = hash

    def __hash__(self):
        return hash(self.hash)

    def __eq__(self, other):
        return (isinstance(other, Code) and self.hash == other.hash)


class Space(Code):

    is_axis = False

    def __init__(self, parent, table,
                 is_contracting, is_expanding, mark,
                 hash=None):
        assert isinstance(parent, maybe(Space))
        assert isinstance(table, maybe(TableEntity))
        super(Space, self).__init__(mark, hash=hash)
        self.parent = parent
        self.table = table
        self.is_contracting = is_contracting
        self.is_expanding = is_expanding
        if self.parent is not None:
            self.scalar = self.parent.scalar
        else:
            self.scalar = self

    def unfold(self):
        components = []
        component = self
        while component is not None:
            components.append(component)
            component = component.parent
        return components

    def axes(self):
        space = None
        for component in reversed(self.unfold()):
            if component.is_axis:
                space = component.clone(parent=space)
        return space


class ScalarSpace(Space):

    is_axis = True

    def __init__(self, parent, mark):
        assert parent is None
        super(ScalarSpace, self).__init__(None, None, False, False, mark,
                                          hash=(self.__class__))


class FreeTableSpace(Space):

    is_axis = True

    def __init__(self, parent, table, mark):
        super(FreeTableSpace, self).__init__(parent, table, True, True, mark,
                                             hash=(self.__class__,
                                                   parent.hash, table))


class JoinedTableSpace(Space):

    is_axis = True

    def __init__(self, parent, join, mark):
        assert isinstance(join, Join)
        super(JoinedTableSpace, self).__init__(parent, join.target,
                                               join.is_contracting,
                                               join.is_expanding,
                                               mark,
                                               hash=(self.__class__,
                                                     parent.hash,
                                                     join.__class__,
                                                     join.foreign_key))
        self.join = join


class ScreenSpace(Space):

    def __init__(self, parent, filter, mark):
        assert isinstance(filter, Code)
        assert isinstance(filter.domain, BooleanDomain)
        super(ScreenSpace, self).__init__(parent, parent.table,
                                          True, False, mark,
                                          hash=(self.__class__,
                                                parent.hash,
                                                filter.hash))
        self.filter = filter


class OrderedSpace(Space):

    def __init__(self, parent, order, limit, offset, mark):
        assert isinstance(order, listof(tupleof(Expression, int)))
        assert isinstance(limit, maybe(int))
        assert isinstance(offset, maybe(int))
        is_expanding = (limit is None and offset is None)
        super(OrderedSpace, self).__init__(parent, parent.table,
                                           True, is_expanding, mark,
                                           hash=(self.__class__,
                                                 parent.hash,
                                                 tuple(order),
                                                 limit, offset))
        self.order = order
        self.limit = limit
        self.offset = offset


class Expression(Code):

    def __init__(self, domain, mark, hash=None):
        assert isinstance(domain, Domain)
        super(Expression, self).__init__(mark, hash=hash)
        self.domain = domain

    def get_units(self):
        return []


class LiteralExpression(Expression):

    def __init__(self, value, domain, mark):
        super(LiteralExpression, self).__init__(domain, mark,
                                                hash=(self.__class__,
                                                      value, domain))
        self.value = value


class ElementExpression(Expression):

    def __init__(self, code, order):
        assert isinstance(code, Code)
        assert order is None or order in [+1, -1]
        super(ElementExpression, self).__init__(code.domain, code.mark)
        self.code = code
        self.order = order

    def get_units(self):
        return self.code.get_units()


class Unit(Expression):

    def __init__(self, domain, space, mark, hash=None):
        assert isinstance(space, Space)
        super(Unit, self).__init__(domain, mark, hash=hash)
        self.space = space

    def get_units(self):
        return [self]


class ColumnUnit(Unit):

    def __init__(self, column, space, mark):
        assert isinstance(column, ColumnEntity)
        super(ColumnUnit, self).__init__(column.domain, space, mark,
                                         hash=(self.__class__,
                                               column, space.hash))
        self.column = column


class AggregateUnit(Unit):

    def __init__(self, expression, plural_space, space, mark):
        assert isinstance(expression, Expression)
        assert isinstance(plural_space, Space)
        super(AggregateUnit, self).__init__(expression.domain, space, mark,
                                            hash=(self.__class__,
                                                  expression.hash,
                                                  plural_space.hash,
                                                  space.hash))
        self.expression = expression
        self.plural_space = plural_space
        self.space = space


class QueryCode(Code):

    def __init__(self, binding, segment, mark):
        assert isinstance(binding, QueryBinding)
        assert isinstance(segment, maybe(SegmentCode))
        super(QueryCode, self).__init__(mark)
        self.binding = binding
        self.syntax = binding.syntax
        self.segment = segment


class SegmentCode(Code):

    def __init__(self, space, elements, mark):
        assert isinstance(space, Space)
        assert isinstance(elements, listof(Expression))
        super(SegmentCode, self).__init__(mark)
        self.space = space
        self.elements = elements


