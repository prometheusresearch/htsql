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

    def inflate(self, other):
        self_components = self.unfold()
        other_components = other.unfold()
        space = None
        while self_components and other_components:
            self_component = self_components[-1]
            other_component = other_components[-1]
            if self_component.resembles(other_component):
                if self_component.is_axis:
                    space = self_component.clone(parent=space)
                self_components.pop()
                other_components.pop()
            elif not other_component.is_axis:
                other_components.pop()
            elif not self_component.is_axis:
                space = self_component.clone(parent=space)
                self_components.pop()
            else:
                break
        while self_components:
            component = self_components.pop()
            space = component.clone(parent=space)
        return space

    def spans(self, other):
        if self is other or self == other:
            return True
        self_axes = self.axes().unfold()
        other_axes = other.axes().unfold()
        while self_axes and other_axes:
            if self_axes[-1].resembles(other_axes[-1]):
                self_axes.pop()
                other_axes.pop()
            else:
                break
        for other_axis in other_axes:
            if not other_axis.is_contracting:
                return False
        return True

    def conforms(self, other):
        if self is other or self == other:
            return True
        self_components = self.unfold()
        other_components = other.unfold()
        while self_components and other_components:
            self_component = self_components[-1]
            other_component = other_components[-1]
            if self_component.resembles(other_component):
                self_components.pop()
                other_components.pop()
            elif (self_component.is_contracting and
                  self_component.is_expanding and
                  not self_component.is_axis):
                self_components.pop()
            elif (other_component.is_contrating and
                  other_component.is_expanding and
                  not other_component.is_axis):
                other_components.pop()
            else:
                break
        for component in self_components + other_components:
            if not (component.is_contracting and component.is_expanding):
                return False
        return True

    def dominates(self, other):
        if self is other or self == other:
            return True
        self_components = self.unfold()
        other_components = other.unfold()
        while self_components and other_components:
            self_component = self_components[-1]
            other_component = other_components[-1]
            if self_component.resembles(other_component):
                self_components.pop()
                other_components.pop()
            elif (other_component.is_contrating and
                  not other_component.is_axis):
                other_components.pop()
            else:
                break
        for component in self_components:
            if not component.is_expanding:
                return False
        for component in other_components:
            if not component.is_contracting:
                return False
        return True

    def concludes(self, other):
        if self is other or self == other:
            return True
        space = self
        while space is not None:
            if space == other:
                return True
            space = space.parent
        return False

    def resembles(self, other):
        return False

    def ordering(self, with_strong=True, with_weak=True):
        return self.parent.ordering(with_strong, with_weak)


class ScalarSpace(Space):

    is_axis = True

    def __init__(self, parent, mark):
        assert parent is None
        super(ScalarSpace, self).__init__(None, None, False, False, mark,
                                          hash=(self.__class__))

    def resembles(self, other):
        return isinstance(other, ScalarSpace)

    def ordering(self, with_strong=True, with_weak=True):
        return []


class FreeTableSpace(Space):

    is_axis = True

    def __init__(self, parent, table, mark):
        super(FreeTableSpace, self).__init__(parent, table, False, False, mark,
                                             hash=(self.__class__,
                                                   parent.hash, table))

    def resembles(self, other):
        return isinstance(other, FreeTableSpace)

    def ordering(self, with_strong=True, with_weak=True):
        order = []
        if with_strong:
            order += self.parent.ordering(with_strong=True, with_weak=False)
        if with_weak:
            order += self.parent.ordering(with_strong=False, with_weak=True)
            if self.table.primary_key is not None:
                for column_name in self.table.primary_key.origin_column_names:
                    column = self.table.columns[column_name]
                    code = ColumnUnit(column, self, self.mark)
                    order.append((code, +1))
        return order


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

    def resembles(self, other):
        return (isinstance(other, JoinedTableSpace) and
                self.join is other.join)

    def ordering(self, with_strong=True, with_weak=True):
        order = []
        if with_strong:
            order += self.parent.ordering(with_strong=True, with_weak=False)
        if with_weak:
            order += self.parent.ordering(with_strong=False, with_weak=True)
            if not self.is_contracting and self.table.primary_key is not None:
                for column_name in self.table.primary_key.origin_column_names:
                    column = self.table.columns[column_name]
                    code = ColumnUnit(column, self, self.mark)
                    order.append((code, +1))
        return order


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

    def resembles(self, other):
        return (isinstance(other, ScreenSpace) and
                self.filter == other.filter)


class RelativeSpace(Space):

    def __init__(self, parent, filter, mark):
        assert isinstance(filter, Space)
        super(RelativeSpace, self).__init__(parent, parent.table,
                                            True, False, mark,
                                            hash=(self.__class__,
                                                  parent.hash,
                                                  filter.hash))
        self.filter = filter

    def resembles(self, other):
        return (isinstance(other, RelativeSpace) and
                self.filter == other.filter)


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

    def resembles(self, other):
        return (isinstance(other, OrderedSpace) and
                self.order == other.order and
                self.limit == other.limit and
                self.offset == other.offset)

    def ordering(self, with_strong=True, with_weak=True):
        order = []
        if with_strong:
            order += self.parent.ordering(with_strong=True, with_weak=False)
            order += self.order
        if with_weak:
            order += self.parent.ordering(with_strong=False, with_weak=True)
        return order


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


class EqualityExpression(Expression):

    def __init__(self, left, right, mark):
        assert isinstance(left, Expression)
        assert isinstance(right, Expression)
        domain = BooleanDomain()
        super(EqualityExpression, self).__init__(domain, mark,
                                                 hash=(self.__class__,
                                                       left.hash,
                                                       right.hash))
        self.left = left
        self.right = right

    def get_units(self):
        return self.left.get_units()+self.right.get_units()


class InequalityExpression(Expression):

    def __init__(self, left, right, mark):
        assert isinstance(left, Expression)
        assert isinstance(right, Expression)
        domain = BooleanDomain()
        super(InequalityExpression, self).__init__(domain, mark,
                                                   hash=(self.__class__,
                                                         left.hash,
                                                         right.hash))
        self.left = left
        self.right = right

    def get_units(self):
        return self.left.get_units()+self.right.get_units()


class TotalEqualityExpression(Expression):

    def __init__(self, left, right, mark):
        assert isinstance(left, Expression)
        assert isinstance(right, Expression)
        domain = BooleanDomain()
        super(TotalEqualityExpression, self).__init__(domain, mark,
                                hash=(self.__class__, left.hash, right.hash))
        self.left = left
        self.right = right

    def get_units(self):
        return self.left.get_units()+self.right.get_units()


class TotalInequalityExpression(Expression):

    def __init__(self, left, right, mark):
        assert isinstance(left, Expression)
        assert isinstance(right, Expression)
        domain = BooleanDomain()
        super(TotalInequalityExpression, self).__init__(domain, mark,
                                hash=(self.__class__, left.hash, right.hash))
        self.left = left
        self.right = right

    def get_units(self):
        return self.left.get_units()+self.right.get_units()


class ConjunctionExpression(Expression):

    def __init__(self, terms, mark):
        assert isinstance(terms, listof(Expression))
        domain = BooleanDomain()
        hash = (self.__class__, tuple(term.hash for term in terms))
        super(ConjunctionExpression, self).__init__(domain, mark, hash=hash)
        self.terms = terms

    def get_units(self):
        units = []
        for term in self.terms:
            units.extend(term.get_units())
        return units


class DisjunctionExpression(Expression):

    def __init__(self, terms, mark):
        assert isinstance(terms, listof(Expression))
        domain = BooleanDomain()
        hash = (self.__class__, tuple(term.hash for term in terms))
        super(DisjunctionExpression, self).__init__(domain, mark, hash=hash)
        self.terms = terms

    def get_units(self):
        units = []
        for term in self.terms:
            units.extend(term.get_units())
        return units


class NegationExpression(Expression):

    def __init__(self, term, mark):
        assert isinstance(term, Expression)
        domain = BooleanDomain()
        hash = (self.__class__, term.hash)
        super(NegationExpression, self).__init__(domain, mark, hash=hash)
        self.term = term

    def get_units(self):
        return self.term.get_units()


class TupleExpression(Expression):

    def __init__(self, space, mark):
        assert space.table is not None
        assert space.table.primary_key is not None
        super(TupleExpression, self).__init__(BooleanDomain(), mark,
                                              hash=(self.__class__,
                                                    space.hash))
        self.space = space
        columns = [space.table.columns[name]
                   for name in space.table.primary_key.origin_column_names]
        self.units = [ColumnUnit(column, space, mark) for column in columns]

    def get_units(self):
        return self.units


class CastExpression(Expression):

    def __init__(self, code, domain, mark):
        super(CastExpression, self).__init__(domain, mark,
                                             hash=(self.__class__,
                                                   code.hash, domain))
        self.code = code

    def get_units(self):
        return self.code.get_units()


class ElementExpression(Expression):

    def __init__(self, code):
        assert isinstance(code, Code)
        super(ElementExpression, self).__init__(code.domain, code.mark)
        self.code = code

    def get_units(self):
        return self.code.get_units()


class FunctionExpression(Expression):

    def __init__(self, domain, mark, **arguments):
        arguments_hash = []
        for key in sorted(arguments):
            value = arguments[key]
            if isinstance(value, list):
                items = []
                for item in value:
                    if isinstance(item, Code):
                        items.append(item.hash)
                    else:
                        items.append(item)
                value = tuple(items)
            else:
                if isinstance(value, Code):
                    value = value.hash
            arguments_hash.append((key, value))
        hash = (self.__class__, tuple(arguments_hash))
        super(FunctionExpression, self).__init__(domain, mark, hash=hash)
        self.arguments = arguments
        for key in arguments:
            setattr(self, key, arguments[key])

    def get_units(self):
        units = []
        for key in sorted(self.arguments):
            value = self.arguments[key]
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, Expression):
                        units.extend(item.get_units())
            elif isinstance(value, Expression):
                units.extend(value.get_units())
        return units


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


