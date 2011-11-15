#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from .util import Comparable, Printable, Clonable, maybe, listof
from .domain import Domain
from .entity import TableEntity, ColumnEntity, Join
from .tr.syntax import Syntax


class Model(Comparable, Clonable, Printable):
    pass


class Node(Model):
    pass


class Arc(Model):

    def __init__(self, origin, target, is_expanding, is_contracting,
                 equality_vector):
        assert isinstance(origin, Node)
        assert isinstance(target, Node)
        assert isinstance(is_expanding, bool)
        assert isinstance(is_contracting, bool)
        super(Arc, self).__init__(equality_vector)
        self.origin = origin
        self.target = target
        self.is_expanding = is_expanding
        self.is_contracting = is_contracting

    def reverse(self):
        return None


class Label(Clonable, Printable):

    def __init__(self, name, arc, is_public):
        assert isinstance(name, unicode)
        assert isinstance(arc, Arc)
        assert isinstance(is_public, bool)
        self.name = name
        self.arc = arc
        self.origin = arc.origin
        self.target = arc.target
        self.is_expanding = arc.is_expanding
        self.is_contracting = arc.is_contracting
        self.is_public = is_public

    def __str__(self):
        return "%s (%s): %s -> %s" % (self.name.encode('utf-8'), self.arc,
                                      self.origin, self.target)


class HomeNode(Node):

    def __init__(self):
        super(HomeNode, self).__init__(equality_vector=())

    def __str__(self):
        return "()"


class TableNode(Node):

    def __init__(self, table):
        assert isinstance(table, TableEntity)
        super(TableNode, self).__init__(equality_vector=(table,))
        self.table = table

    def __str__(self):
        return str(self.table)


class DomainNode(Node):

    def __init__(self, domain):
        assert isinstance(domain, Domain)
        super(DomainNode, self).__init__(equality_vector=(domain,))
        self.domain = domain

    def __str__(self):
        return str(self.domain)


class UnknownNode(Node):

    def __init__(self):
        super(UnknownNode, self).__init__(equality_vector=())

    def __str__(self):
        return "?"


class InvalidNode(Node):

    def __init__(self):
        super(InvalidNode, self).__init__(equality_vector=())

    def __str__(self):
        return "!"


class TableArc(Arc):

    def __init__(self, table):
        assert isinstance(table, TableEntity)
        super(TableArc, self).__init__(
                origin=HomeNode(),
                target=TableNode(table),
                is_expanding=False,
                is_contracting=False,
                equality_vector=(table,))
        self.table = table

    def __str__(self):
        return str(self.table)


class ChainArc(Arc):

    def __init__(self, table, joins):
        assert isinstance(table, TableEntity)
        assert isinstance(joins, listof(Join)) and len(joins) > 0
        assert table == joins[0].origin
        super(ChainArc, self).__init__(
                origin=TableNode(joins[0].origin),
                target=TableNode(joins[-1].target),
                is_expanding=all(join.is_expanding for join in joins),
                is_contracting=all(join.is_contracting for join in joins),
                equality_vector=(table, tuple(joins),))
        self.table = table
        self.joins = joins
        self.is_direct = all(join.is_direct for join in joins)
        self.is_reverse = all(join.is_reverse for join in joins)

    def reverse(self):
        return ChainArc(self.target.table,
                        [join.reverse() for join in reversed(self.joins)])

    def __str__(self):
        return " => ".join("(%s)" % join for join in self.joins)


class ColumnArc(Arc):

    def __init__(self, table, column, link=None):
        assert isinstance(table, TableEntity)
        assert isinstance(column, ColumnEntity) and column.table is table
        assert isinstance(link, maybe(Arc))
        if link is not None:
            assert link.origin == TableNode(table)
        super(ColumnArc, self).__init__(
                origin=TableNode(table),
                target=DomainNode(column.domain),
                is_expanding=(not column.is_nullable),
                is_contracting=True,
                equality_vector=(table, column))
        self.table = table
        self.column = column
        self.link = link

    def __str__(self):
        return str(self.column)


class SyntaxArc(Arc):

    def __init__(self, origin, syntax):
        assert isinstance(syntax, Syntax)
        super(SyntaxArc, self).__init__(
                origin=origin,
                target=UnknownNode(),
                is_expanding=False,
                is_contracting=False,
                equality_vector=(origin, syntax))
        self.syntax = syntax

    def __str__(self):
        return str(self.syntax)


class InvalidArc(Arc):

    def __init__(self, origin, equality_vector):
        assert isinstance(origin, Node)
        super(InvalidArc, self).__init__(
                origin=origin,
                target=InvalidNode(),
                is_expanding=False,
                is_contracting=False,
                equality_vector=equality_vector)

    def __str__(self):
        return "!"


class AmbiguousArc(InvalidArc):

    def __init__(self, alternatives):
        assert isinstance(alternatives, listof(Arc)) and len(alternatives) > 0
        origin = alternatives[0].origin
        assert all(alternative.origin == origin
                   for alternative in alternatives)
        super(AmbiguousArc, self).__init__(
                origin=origin,
                equality_vector=(origin, tuple(alternatives)))
        self.alternatives = alternatives

    def __str__(self):
        return "?(%s)" % ", ".join(str(alternative)
                                   for alternative in self.alternatives)


