#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from .util import Comparable, Printable, Clonable, maybe, listof, tupleof
from .domain import Domain
from .entity import TableEntity, ColumnEntity, Join
from .tr.syntax import Syntax


class Model(Comparable, Clonable, Printable):

    def __init__(self):
        pass


class Node(Model):
    pass


class Arc(Model):

    def __init__(self, origin, target, arity, is_expanding, is_contracting):
        assert isinstance(origin, Node)
        assert isinstance(target, Node)
        assert isinstance(arity, maybe(int))
        assert isinstance(is_expanding, bool)
        assert isinstance(is_contracting, bool)
        self.origin = origin
        self.target = target
        self.arity = arity
        self.is_expanding = is_expanding
        self.is_contracting = is_contracting

    def reverse(self):
        return None


class Label(Clonable, Printable):

    def __init__(self, name, arc, is_public):
        assert isinstance(name, unicode)
        assert isinstance(arc, Arc)
        assert isinstance(is_public, bool)
        assert arc.arity is None or not is_public
        self.name = name
        self.arc = arc
        self.origin = arc.origin
        self.target = arc.target
        self.arity = arc.arity
        self.is_expanding = arc.is_expanding
        self.is_contracting = arc.is_contracting
        self.is_public = is_public

    def __str__(self):
        return "%s%s (%s): %s -> %s" % (self.name.encode('utf-8'),
                                        "(%s)" % ",".join(["_"]*self.arity)
                                        if self.arity is not None else "",
                                        self.arc, self.origin, self.target)


class HomeNode(Node):

    def __basis__(self):
        return ()

    def __str__(self):
        return "()"


class TableNode(Node):

    def __init__(self, table):
        assert isinstance(table, TableEntity)
        self.table = table

    def __basis__(self):
        return (self.table,)

    def __str__(self):
        return str(self.table)


class DomainNode(Node):

    def __init__(self, domain):
        assert isinstance(domain, Domain)
        self.domain = domain

    def __basis__(self):
        return (self.domain,)

    def __str__(self):
        return str(self.domain)


class UnknownNode(Node):

    def __basis__(self):
        return ()

    def __str__(self):
        return "?"


class InvalidNode(Node):

    def __basis__(self):
        return ()

    def __str__(self):
        return "!"


class TableArc(Arc):

    def __init__(self, table):
        assert isinstance(table, TableEntity)
        super(TableArc, self).__init__(
                origin=HomeNode(),
                target=TableNode(table),
                arity=None,
                is_expanding=False,
                is_contracting=False)
        self.table = table

    def __basis__(self):
        return (self.table,)

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
                arity=None,
                is_expanding=all(join.is_expanding for join in joins),
                is_contracting=all(join.is_contracting for join in joins))
        self.table = table
        self.joins = joins
        self.is_direct = all(join.is_direct for join in joins)
        self.is_reverse = all(join.is_reverse for join in joins)

    def __basis__(self):
        return (self.table, tuple(self.joins))

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
                arity=None,
                is_expanding=(not column.is_nullable),
                is_contracting=True)
        self.table = table
        self.column = column
        self.link = link

    def __basis__(self):
        return (self.table, self.column)

    def __str__(self):
        return str(self.column)


class SyntaxArc(Arc):

    def __init__(self, origin, parameters, syntax):
        assert isinstance(parameters, maybe(listof(tupleof(unicode, bool))))
        assert isinstance(syntax, Syntax)
        super(SyntaxArc, self).__init__(
                origin=origin,
                target=UnknownNode(),
                arity=(len(parameters) if parameters is not None else None),
                is_expanding=False,
                is_contracting=False)
        self.parameters = parameters
        self.syntax = syntax

    def __basis__(self):
        return (self.origin, tuple(self.parameters)
                             if self.parameters is not None else None,
                self.syntax)

    def __str__(self):
        return str(self.syntax)


class InvalidArc(Arc):

    def __init__(self, origin, arity):
        assert isinstance(origin, Node)
        super(InvalidArc, self).__init__(
                origin=origin,
                target=InvalidNode(),
                arity=arity,
                is_expanding=False,
                is_contracting=False)

    def __basis__(self):
        return ()

    def __str__(self):
        return "!"


class AmbiguousArc(InvalidArc):

    def __init__(self, arity, alternatives):
        assert isinstance(alternatives, listof(Arc)) and len(alternatives) > 0
        origin = alternatives[0].origin
        assert all(alternative.origin == origin
                   for alternative in alternatives)
        super(AmbiguousArc, self).__init__(
                origin=origin,
                arity=arity)
        self.alternatives = alternatives

    def __basis__(self):
        return (self.origin, tuple(self.alternatives))

    def __str__(self):
        return "?(%s)" % ", ".join(str(alternative)
                                   for alternative in self.alternatives)


