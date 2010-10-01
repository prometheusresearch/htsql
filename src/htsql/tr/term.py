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
    """
    Represents a term node.

    A term represents a relational algebraic expression.  :class:`Term`
    is an abstract class, each its subclass represents a specific relational
    operation.

    The term tree is an intermediate stage of the HTSQL translator. A term
    tree is translated from the expression graph by the *assembling* process.
    It is then translated to the sketch tree by the *outline* process.

    The following adapters are associated with the assembling process and
    generate new term nodes::

        Assemble: (Space, AssemblingState) -> Term
        Inject: (Unit, Term, AssemblingState) -> Term

    See :class:`htsql.tr.assemble.Assemble` and
    :class:`htsql.tr.assemble.Inject` for more detail.

    The following adapter implements the outline process::

        Outline: (Term, OutliningState) -> Sketch

    See :class:`htsql.tr.outline.Outline` for more detail.

    Each term node has a unique (in the context of the term tree) identifier,
    called the term *tag*.  Tags are used to refer to term objects indirectly.

    Arguments:

    `tag` (an integer)
        A unique identifier of the node.

    `expression` (:class:`htsql.tr.code.Expression`)
        The expression node which gave rise to the term node; used only for
        presentation or error reporting.

    Other attributes:

    `binding` (:class:`htsql.tr.binding.Binding`)
        The binding node which gave rise to the term node; for debugging.

    `syntax` (:class:`htsql.tr.syntax.Syntax`)
        The syntax node which gave rise to the term node; for debugging.

    `mark` (:class:`htsql.mark.Mark`)
        The location of the node in the original query; for error reporting.
    """

    def __init__(self, tag, expression):
        assert isinstance(tag, int)
        assert isinstance(expression, Expression)
        self.tag = tag
        self.expression = expression
        self.binding = expression.binding
        self.syntax = expression.syntax
        self.mark = expression.mark

    def __str__(self):
        return str(self.expression)


class RoutingTerm(Term):
    """
    Represents a relational algebraic expression.

    There are three classes of terms: nullary, unary and binary.
    Nullary terms represents terminal expressions (for example,
    :class:`TableTerm`), unary terms represent relational expressions
    with a single operand (for example, :class:`FilterTerm`), and binary
    terms represent relational expressions with two arguments (for example,
    :class:`JoinTerm`).

    Each term represents some space, called *the term space*.  It means
    that, *as a part of some relational expression*, the term will produce
    the rows of the space.  Note that taken alone, the term does not
    necessarily generates the rows of the space: some of the operations
    that comprise the space may be missing from the term.  Thus the term
    space represents a promise: once the term is tied with some other
    appropriate term, it will generate the rows of the space.

    Each term maintains a table of units it is capable to produce.
    For each unit, the table contains a reference to a node directly
    responsible for evaluating the unit.

    Class attributes:

    `is_nullary` (Boolean)
        Indicates that the term represents a nullary expression.

    `is_unary` (Boolean)
        Indicates that the term represents a unary expression.

    `is_binary` (Boolean)
        Indicates that the term represents a binary expression.

    Arguments:

    `tag` (an integer)
        A unique identifier of the node.

    `kids` (a list of zero, one or two :class:`RoutingTerm` objects)
        The operands of the relational expression.

    `space` (:class:`htsql.tr.code.Space`)
        The space represented by the term.

    `routes` (a mapping from :class:`htsql.tr.code.Unit` to term tag)
        A mapping from unit objects to term tags that specifies the units
        which the term is capable to produce.

        A key of the mapping is either a :class:`htsql.tr.code.Unit`
        or a :class:`htsql.tr.code.Space` node.  A value of the mapping
        is a term tag, either of the term itself or of one of its
        descendants.

        The presence of a unit object in the `routes` table indicates
        that the term is able to evaluate the unit.  The respective
        term tag indicates the term directly responsible for evaluating
        the unit.

        A space node being a key in the `routes` table indicates that
        any column of the space could be produced by the term.

    Other attributes:

    `backbone` (:class:`htsql.tr.code.Space`)
        The inflation of the term space.

    `baseline` (:class:`htsql.tr.code.Space`)
        The leftmost axis of the term space that the term is capable
        to produce.
    """

    is_nullary = False
    is_unary = False
    is_binary = False

    def __init__(self, tag, kids, space, routes):
        assert isinstance(kids, listof(RoutingTerm))
        assert isinstance(space, Space)
        assert isinstance(routes, dictof(oneof(Space, Unit), int))
        # The inflation of the term space.
        backbone = space.inflate()
        # The lestmost axis exported by the term.
        baseline = backbone
        while baseline.base in routes:
            baseline = baseline.base
        # Verify the validity of the `routes` table.  Note that we only do
        # a few simple checks.  Here is the full list of assumptions that
        # must be maintained:
        # - The term space must be present in `routes`;
        # - for any space in `routes`, its inflation must also be in `routes`;
        # - if `axis` is an inflated prefix of the term space and its base
        #   is in `routes`, `axis` must also be in `routes`.
        assert space in routes
        assert backbone in routes
        axis = baseline.base
        while axis is not None:
            assert axis not in routes
            axis = axis.base
        super(RoutingTerm, self).__init__(tag, space)
        self.kids = kids
        self.space = space
        self.routes = routes
        self.backbone = backbone
        self.baseline = baseline


class NullaryTerm(RoutingTerm):
    """
    Represents a terminal relational algebraic expression.
    """

    is_nullary = True

    def __init__(self, tag, space, routes):
        super(NullaryTerm, self).__init__(tag, [], space, routes)


class UnaryTerm(RoutingTerm):
    """
    Represents a unary relational algebraic expression.

    `kid` (:class:`RoutingTerm`)
        The operand of the expression.
    """

    is_unary = True

    def __init__(self, tag, kid, space, routes):
        super(UnaryTerm, self).__init__(tag, [kid], space, routes)
        self.kid = kid


class BinaryTerm(RoutingTerm):
    """
    Represents a binary relational algebraic expression.

    `lkid` (:class:`RoutingTerm`)
        The left operand of the expression.

    `rkid` (:class:`RoutingTerm`)
        The right operand of the expression.
    """

    is_binary = True

    def __init__(self, tag, lkid, rkid, space, routes):
        super(BinaryTerm, self).__init__(tag, [lkid, rkid], space, routes)
        self.lkid = lkid
        self.rkid = rkid


class ScalarTerm(NullaryTerm):
    """
    Represents a scalar term.

    A scalar term is a terminal relational expression that produces
    exactly one row.

    A scalar term generates the following SQL clause::

        (SELECT ... FROM DUAL)
    """

    def __init__(self, tag, space, routes):
        # The space itself is not required to be a scalar, but it
        # should not contain any other axes.
        assert space.table is None
        super(ScalarTerm, self).__init__(tag, space, routes)

    def __str__(self):
        return "I"


class TableTerm(NullaryTerm):
    """
    Represents a table term.

    A table term is a terminal relational expression that produces
    all the rows of a table.

    A table term generates the following SQL clause::

        (SELECT ... FROM <table>)
    """

    def __init__(self, tag, space, routes):
        # We assume that the table of the term is the prominent table
        # of the term space.
        assert space.table is not None
        super(TableTerm, self).__init__(tag, space, routes)
        self.table = space.table

    def __str__(self):
        # Display:
        #   <schema>.<table>
        return str(self.table)


class FilterTerm(UnaryTerm):
    """
    Represents a filter term.

    A filter term is a unary relational expression that produces all the rows
    of its operand that satisfy the given predicate expression.

    A filter term generates the following SQL clause::

        (SELECT ... FROM <kid> WHERE <filter>)

    `kid` (:class:`RoutingTerm`)
        The operand of the filter expression.

    `filter` (:class:`htsql.tr.code.Code`)
        The conditional expression.
    """

    def __init__(self, tag, kid, filter, space, routes):
        assert (isinstance(filter, Code) and
                isinstance(filter.domain, BooleanDomain))
        super(FilterTerm, self).__init__(tag, kid, space, routes)
        self.filter = filter

    def __str__(self):
        # Display:
        #   (<kid> ? <filter>)
        return "(%s ? %s)" % (self.kid, self.filter)


class JoinTerm(BinaryTerm):
    """
    Represents a join term.

    A join term takes two operands and produces a set of pairs satisfying
    the given ties.

    Two types of joins are supported by a join term.  When the join is
    *inner*, given the operands `A` and `B`, the term produces a set of
    pairs `(a, b)`, where `a` is from `A`, `b` is from `B` and the pair
    satisfies the given tie conditions.

    A *left outer joins* produces the same rows as the inner join, but
    also includes rows of the form `(a, NULL)` for each `a` from `A`
    such that there are no rows `b` from `B` such that `(a, b)` satisfies
    the given ties.

    A join term generates the following SQL clause::

        (SELECT ... FROM <lkid> (INNER | LEFT OUTER) JOIN <rkid> ON (<ties>))

    `lkid` (:class:`RoutingTerm`)
        The left operand of the join.

    `rkid` (:class:`RoutingTerm`)
        The right operand of the join.

    `ties` (a list of :class:`Tie`)
        The ties that establish the join condition.

    `is_inner` (Boolean)
        Indicates whether the join is inner or left outer.
    """

    def __init__(self, tag, lkid, rkid, ties, is_inner, space, routes):
        assert isinstance(ties, listof(Tie))
        assert isinstance(is_inner, bool)
        super(JoinTerm, self).__init__(tag, lkid, rkid, space, routes)
        self.ties = ties
        self.is_inner = is_inner

    def __str__(self):
        # Display, for inner join:
        #   (<lkid> ++ <rkid> | <tie>, <tie>, ...)
        # or, for left outer join:
        #   (<lkid> +* <rkid> | <tie>, <tie>, ...)
        conditions = ", ".join(str(tie) for tie in self.ties)
        if conditions:
            conditions = " | %s" % conditions
        if self.is_inner:
            op = "++"
        else:
            op = "+*"
        return "(%s %s %s%s)" % (self.lkid, op, self.rkid, conditions)


class CorrelationTerm(BinaryTerm):
    """
    Represents a correlation term.

    A correlation term has a semantics similar to a left outer join term,
    but is serialized to SQL as a correlated sub-SELECT clause.

    `lkid` (:class:`RoutingTerm`)
        The main term.

    `rkid` (:class:`RoutingTerm`)
        The correlated term.

    `ties` (a list of :class:`Tie`)
        The ties that establish the join condition.
    """

    def __init__(self, tag, lkid, rkid, ties, space, routes):
        assert isinstance(ties, listof(Tie))
        super(CorrelationTerm, self).__init__(tag, lkid, rkid, space, routes)
        self.ties = ties

    def __str__(self):
        # Display:
        #   (<lkid> // <rkid> | <tie>, <tie>, ...)
        conditions = ", ".join(str(tie) for tie in self.ties)
        if conditions:
            conditions = " | %s" % conditions
        return "(%s // %s%s)" % (self.lkid, self.rkid, conditions)


class ProjectionTerm(UnaryTerm):
    """
    Represents a projection term.

    Given an operand term and tie conditions, the ties naturally establish
    an equivalence relation on the operand.  A projection term produces
    rows of the quotient set corresponding to the equivalence relation.

    A projection term generates the following SQL clause::

        (SELECT ... FROM <kid> GROUP BY <ties>)

    `kid` (:class:`RoutingTerm`)
        The operand of the projection.

    `ties` (a list of :class:`Tie`)
        The ties that establish the quotient space.
    """

    def __init__(self, tag, kid, ties, space, routes):
        assert isinstance(ties, listof(Tie))
        super(ProjectionTerm, self).__init__(tag, kid, space, routes)
        self.ties = ties

    def __str__(self):
        # Display:
        #   (<kid> ^ <tie>, <tie>, ...)
        if not self.ties:
            return "(%s ^)" % self.kid
        return "(%s ^ %s)" % (self.kid,
                              ", ".join(str(tie) for tie in self.ties))


class OrderTerm(UnaryTerm):
    """
    Represents an order term.

    An order term reorders the rows of its operand and optionally extracts
    a slice of the operand.

    An order term generates the following SQL clause::

        (SELECT ... FROM <kid> ORDER BY <order> LIMIT <limit> OFFSET <offset>)

    `kid` (:class:`RoutingTerm`)
        The operand.

    `order` (a list of pairs `(code, direction)`)
        Expressions to sort the rows by.

        Here `code` is a :class:`htsql.tr.code.Code` instance, `direction`
        is either ``+1`` (indicates ascending order) or ``-1`` (indicates
        descending order).

    `limit` (a non-negative integer or ``None``)
        If set, the first `limit` rows of the operand are extracted and
        the remaining rows are discared.

    `offset` (a non-negative integer or ``None``)
        If set, indicates that when extracting rows from the operand,
        the first `offset` rows should be skipped.
    """

    def __init__(self, tag, kid, order, limit, offset, space, routes):
        assert isinstance(order, listof(tupleof(Code, int)))
        assert isinstance(limit, maybe(int))
        assert isinstance(offset, maybe(int))
        assert limit is None or limit >= 0
        assert offset is None or offset >= 0
        super(OrderTerm, self).__init__(tag, kid, space, routes)
        self.order = order
        self.limit = limit
        self.offset = offset

    def __str__(self):
        # Display:
        #   <kid> [<code>,...;<offset>:<limit>+<offset>]
        # FIXME: duplicated from `OrderedSpace.__str__`.
        indicators = []
        if self.order:
            indicator = ",".join(str(code) for code, dir in self.order)
            indicators.append(indicator)
        if self.limit is not None and self.offset is not None:
            indicator = "%s:%s+%s" % (self.offset, self.offset, self.limit)
            indicators.append(indicator)
        elif self.limit is not None:
            indicator = ":%s" % self.limit
            indicators.append(indicator)
        elif self.offset is not None:
            indicator = "%s:" % self.offset
            indicators.append(indicator)
        indicators = ";".join(indicators)
        return "%s [%s]" % (self.kid, indicators)


class WrapperTerm(UnaryTerm):
    """
    Represents a no-op operation.

    A wrapper term represents exactly the same rows as its operand.  It is
    used by the assembler to wrap nullary terms when SQL syntax requires
    a non-terminal expression.
    """

    def __str__(self):
        # Display:
        #   (<kid>)
        return "(%s)" % self.kid


class SegmentTerm(UnaryTerm):
    """
    Represents a segment term.

    A segment term evaluates the given expressions on the rows of the operand.

    A segment term generates the following SQL clause::

        (SELECT <elements> FROM <kid>)

    `kid` (:class:`RoutingTerm`)
        The operand.

    `elements` (a list of :class:`htsql.tr.code.Code`)
        A list of expressions to produce.
    """

    def __init__(self, tag, kid, elements, space, routes):
        assert isinstance(elements, listof(Code))
        super(SegmentTerm, self).__init__(tag, kid, space, routes)
        self.elements = elements

    def __str__(self):
        # Display:
        #   <kid> {<element>,...}
        return "%s {%s}" % (self.kid, ",".join(str(element)
                                               for element in self.elements))


class QueryTerm(Term):
    """
    Represents a whole HTSQL query.

    `segment` (:class:`SegmentTerm` or ``None``)
        The query segment.
    """

    def __init__(self, tag, segment, expression):
        assert isinstance(segment, maybe(SegmentTerm))
        assert isinstance(expression, QueryExpression)
        super(QueryTerm, self).__init__(tag, expression)
        self.segment = segment


class Tie(Node):
    """
    Represents a connection between two axes.

    An axis space could be naturally connected with:

    - an identical axis space;
    - or its base space.

    These two types of connections are called *parallel* and *series*
    ties respectively.  Typically, a parallel tie is implemented using
    a primary key constraint while a series tie is implemented using
    a foreign key constraint, but, in general, it depends on the type
    of the axis.

    :class:`Tie` is an abstract case class with exactly two subclasses:
    :class:`ParallelTie` and :class:`SeriesTie`.

    Class attributes:

    `is_parallel` (Boolean)
        Denotes a parallel tie.

    `is_series` (Boolean)
        Denotes a series tie.
    """

    is_parallel = False
    is_series = False


class ParallelTie(Tie):
    """
    Represents a parallel tie.

    A parallel tie is a connection of an axis with itself.

    `space` (:class:`htsql.tr.code.Space`)
        An axis space.
    """

    is_parallel = True

    def __init__(self, space):
        assert isinstance(space, Space) and space.is_axis
        # Technically, non-inflated axis spaces could be permitted, but
        # since the assembler only generates ties for inflated spaces,
        # we add a respective check here.
        assert space.is_inflated
        self.space = space

    def __str__(self):
        # Display:
        #   ==<space>
        return "==%s" % self.space


class SeriesTie(Tie):
    """
    Represents a series tie.

    A series tie is a connection between an axis and its base.  Note that
    a series tie is assimetric, that is, depending on the order of the
    operands, it could connect either an axis to its base, or the base
    to the axis.

    `space` (:class:`htsql.tr.code.Space`)
        An axis space.

    `is_backward` (Boolean)
        If set, indicates that the tie connects the axis base to the axis
        (i.e. the operands are switched).
    """

    is_series = True

    def __init__(self, space, is_backward=False):
        assert isinstance(space, Space) and space.is_axis
        # Technically, non-inflated axis spaces could be permitted, but
        # since the assembler only generates ties for inflated spaces,
        # we add a respective check here.
        assert space.is_inflated
        assert isinstance(is_backward, bool)
        self.space = space
        self.is_backward = is_backward

    def __str__(self):
        # Depending on the direction, display
        #   =><space>  or  <=<space>
        if self.is_backward:
            return "<=%s" % self.space
        else:
            return "=>%s" % self.space


