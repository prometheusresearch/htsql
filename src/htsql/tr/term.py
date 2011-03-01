#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.term`
====================

This module declares term nodes.
"""


from ..util import (listof, dictof, oneof, tupleof, maybe,
                    Clonable, Printable, Comparable)
from ..domain import BooleanDomain
from .code import Expression, Space, Code, Unit, QueryExpr


class Joint(Comparable, Clonable, Printable):

    def __init__(self, lop, rop, is_total=False):
        assert isinstance(lop, Code)
        assert isinstance(rop, Code)
        assert isinstance(is_total, bool)
        super(Joint, self).__init__(equality_vector=(lop, rop, is_total))
        self.lop = lop
        self.rop = rop
        self.is_total = is_total

    def __iter__(self):
        return iter([self.lop, self.rop])


class PreTerm(Clonable, Printable):
    """
    Represents a term node.

    A term represents a relational algebraic expression.  :class:`PreTerm`
    is an abstract class, each its subclass represents a specific relational
    operation.

    The term tree is an intermediate stage of the HTSQL translator. A term
    tree is translated from the expression graph by the *compiling* process.
    It is then translated to the frame tree by the *assembling* process.

    The following adapters are associated with the compiling process and
    generate new term nodes::

        Compile: (Space, CompilingState) -> Term
        Inject: (Unit, Term, CompilingState) -> Term

    See :class:`htsql.tr.compile.Compile` and
    :class:`htsql.tr.compile.Inject` for more detail.

    The following adapter implements the assembling process::

        Assemble: (Term, AssemblingState) -> Frame

    See :class:`htsql.tr.assemble.Assemble` for more detail.

    Arguments:

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

    def __init__(self, expression):
        assert isinstance(expression, Expression)
        self.expression = expression
        self.binding = expression.binding
        self.syntax = expression.syntax
        self.mark = expression.mark

    def __str__(self):
        return str(self.expression)


class Term(PreTerm):
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

    Each term node has a unique (in the context of the term tree) identifier,
    called the term *tag*.  Tags are used to refer to term objects indirectly.

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

    `kids` (a list of zero, one or two :class:`Term` objects)
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

    `offsprings` (a dictionary `tag -> tag`)
        Maps a descendant term to the immediate child whose subtree
        contains the term.
    """

    is_nullary = False
    is_unary = False
    is_binary = False

    def __init__(self, tag, kids, space, baseline, routes):
        assert isinstance(tag, int)
        assert isinstance(kids, listof(Term))
        assert isinstance(space, Space)
        assert isinstance(baseline, Space)
        assert space.concludes(baseline)
        assert baseline.is_inflated
        assert isinstance(routes, dictof(Unit, int))
        # The inflation of the term space.
        backbone = space.inflate()
        # For each descendant term, determine the immediate child whose
        # subtree contain the descendant.
        offsprings = {}
        for kid in kids:
            offsprings[kid.tag] = kid.tag
            for offspring in kid.offsprings:
                offsprings[offspring] = kid.tag
        super(Term, self).__init__(space)
        self.tag = tag
        self.kids = kids
        self.space = space
        self.backbone = backbone
        self.baseline = baseline
        self.routes = routes
        self.offsprings = offsprings


class NullaryTerm(Term):
    """
    Represents a terminal relational algebraic expression.
    """

    is_nullary = True

    def __init__(self, tag, space, baseline, routes):
        super(NullaryTerm, self).__init__(tag, [], space, baseline, routes)


class UnaryTerm(Term):
    """
    Represents a unary relational algebraic expression.

    `kid` (:class:`Term`)
        The operand of the expression.
    """

    is_unary = True

    def __init__(self, tag, kid, space, baseline, routes):
        super(UnaryTerm, self).__init__(tag, [kid], space, baseline, routes)
        self.kid = kid


class BinaryTerm(Term):
    """
    Represents a binary relational algebraic expression.

    `lkid` (:class:`Term`)
        The left operand of the expression.

    `rkid` (:class:`Term`)
        The right operand of the expression.
    """

    is_binary = True

    def __init__(self, tag, lkid, rkid, space, baseline, routes):
        super(BinaryTerm, self).__init__(tag, [lkid, rkid],
                                         space, baseline, routes)
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

    def __init__(self, tag, space, baseline, routes):
        # The space itself is not required to be a scalar, but it
        # should not contain any other axes.
        assert space.family.is_scalar
        super(ScalarTerm, self).__init__(tag, space, baseline, routes)

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

    def __init__(self, tag, space, baseline, routes):
        # We assume that the table of the term is the prominent table
        # of the term space.
        assert space.family.is_table
        assert space == baseline
        super(TableTerm, self).__init__(tag, space, baseline, routes)
        self.table = space.family.table

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

    `kid` (:class:`Term`)
        The operand of the filter expression.

    `filter` (:class:`htsql.tr.code.Code`)
        The conditional expression.
    """

    def __init__(self, tag, kid, filter, space, baseline, routes):
        assert (isinstance(filter, Code) and
                isinstance(filter.domain, BooleanDomain))
        super(FilterTerm, self).__init__(tag, kid, space, baseline, routes)
        self.filter = filter

    def __str__(self):
        # Display:
        #   (<kid> ? <filter>)
        return "(%s ? %s)" % (self.kid, self.filter)


class JoinTerm(BinaryTerm):
    """
    Represents a join term.

    A join term takes two operands and produces a set of pairs satisfying
    the given join conditions.

    Two types of joins are supported by a join term.  When the join is
    *inner*, given the operands `A` and `B`, the term produces a set of
    pairs `(a, b)`, where `a` is from `A`, `b` is from `B` and the pair
    satisfies the given tie conditions.

    A *left outer join* produces the same rows as the inner join, but
    also includes rows of the form `(a, NULL)` for each `a` from `A`
    such that there are no rows `b` from `B` such that `(a, b)` satisfies
    the given conditions.  Similarly, a *right outer join* includes rows
    of the form `(NULL, b)` for each `b` from `B` such that there are no
    corresponding rows `a` from `A`.

    A join term generates the following SQL clause::

        (SELECT ... FROM <lkid> (INNER | LEFT OUTER) JOIN <rkid> ON (<joints>))

    `lkid` (:class:`Term`)
        The left operand of the join.

    `rkid` (:class:`Term`)
        The right operand of the join.

    `joints` (a list of pairs of :class:`htsql.tr.code.Code`)
        A list of pairs `(lop, rop)` that establish join conditions
        of the form `lop = rop`.

    `is_left` (Boolean)
        Indicates that the join is left outer.

    `is_right` (Boolean)
        Indicates that the join is right outer.
    """

    def __init__(self, tag, lkid, rkid, joints,
                 is_left, is_right, space, baseline, routes):
        assert isinstance(joints, listof(Joint))
        assert isinstance(is_left, bool) and isinstance(is_right, bool)
        # Note: currently we never generate right outer joins.
        assert is_right is False
        super(JoinTerm, self).__init__(tag, lkid, rkid,
                                       space, baseline, routes)
        self.joints = joints
        self.is_left = is_left
        self.is_right = is_right

    def __str__(self):
        # Display, for inner join:
        #   (<lkid> ++ <rkid> | <lop>=<rop>, ...)
        # or, for left outer join:
        #   (<lkid> +* <rkid> | <lop>=<rop>, ...)
        conditions = ", ".join("%s=%s" % joint for joint in self.joints)
        if conditions:
            conditions = " | %s" % conditions
        symbol = ""
        for is_outer in [self.is_right, self.is_left]:
            if is_outer:
                symbol += "*"
            else:
                symbol += "+"
        return "(%s %s %s%s)" % (self.lkid, symbol, self.rkid, conditions)


class EmbeddingTerm(BinaryTerm):
    """
    Represents an embedding term.

    An embedding term implants a correlated term into a term tree.

    An embedding term has two children: the left child is a regular term
    and the right child is a correlation term.

    The joint condition of the correlation term connects it to the left
    child.  That is, the left child serves as the *link* term for the right
    child.

    An embedding term generates the following SQL clause::

        (SELECT ... (SELECT ... FROM <rkid>) ... FROM <lkid>)

    `lkid` (:class:`Term`)
        The main term.

    `rkid` (:class:`CorrelationTerm`)
        The correlated term.

    """

    def __init__(self, tag, lkid, rkid, space, baseline, routes):
        # Verify that the right child is a correlation term and the left
        # child is its link term.
        assert isinstance(rkid, CorrelationTerm) and rkid.link is lkid
        super(EmbeddingTerm, self).__init__(tag, lkid, rkid,
                                            space, baseline, routes)

    def __str__(self):
        # Display:
        #   (<lkid> // <rkid>)
        return "(%s // %s)" % (self.lkid, self.rkid)


class CorrelationTerm(UnaryTerm):
    """
    Represents a correlation term.

    A correlation term connects the child term with a *link* term using
    the given joint condition.  Note that the link term is not a part
    of the sub-tree under the correlation term.

    A correlation term must always be embedded into the term tree with
    a :class:`EmbeddingTerm` instance.  The left child of the embedding
    term must coincide with the link term.

    `kid` (:class:`Term`)
        The operand of the correlation condition.

    `link` (:class:`Term`)
        The term to link to.

    `joints` (a list of pairs of :class:`htsql.tr.code.Code`)
        A list of pairs `(lop, rop)` that establish join conditions
        of the form `lop = rop`.
    """

    def __init__(self, tag, kid, link, joints, space, baseline, routes):
        assert isinstance(link, Term)
        assert isinstance(joints, listof(Joint))
        super(CorrelationTerm, self).__init__(tag, kid,
                                              space, baseline, routes)
        self.link = link
        self.joints = joints

    def __str__(self):
        # Display:
        #   (<kid> | <lop>=<rop>, ...)
        conditions = ", ".join("%s=%s" % joint for joint in self.joints)
        if conditions:
            conditions = " | %s" % conditions
        return "(%s%s)" % (self.kid, conditions)


class ProjectionTerm(UnaryTerm):
    """
    Represents a projection term.

    Given an operand term and a function on it (called the *kernel*), the
    kernel naturally establishes an equivalence relation on the operand.
    That is, two rows from the operand are equivalent if their images under
    the kernel are equal to each other.  A projection term produces rows
    of the quotient set corresponding to the equivalence relation.

    A projection term generates the following SQL clause::

        (SELECT ... FROM <kid> GROUP BY <kernel>)

    `kid` (:class:`Term`)
        The operand of the projection.

    `kernel` (a list of :class:`htsql.tr.code.Code`)
        The equivalence kernel.
    """

    def __init__(self, tag, kid, kernel, space, baseline, routes):
        assert isinstance(kernel, listof(Code))
        super(ProjectionTerm, self).__init__(tag, kid, space, baseline, routes)
        self.kernel = kernel

    def __str__(self):
        # Display:
        #   (<kid> ^ <code>, <code>, ...)
        if not self.kernel:
            return "(%s ^)" % self.kid
        return "(%s ^ %s)" % (self.kid,
                              ", ".join(str(code) for code in self.kernel))


class OrderTerm(UnaryTerm):
    """
    Represents an order term.

    An order term reorders the rows of its operand and optionally extracts
    a slice of the operand.

    An order term generates the following SQL clause::

        (SELECT ... FROM <kid> ORDER BY <order> LIMIT <limit> OFFSET <offset>)

    `kid` (:class:`Term`)
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

    def __init__(self, tag, kid, order, limit, offset,
                 space, baseline, routes):
        assert isinstance(order, listof(tupleof(Code, int)))
        assert isinstance(limit, maybe(int))
        assert isinstance(offset, maybe(int))
        assert limit is None or limit >= 0
        assert offset is None or offset >= 0
        super(OrderTerm, self).__init__(tag, kid, space, baseline, routes)
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


class PermanentTerm(WrapperTerm):
    """
    Represents a no-op operation.

    A permanent term is never collapsed with the outer term.
    """

    def __str__(self):
        # Display:
        #   (!<kid>!)
        return "(!%s!)" % self.kid


class SegmentTerm(UnaryTerm):
    """
    Represents a segment term.

    A segment term evaluates the given expressions on the rows of the operand.

    A segment term generates the following SQL clause::

        (SELECT <elements> FROM <kid>)

    `kid` (:class:`Term`)
        The operand.

    `elements` (a list of :class:`htsql.tr.code.Code`)
        A list of expressions to produce.
    """

    def __init__(self, tag, kid, elements, space, routes):
        assert isinstance(elements, listof(Code))
        super(SegmentTerm, self).__init__(tag, kid,
                                          space, space.scalar, routes)
        self.elements = elements

    def __str__(self):
        # Display:
        #   <kid> {<element>,...}
        return "%s {%s}" % (self.kid, ",".join(str(element)
                                               for element in self.elements))


class QueryTerm(PreTerm):
    """
    Represents a whole HTSQL query.

    `segment` (:class:`SegmentTerm` or ``None``)
        The query segment.
    """

    def __init__(self, segment, expression):
        assert isinstance(segment, maybe(SegmentTerm))
        assert isinstance(expression, QueryExpr)
        super(QueryTerm, self).__init__(expression)
        self.segment = segment


