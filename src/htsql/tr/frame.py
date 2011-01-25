#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.frame`
=====================

This module declares frame and phrase nodes.
"""


from ..util import listof, tupleof, maybe, Clonable, Comparable, Printable
from ..entity import TableEntity, ColumnEntity
from ..domain import Domain, BooleanDomain
from .coerce import coerce
from .code import Expression
from .term import Term, QueryTerm
from .signature import Signature, Bag, Formula


class Clause(Comparable, Clonable, Printable):
    """
    Represents a SQL clause.

    This is an abstract class; its subclasses are divided into two categories:
    frames (see :class:`Frame`) and phrases (see :class:`Phrase`).

    A clause tree represents a SQL statement and is the next-to-last
    structure of the HTSQL translator.  A clause tree is translated from
    the term tree and the code graph by the *assembling* process.  It is then
    translated to SQL by the *serializing* process.

    The following adapters are associated with the assembling process and
    generate new clause nodes::

        Assemble: (Term, AssemblingState) -> Frame
        Evaluate: (Code, AssemblingState) -> Phrase

    See :class:`htsql.tr.assemble.Assemble` and
    :class:`htsql.tr.assemble.Evaluate` for more detail.

    The following adapter is associated with the serializing process::

        Dump: (Clause, DumpingState) -> str

    See :class:`htsql.tr.dump.Dump` for more detail.

    Clause nodes support equality by value.

    The constructor arguments:

    `expression` (:class:`htsql.tr.code.Expression`)
        The expression node that gave rise to the clause; for debugging
        and error reporting only.

    `equality_vector` (an immutable tuple or ``None``)
        Encapsulates all essential attributes of a node.  Two clauses are
        equal if and only if they are of the same type and their equality
        vectors are equal.  If ``None``, the clause is compared by identity.

        Note that the `expression` attribute is not essential and should
        not be a part of the equality vector.

    Other attributes:

    `binding` (:class:`htsql.tr.binding.Binding`)
        The binding node that gave rise to the expression; for debugging
        purposes only.

    `syntax` (:class:`htsql.tr.syntax.Syntax`)
        The syntax node that gave rise to the expression; for debugging
        purposes only.

    `mark` (:class:`htsql.mark.Mark`)
        The location of the node in the original query; for error reporting.

    `hash` (an integer)
        The node hash; if two nodes are considered equal, their hashes
        must be equal too.
    """

    def __init__(self, expression, equality_vector=None):
        assert isinstance(expression, Expression)
        super(Clause, self).__init__(equality_vector)
        self.expression = expression
        self.binding = expression.binding
        self.syntax = expression.syntax
        self.mark = expression.mark

    def __str__(self):
        return str(self.expression)


class Frame(Clause):
    """
    Represents a SQL frame.

    A *frame* is a node in the query tree, that is, one of these:

    - a top level ``SELECT`` clause, the root of the tree;
    - a nested ``SELECT`` clause, a branch of the tree;
    - a table or a scalar (``DUAL``) clause, a leaf of the tree.

    :class:`Frame` is an abstract case class, see subclasses for concrete
    frame types.

    Each frame node has a unique (in the context of the whole tree) identifier
    called the *tag*.  Tags are used to refer to frame nodes indirectly.

    As opposed to phrases, frame nodes are always compared by identity.

    Class attributes:

    `is_leaf` (Boolean)
        Indicates that the frame is terminal (either a table or a scalar).

    `is_scalar` (Boolean)
        Indicates that the frame is a scalar frame.

    `is_table` (Boolean)
        Indicates that the frame is a table frame.

    `is_branch` (Boolean)
        Indicates that the frame is non-terminal (either nested or a segment).

    `is_nested` (Boolean)
        Indicates that the frame is a nested branch frame.

    `is_segment` (Boolean)
        Indicates that the frame is the segment frame.

    Constructor arguments:

    `kids` (a list of :class:`Frame`)
        A list of child frames.

    `term` (:class:`htsql.tr.term.Term`)
        The term node that gave rise to the frame.

    Other attributes:

    `tag` (an integer)
        A unique identifier of the frame; inherited from the term.

    `space` (:class:`htsql.tr.code.Space`)
        The space represented by the frame; inherited from the term.

    `baseline` (:class:`htsql.tr.code.Space`)
        The baseline space of the frame; inherited from the term.
    """

    is_leaf = False
    is_scalar = False
    is_table = False
    is_branch = False
    is_nested = False
    is_segment = False

    def __init__(self, kids, term):
        # Sanity check on the arguments.
        assert isinstance(kids, listof(Frame))
        assert isinstance(term, Term)
        super(Frame, self).__init__(term.expression)
        self.kids = kids
        self.term = term
        # Extract semantically important attributes of the term.
        self.tag = term.tag
        self.space = term.space
        self.baseline = term.baseline

    def __str__(self):
        return str(self.term)


class LeafFrame(Frame):
    """
    Represents a leaf frame.

    This is an abstract class; for concrete subclasses, see
    :class:`ScalarFrame` and :class:`TableFrame`.
    """

    is_leaf = True

    def __init__(self, term):
        super(LeafFrame, self).__init__([], term)


class ScalarFrame(LeafFrame):
    """
    Represents a scalar frame.

    In SQL, a scalar frame is embodied by a special one-row ``DUAL`` table.
    """

    is_scalar = True


class TableFrame(LeafFrame):
    """
    Represents a table frame.

    In SQL, table frames are serialized as tables in the ``FROM`` list.

    `table` (:class:`htsql.entity.TableEntity`)
        The table represented by the frame.
    """

    is_table = True

    def __init__(self, table, term):
        assert isinstance(table, TableEntity)
        super(TableFrame, self).__init__(term)
        self.table = table


class BranchFrame(Frame):
    """
    Represents a branch frame.

    This is an abstract class; for concrete subclasses, see
    :class:`NestedFrame` and :class:`SegmentFrame`.

    In SQL, a branch frame is serialized as a top level (segment)
    or a nested ``SELECT`` statement.

    `include` (a list of :class:`Anchor`)
        Represents the ``FROM`` clause.

    `embed` (a list of :class:`NestedFrame`)
        Correlated subqueries that are used in the frame.

        A correlated subquery is a sub-``SELECT`` statement that appears
        outside the ``FROM`` list.  The `embed` list keeps all correlated
        subqueries that appear in the frame.  To refer to a correlated
        subquery from a phrase, use :class:`EmbeddingPhrase`.

    `select` (a list of :class:`Phrase`)
        Represents the ``SELECT`` clause.

    `where` (:class:`Phrase` or ``None``)
        Represents the ``WHERE`` clause.

    `group` (a list of :class:`Phrase`)
        Represents the ``GROUP BY`` clause.

    `having` (:class:`Phrase` or ``None``)
        Represents the ``HAVING`` clause.

    `order` (a list of pairs `(phrase, direction)`)
        Represents the ``ORDER BY`` clause.

        Here `phrase` is a :class:`Phrase` instance, `direction`
        is either ``+1`` (indicates ascending order) or ``-1``
        (indicates descending order).

    `limit` (a non-negative integer or ``None``)
        Represents the ``LIMIT`` clause.

    `offset` (a non-negative integer or ``None``)
        Represents the ``OFFSET`` clause.
    """

    is_branch = True

    def __init__(self, include, embed, select,
                 where, group, having, order, limit, offset, term):
        # Note that we do not require `include` list to be non-empty,
        # thus an instance of `BranchFrame` could actually be a leaf
        # in the frame tree!
        assert isinstance(include, listof(Anchor))
        # Check that the join condition on the first subframe is no-op.
        if include:
            assert isinstance(include[0], LeadingAnchor)
        assert isinstance(embed, listof(NestedFrame))
        assert isinstance(select, listof(Phrase)) and len(select) > 0
        assert isinstance(where, maybe(Phrase))
        assert isinstance(group, listof(Phrase))
        assert isinstance(having, maybe(Phrase))
        assert isinstance(order, listof(tupleof(Phrase, int)))
        assert isinstance(limit, maybe(int))
        assert isinstance(offset, maybe(int))
        assert limit is None or limit >= 0
        assert offset is None or offset >= 0
        kids = [anchor.frame for anchor in include] + embed
        super(BranchFrame, self).__init__(kids, term)
        self.include = include
        self.embed = embed
        self.select = select
        self.where = where
        self.group = group
        self.having = having
        self.order = order
        self.limit = limit
        self.offset = offset


class NestedFrame(BranchFrame):
    """
    Represents a nested ``SELECT`` statement.
    """

    is_nested = True


class SegmentFrame(BranchFrame):
    """
    Represents a top-level ``SELECT`` statement.
    """

    is_segment = True


class Anchor(Clause):
    """
    Represents a ``JOIN`` clause.

    `frame` (:class:`Frame`)
        The joined frame.

    `condition` (:class:`Phrase` or ``None``)
        The join condition.

    `is_left` (Boolean)
        Indicates that the join is ``LEFT OUTER``.

    `is_right` (Boolean)
        Indicates that the join is ``RIGHT OUTER``.

    Other attributes:

    `is_inner` (Boolean)
        Indicates that the join is ``INNER`` (that is, not left or right).

    `is_cross` (Boolean)
        Indicates that the join is ``CROSS`` (that is, inner without
        any join condition).
    """

    def __init__(self, frame, condition, is_left, is_right):
        assert isinstance(frame, Frame) and not frame.is_segment
        assert isinstance(condition, maybe(Phrase))
        assert condition is None or isinstance(condition.domain, BooleanDomain)
        assert isinstance(is_left, bool) and isinstance(is_right, bool)
        super(Anchor, self).__init__(frame.expression)
        self.frame = frame
        self.condition = condition
        self.is_left = is_left
        self.is_right = is_right
        self.is_inner = (not is_left and not is_right)
        self.is_cross = (self.is_inner and condition is None)


class LeadingAnchor(Anchor):
    """
    Represents the leading frame in the ``FROM`` list.

    `frame` (:class:`Frame`)
        The leading frame.

    `condition` (``None``)
        The join condition.

    `is_left` (``False``)
        Indicates that the join is ``LEFT OUTER``.

    `is_right` (``False``)
        Indicates that the join is ``RIGHT OUTER``.

    """

    def __init__(self, frame, condition=None, is_left=False, is_right=False):
        # We retain the constructor arguments to faciliate `clone()`, but
        # we ensure that their values are always fixed.
        assert condition is None
        assert is_left is False and is_right is False
        super(LeadingAnchor, self).__init__(frame, condition,
                                            is_left, is_right)


class QueryFrame(Clause):
    """
    Represents the whole HTSQL query.

    `segment` (:class:`SegmentFrame` or ``None``)
        The query segment.
    """

    def __init__(self, segment, term):
        assert isinstance(segment, maybe(SegmentFrame))
        assert isinstance(term, QueryTerm)
        super(QueryFrame, self).__init__(term.expression)
        self.segment = segment
        self.term = term


class Phrase(Clause):
    """
    Represents a SQL expression.

    `domain` (:class:`htsql.domain.Domain`)
        The co-domain of the expression.

    `is_nullable` (Boolean)
        Indicates if the expression may evaluate to ``NULL``.
    """

    def __init__(self, domain, is_nullable, expression, equality_vector):
        assert isinstance(domain, Domain)
        assert isinstance(is_nullable, bool)
        super(Phrase, self).__init__(expression, equality_vector)
        self.domain = domain
        self.is_nullable = is_nullable


class LiteralPhrase(Phrase):
    """
    Represents a literal value.

    `value` (valid type depends on the domain)
        The value.

    `domain` (:class:`htsql.domain.Domain`)
        The value type.
    """

    def __init__(self, value, domain, expression):
        # Note: `NULL` values are represented as `None`.
        is_nullable = (value is None)
        equality_vector = (value, domain)
        super(LiteralPhrase, self).__init__(domain, is_nullable, expression,
                                            equality_vector)
        self.value = value


class NullPhrase(LiteralPhrase):
    """
    Represents a ``NULL`` value.

    ``NULL`` values are commonly generated and checked for, so for
    convenience, they are extracted in a separate class.  Note that
    it is also valid for a ``NULL`` value to be represented as a regular
    :class:`LiteralPhrase` instance.
    """

    def __init__(self, domain, expression):
        super(NullPhrase, self).__init__(None, domain, expression)


class TruePhrase(LiteralPhrase):
    """
    Represents a ``TRUE`` value.

    ``TRUE`` values are commonly generated and checked for, so for
    convenience, they are extracted in a separate class.  Note that
    it is also valid for a ``TRUE`` value to be represented as a regular
    :class:`LiteralPhrase` instance.
    """

    def __init__(self, expression):
        domain = coerce(BooleanDomain())
        super(TruePhrase, self).__init__(True, domain, expression)


class FalsePhrase(LiteralPhrase):
    """
    Represents a ``FALSE`` value.

    ``FALSE`` values are commonly generated and checked for, so for
    convenience, they are extracted in a separate class.  Note that
    it is also valid for a ``FALSE`` value to be represented as a regular
    :class:`LiteralPhrase` instance.
    """

    def __init__(self, expression):
        domain = coerce(BooleanDomain())
        super(FalsePhrase, self).__init__(False, domain, expression)


class CastPhrase(Phrase):
    """
    Represents the ``CAST`` operator.

    `base` (:class:`Phrase`)
        The expression to convert.

    `domain` (:class:`htsql.domain.Domain`)
        The target domain.
    """

    def __init__(self, base, domain, is_nullable, expression):
        assert isinstance(base, Phrase)
        equality_vector = (base, domain, is_nullable)
        super(CastPhrase, self).__init__(domain, is_nullable, expression,
                                         equality_vector)
        self.base = base


class FormulaPhrase(Formula, Phrase):
    """
    Represents a formula phrase.

    A formula phrase represents a function or an operator call as
    a phrase node.

    `signature` (:class:`htsql.tr.signature.Signature`)
        The signature of the formula.

    `domain` (:class:`Domain`)
        The co-domain of the formula.

    `arguments` (a dictionary)
        The arguments of the formula.

        Note that all the arguments become attributes of the node object.
    """

    def __init__(self, signature, domain, is_nullable, expression, **arguments):
        assert isinstance(signature, Signature)
        # Check that the arguments match the formula signature.
        arguments = Bag(**arguments)
        assert arguments.admits(Phrase, signature)
        equality_vector = (signature, domain, arguments.freeze())
        # The first tow arguments are processed by the `Formula`
        # constructor; the rest of them go to the `Phrase` constructor.
        super(FormulaPhrase, self).__init__(signature, arguments,
                                            domain, is_nullable, expression,
                                            equality_vector)


class ExportPhrase(Phrase):
    """
    Represents a value exported from another frame.

    This is an abstract class; for concrete subclasses, see
    :class:`ColumnPhrase`, :class:`ReferencePhrase`, and
    :class:`EmbeddingPhrase`.

    `tag` (an integer)
        The tag of the frame that exports the value.
    """

    def __init__(self, tag, domain, is_nullable, expression, equality_vector):
        assert isinstance(tag, int)
        super(ExportPhrase, self).__init__(domain, is_nullable, expression,
                                           equality_vector)
        self.tag = tag


class ColumnPhrase(ExportPhrase):
    """
    Represents a column exported from a table frame.

    `tag` (an integer)
        The tag of the table frame.

        The tag must point to an immediate child of the current frame.

    `class` (:class:`htsql.entity.ColumnEntity`)
        The column to export.
    """

    def __init__(self, tag, column, is_nullable, expression):
        assert isinstance(column, ColumnEntity)
        domain = column.domain
        equality_vector = (tag, column)
        super(ColumnPhrase, self).__init__(tag, domain, is_nullable,
                                           expression, equality_vector)
        self.column = column


class ReferencePhrase(ExportPhrase):
    """
    Represents a value exported from a nested sub-``SELECT`` frame.

    `tag` (an integer)
        The tag of the nested frame.

        The tag must point to an immediate child of the current frame.

    `index` (an integer)
        The position of the exported value in the ``SELECT`` clause.
    """

    def __init__(self, tag, index, domain, is_nullable, expression):
        assert isinstance(index, int) and index >= 0
        equality_vector = (tag, index)
        super(ReferencePhrase, self).__init__(tag, domain, is_nullable,
                                              expression, equality_vector)
        self.index = index


class EmbeddingPhrase(ExportPhrase):
    """
    Represents an embedding of a correlated subquery.

    `tag` (an integer)
        The tag of the nested frame.

        The tag must point to one of the subframes contained in the
        `embed` list of the current frame.
    """

    def __init__(self, tag, domain, is_nullable, expression):
        equality_vector = (tag,)
        super(EmbeddingPhrase, self).__init__(tag, domain, is_nullable,
                                              expression, equality_vector)


