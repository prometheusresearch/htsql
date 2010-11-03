#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.frame`
=====================

This module declares frame and phrase nodes.
"""


from ..util import listof, tupleof, maybe, Node, Comparable
from ..entity import TableEntity, ColumnEntity
from ..domain import Domain, BooleanDomain
from .coerce import coerce
from .code import Expression
from .term import Term, RoutingTerm


class Clause(Comparable, Node):

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

    is_leaf = False
    is_scalar = False
    is_table = False
    is_branch = False
    is_nested = False
    is_segment = False
    is_query = False

    def __init__(self, kids, term):
        assert isinstance(kids, listof(Frame))
        assert isinstance(term, Term)
        if self.is_leaf or self.is_branch:
            assert isinstance(term, RoutingTerm)
        super(Frame, self).__init__(term.expression)
        self.kids = kids
        self.term = term
        self.tag = term.tag
        if self.is_leaf or self.is_branch:
            self.space = term.space
            self.baseline = term.baseline

    def __str__(self):
        return str(self.term)


class LeafFrame(Frame):

    is_leaf = True

    def __init__(self, term):
        super(LeafFrame, self).__init__([], term)


class ScalarFrame(LeafFrame):

    is_scalar = True


class TableFrame(LeafFrame):

    is_table = True

    def __init__(self, table, term):
        assert isinstance(table, TableEntity)
        super(TableFrame, self).__init__(term)
        self.table = table


class BranchFrame(Frame):

    is_branch = True

    def __init__(self, include, embed, select, where, group, having,
                 order, limit, offset, term):
        assert isinstance(include, listof(Anchor))
        if include:
            assert include[0].is_cross
        assert isinstance(embed, listof(NestedFrame))
        assert isinstance(select, listof(Phrase)) and len(select) > 0
        assert isinstance(where, maybe(Phrase))
        assert isinstance(group, listof(Phrase))
        assert isinstance(having, maybe(Phrase))
        assert isinstance(order, listof(tupleof(Phrase, int)))
        assert isinstance(limit, maybe(int))
        assert isinstance(offset, maybe(int))
        assert isinstance(term, RoutingTerm)
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

    is_nested = True


class SegmentFrame(BranchFrame):

    is_segment = True


class QueryFrame(Frame):

    is_query = True

    def __init__(self, segment, term):
        assert isinstance(segment, maybe(SegmentFrame))
        super(QueryFrame, self).__init__([], term)
        self.segment = segment


class Phrase(Clause):

    def __init__(self, domain, is_nullable, expression, equality_vector):
        assert isinstance(domain, Domain)
        assert isinstance(is_nullable, bool)
        super(Phrase, self).__init__(expression, equality_vector)
        self.domain = domain
        self.is_nullable = is_nullable


class LiteralPhrase(Phrase):

    def __init__(self, value, domain, expression):
        is_nullable = (value is None)
        equality_vector = (value, domain)
        super(LiteralPhrase, self).__init__(domain, is_nullable, expression,
                                            equality_vector)
        self.value = value


class NullPhrase(LiteralPhrase):

    def __init__(self, domain, expression):
        super(NullPhrase, self).__init__(None, domain, expression)


class TruePhrase(LiteralPhrase):

    def __init__(self, expression):
        domain = coerce(BooleanDomain())
        super(TruePhrase, self).__init__(True, domain, expression)


class FalsePhrase(LiteralPhrase):

    def __init__(self, expression):
        domain = coerce(BooleanDomain())
        super(FalsePhrase, self).__init__(False, domain, expression)


class EqualityPhraseBase(Phrase):

    is_regular = False
    is_total = False
    is_positive = False
    is_negative = False

    def __init__(self, lop, rop, expression):
        assert isinstance(lop, Phrase)
        assert isinstance(rop, Phrase)
        domain = coerce(BooleanDomain())
        is_nullable = self.is_regular and (lop.is_nullable or rop.is_nullable)
        equality_vector = (lop, rop)
        super(EqualityPhraseBase, self).__init__(domain, is_nullable,
                                                 expression, equality_vector)
        self.lop = lop
        self.rop = rop


class EqualityPhrase(EqualityPhraseBase):

    is_regular = True
    is_positive = True


class InequalityPhrase(EqualityPhraseBase):

    is_regular = True
    is_negative = True


class TotalEqualityPhrase(EqualityPhraseBase):

    is_total = True
    is_positive = True


class TotalInequalityPhrase(EqualityPhraseBase):

    is_total = True
    is_negative = True


class ConnectivePhraseBase(Phrase):

    is_conjunction = False
    is_disjunction = False

    def __init__(self, ops, expression):
        assert isinstance(ops, listof(Phrase))
        assert all(isinstance(op.domain, BooleanDomain) for op in ops)
        domain = coerce(BooleanDomain())
        is_nullable = any(op.is_nullable for op in ops)
        equality_vector = tuple(ops)
        super(ConnectivePhraseBase, self).__init__(domain, is_nullable,
                                                   expression, equality_vector)
        self.ops = ops


class ConjunctionPhrase(ConnectivePhraseBase):

    is_conjunction = True


class DisjunctionPhrase(ConnectivePhraseBase):

    is_disjunction = True


class NegationPhrase(Phrase):

    def __init__(self, op, expression):
        assert isinstance(op, Phrase)
        assert isinstance(op.domain, BooleanDomain)
        domain = coerce(BooleanDomain())
        is_nullable = op.is_nullable
        equality_vector = (op,)
        super(NegationPhrase, self).__init__(domain, is_nullable, expression,
                                             equality_vector)
        self.op = op


class IsNullPhraseBase(Phrase):

    is_positive = False
    is_negative = False

    def __init__(self, op, expression):
        assert isinstance(op, Phrase)
        domain = coerce(BooleanDomain())
        is_nullable = False
        equality_vector = (op,)
        super(IsNullPhraseBase, self).__init__(domain, is_nullable, expression,
                                               equality_vector)
        self.op = op


class IsNullPhrase(IsNullPhraseBase):

    is_positive = True


class IsNotNullPhrase(IsNullPhraseBase):

    is_negative = True


class IfNullPhrase(Phrase):

    def __init__(self, lop, rop, domain, expression):
        assert isinstance(lop, Phrase)
        assert isinstance(rop, Phrase)
        assert isinstance(domain, Domain)
        is_nullable = (lop.is_nullable and rop.is_nullable)
        equality_vector = (lop, rop)
        super(IfNullPhrase, self).__init__(domain, is_nullable, expression,
                                           equality_vector)
        self.lop = lop
        self.rop = rop


class NullIfPhrase(Phrase):

    def __init__(self, lop, rop, domain, expression):
        assert isinstance(lop, Phrase)
        assert isinstance(rop, Phrase)
        assert isinstance(domain, Domain)
        is_nullable = True
        equality_vector = (lop, rop)
        super(NullIfPhrase, self).__init__(domain, is_nullable, expression,
                                           equality_vector)
        self.lop = lop
        self.rop = rop


class CastPhrase(Phrase):

    def __init__(self, base, domain, is_nullable, expression):
        assert isinstance(base, Phrase)
        equality_vector = (base, domain, is_nullable)
        super(CastPhrase, self).__init__(domain, is_nullable, expression,
                                         equality_vector)
        self.base = base


class FunctionPhrase(Phrase):

    def __init__(self, domain, is_nullable, expression, **arguments):
        # Extract the equality vector from the arguments (FIXME: messy).
        equality_vector = [domain]
        for key in sorted(arguments):
            value = arguments[key]
            # Argument values are expected to be `Phrase` objects,
            # lists of `Phrase` objects or some other (immutable) objects.
            if isinstance(value, list):
                value = tuple(value)
            equality_vector.append((key, value))
        equality_vector = tuple(equality_vector)
        super(FunctionPhrase, self).__init__(domain, is_nullable, expression,
                                             equality_vector)
        self.arguments = arguments
        for key in arguments:
            setattr(self, key, arguments[key])


class Link(Phrase):

    def __init__(self, tag, domain, is_nullable, expression, equality_vector):
        assert isinstance(tag, int)
        super(Link, self).__init__(domain, is_nullable, expression,
                                   equality_vector)
        self.tag = tag


class ColumnLink(Link):

    def __init__(self, tag, column, is_nullable, expression):
        assert isinstance(column, ColumnEntity)
        domain = column.domain
        equality_vector = (tag, column)
        super(ColumnLink, self).__init__(tag, domain, is_nullable,
                                         expression, equality_vector)
        self.column = column


class ReferenceLink(Link):

    def __init__(self, tag, index, domain, is_nullable, expression):
        assert isinstance(index, int) and index >= 0
        equality_vector = (tag, index)
        super(ReferenceLink, self).__init__(tag, domain, is_nullable,
                                            expression, equality_vector)
        self.index = index


class EmbeddingLink(Link):

    def __init__(self, tag, domain, is_nullable, expression):
        equality_vector = (tag,)
        super(EmbeddingLink, self).__init__(tag, domain, is_nullable,
                                            expression, equality_vector)


class Anchor(Phrase):

    def __init__(self, frame, condition, is_left, is_right, expression):
        assert isinstance(frame, (ScalarFrame, TableFrame, NestedFrame))
        assert isinstance(condition, maybe(Phrase))
        domain = coerce(BooleanDomain())
        is_nullable = (condition.is_nullable
                       if condition is not None else False)
        assert isinstance(is_left, bool) and isinstance(is_right, bool)
        equality_vector = (frame, condition, is_left, is_right)
        super(Anchor, self).__init__(domain, is_nullable, expression,
                                     equality_vector)
        self.frame = frame
        self.condition = condition
        self.is_left = is_left
        self.is_right = is_right
        self.is_inner = (not is_left and not is_right)
        self.is_cross = (self.is_inner and condition is None)


