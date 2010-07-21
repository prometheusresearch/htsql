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


from ..util import listof, tupleof, maybe, Node
from ..entity import TableEntity, ColumnEntity
from ..domain import Domain, BooleanDomain
from ..mark import Mark
from .sketch import QuerySketch


class Clause(Node):

    def __init__(self, mark):
        assert isinstance(mark, Mark)
        self.mark = mark


class Frame(Clause):

    is_leaf = False
    is_scalar = False
    is_branch = False
    is_segment = False
    is_query = False


class LeafFrame(Frame):

    is_leaf = True

    def __init__(self, table, mark):
        assert isinstance(table, TableEntity)
        super(LeafFrame, self).__init__(mark)
        self.table = table


class ScalarFrame(Frame):

    is_scalar = True


class BranchFrame(Frame):

    is_branch = True

    def __init__(self, select=[], linkage=[], filter=None,
                 group=[], group_filter=None, order=[],
                 limit=None, offset=None, mark=None):
        assert isinstance(select, listof(Phrase))
        assert isinstance(linkage, listof(Link))
        assert isinstance(filter, maybe(Phrase))
        assert isinstance(group, listof(Phrase))
        assert isinstance(group_filter, maybe(Phrase))
        assert isinstance(order, listof(tupleof(Phrase, int)))
        assert isinstance(limit, maybe(int))
        assert isinstance(offset, maybe(int))
        super(BranchFrame, self).__init__(mark)
        self.select = select
        self.linkage = linkage
        self.filter = filter
        self.group = group
        self.group_filter = group_filter
        self.order = order
        self.limit = limit
        self.offset = offset


class CorrelatedFrame(BranchFrame):

    is_correlated = True


class SegmentFrame(BranchFrame):

    is_segment = True


class QueryFrame(Frame):

    def __init__(self, sketch, segment, mark):
        assert isinstance(sketch, QuerySketch)
        assert isinstance(segment, maybe(SegmentFrame))
        super(QueryFrame, self).__init__(mark)
        self.sketch = sketch
        self.term = sketch.term
        self.code = sketch.code
        self.binding = sketch.binding
        self.syntax = sketch.syntax
        self.segment = segment


class Link(Clause):

    def __init__(self, frame, condition=None, is_inner=True):
        assert isinstance(frame, Frame)
        assert isinstance(condition, maybe(Phrase))
        assert isinstance(is_inner, bool)
        self.frame = frame
        self.condition = condition
        self.is_inner = is_inner


class Phrase(Clause):

    def __init__(self, domain, is_nullable, mark):
        assert isinstance(domain, Domain)
        assert isinstance(is_nullable, bool)
        assert isinstance(mark, Mark)
        self.domain = domain
        self.is_nullable = is_nullable
        self.mark = mark

    def optimize(self):
        return self


class EqualityPhrase(Phrase):

    def __init__(self, left, right, mark):
        assert isinstance(left, Phrase)
        assert isinstance(right, Phrase)
        domain = BooleanDomain()
        is_nullable = (left.is_nullable or right.is_nullable)
        super(EqualityPhrase, self).__init__(domain, is_nullable, mark)
        self.left = left
        self.right = right


class InequalityPhrase(Phrase):

    def __init__(self, left, right, mark):
        assert isinstance(left, Phrase)
        assert isinstance(right, Phrase)
        domain = BooleanDomain()
        is_nullable = (left.is_nullable or right.is_nullable)
        super(InequalityPhrase, self).__init__(domain, is_nullable, mark)
        self.left = left
        self.right = right


class ConjunctionPhrase(Phrase):

    def __init__(self, terms, mark):
        assert isinstance(terms, listof(Phrase))
        domain = BooleanDomain()
        is_nullable = any(term.is_nullable for term in terms)
        super(ConjunctionPhrase, self).__init__(domain, is_nullable, mark)
        self.terms = terms

    def optimize(self):
        terms = []
        for term in self.terms:
            if isinstance(term, LiteralPhrase):
                if term.value is True:
                    continue
                if term.value is False:
                    return term
            terms.append(term)
        if not terms:
            return LiteralPhrase(True, BooleanDomain(), self.mark)
        if len(terms) == 1:
            return terms[0]
        if len(terms) != len(self.terms):
            return self.clone(terms=terms)
        return self


class DisjunctionPhrase(Phrase):

    def __init__(self, terms, mark):
        assert isinstance(terms, listof(Phrase))
        domain = BooleanDomain()
        is_nullable = any(term.is_nullable for term in terms)
        super(DisjunctionPhrase, self).__init__(domain, is_nullable, mark)
        self.terms = terms

    def optimize(self):
        terms = []
        for term in self.terms:
            if isinstance(term, LiteralPhrase):
                if term.value is False:
                    continue
                if term.value is True:
                    return term
            terms.append(term)
        if not terms:
            return LiteralPhrase(False, BooleanDomain(), self.mark)
        if len(terms) == 1:
            return terms[0]
        if len(terms) != len(self.terms):
            return self.clone(terms=terms)
        return self


class NegationPhrase(Phrase):

    def __init__(self, term, mark):
        assert isinstance(term, Phrase)
        domain = BooleanDomain()
        is_nullable = term.is_nullable
        super(NegationPhrase, self).__init__(domain, is_nullable, mark)
        self.term = term

    def optimize(self):
        if isinstance(self.term, LiteralPhrase):
            if self.term.value is True:
                return LiteralPhrase(False, BooleanDomain(), self.mark)
            if self.term.value is False:
                return LiteralPhrase(True, BooleanDomain(), self.mark)
            return self.term
        return self


class TuplePhrase(Phrase):

    def __init__(self, units, mark):
        assert isinstance(units, listof(Phrase))
        domain = BooleanDomain()
        is_nullable = False
        super(TuplePhrase, self).__init__(domain, is_nullable, mark)
        self.units = units


class CastPhrase(Phrase):

    def __init__(self, phrase, domain, is_nullable, mark):
        assert isinstance(phrase, Phrase)
        super(CastPhrase, self).__init__(domain, is_nullable, mark)
        self.phrase = phrase


class LiteralPhrase(Phrase):

    def __init__(self, value, domain, mark):
        is_nullable = (value is None)
        super(LiteralPhrase, self).__init__(domain, is_nullable, mark)
        self.value = value


class FunctionPhrase(Phrase):

    def __init__(self, domain, is_nullable, mark, **arguments):
        super(FunctionPhrase, self).__init__(domain, is_nullable, mark)
        self.arguments = arguments
        for key in arguments:
            setattr(self, key, arguments[key])


class LeafReferencePhrase(Phrase):

    def __init__(self, frame, is_inner, column, mark):
        assert isinstance(frame, LeafFrame)
        assert isinstance(is_inner, bool)
        assert isinstance(column, ColumnEntity)
        is_nullable = (column.is_nullable or not is_inner)
        super(LeafReferencePhrase, self).__init__(column.domain,
                                                  is_nullable, mark)
        self.frame = frame
        self.is_inner = is_inner
        self.column = column


class BranchReferencePhrase(Phrase):

    def __init__(self, frame, is_inner, position, mark):
        assert isinstance(frame, BranchFrame)
        assert isinstance(is_inner, bool)
        assert isinstance(position, int)
        assert 0 <= position < len(frame.select)
        phrase = frame.select[position]
        domain = phrase.domain
        is_nullable = (phrase.is_nullable or not is_inner)
        super(BranchReferencePhrase, self).__init__(domain, is_nullable, mark)
        self.frame = frame
        self.is_inner = is_inner
        self.position = position
        self.mark = mark
        self.phrase = phrase


class CorrelatedFramePhrase(Phrase):

    def __init__(self, frame, mark):
        assert isinstance(frame, CorrelatedFrame)
        assert len(frame.select) == 1
        domain = frame.select[0].domain
        super(CorrelatedFramePhrase, self).__init__(domain, True, mark)
        self.frame = frame


