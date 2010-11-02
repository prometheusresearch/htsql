#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.reduce`
======================

This module implements the reducing process.
"""


from ..adapter import Adapter, adapts
from ..domain import BooleanDomain
from .coerce import coerce
from .frame import (Clause, Frame, ScalarFrame, TableFrame, BranchFrame,
                    NestedFrame, QueryFrame, Phrase, LiteralPhrase,
                    EqualityPhraseBase, ConnectivePhraseBase, NegationPhrase,
                    CastPhrase, Link, ReferenceLink, Anchor)


class ReducingState(object):

    def __init__(self):
        self.substitutes = {}

    def reduce(self, clause):
        return reduce(clause, self)


class Reduce(Adapter):

    adapts(Clause)

    def __init__(self, clause, state):
        self.clause = clause
        self.state = state

    def __call__(self):
        raise NotImplementedError()


class Collapse(Reduce):

    adapts(Frame)

    def __init__(self, clause, state):
        super(Collapse, self).__init__(clause, state)
        self.frame = clause
        self.term = clause.term


class CollapseScalar(Collapse):

    adapts(ScalarFrame)

    def __call__(self):
        phrase = LiteralPhrase(True, coerce(BooleanDomain()),
                               self.term.expression)
        return NestedFrame(include=[], embed=[],
                           select=[phrase], where=None,
                           group=[], having=None,
                           order=[], limit=None, offset=None,
                           term=self.term)


class CollapseTable(Collapse):

    adapts(TableFrame)

    def __call__(self):
        return self.frame


class CollapseBranch(Collapse):

    adapts(BranchFrame)

    def __call__(self):
        include = [self.state.reduce(anchor)
                   for anchor in self.frame.include]
        embed = [self.state.reduce(frame)
                 for frame in self.frame.embed]
        select = [self.state.reduce(phrase)
                  for phrase in self.frame.select]
        where = (self.state.reduce(self.frame.where)
                 if self.frame.where is not None else None)
        if (isinstance(where, LiteralPhrase) and
            isinstance(where.domain, BooleanDomain) and
            where.value is True):
            where = None
        group = [self.state.reduce(phrase)
                 for phrase in self.frame.group]
        having = (self.state.reduce(self.frame.having)
                  if self.frame.having is not None else None)
        if (isinstance(having, LiteralPhrase) and
            isinstance(having.domain, BooleanDomain) and
            having.value is True):
            having = None
        order = [(self.state.reduce(phrase), direction)
                 for phrase, direction in self.frame.order]
        limit = self.frame.limit
        offset = self.frame.offset
        return self.frame.clone(include=include,
                                embed=embed,
                                select=select,
                                where=where,
                                group=group,
                                having=having,
                                order=order,
                                limit=limit,
                                offset=offset)


class CollapseQuery(Collapse):

    adapts(QueryFrame)

    def __call__(self):
        if self.frame.segment is not None:
            segment = self.state.reduce(self.frame.segment)
            return self.frame.clone(segment=segment)
        return self.frame


class ReducePhrase(Reduce):

    adapts(Phrase)

    def __init__(self, clause, state):
        super(ReducePhrase, self).__init__(clause, state)
        self.phrase = clause


class ReduceLiteral(Reduce):

    adapts(LiteralPhrase)

    def __call__(self):
        return self.phrase


class ReduceEquality(Reduce):

    adapts(EqualityPhraseBase)

    def __call__(self):
        lop = self.state.reduce(self.phrase.lop)
        rop = self.state.reduce(self.phrase.rop)
        return self.phrase.clone(lop=lop, rop=rop)


class ReduceConnective(Reduce):

    adapts(ConnectivePhraseBase)

    def __call__(self):
        ops = [self.state.reduce(op) for op in self.phrase.ops]
        if len(ops) == 1:
            return ops[0]
        return self.phrase.clone(ops=ops)


class ReduceNegation(Reduce):

    adapts(NegationPhrase)

    def __call__(self):
        op = self.state.reduce(self.phrase.op)
        return self.phrase.clone(op=op)


class ReduceCast(Reduce):

    adapts(CastPhrase)

    def __call__(self):
        base = self.state.reduce(self.phrase.base)
        return self.phrase.clone(base=base)


class ReduceLink(Reduce):

    adapts(Link)

    def __init__(self, clause, state):
        super(ReduceLink, self).__init__(clause, state)
        self.link = clause

    def __call__(self):
        return self.link


class ReduceReference(Reduce):

    adapts(ReferenceLink)

    def __call__(self):
        key = (self.link.tag, self.link.index)
        if key in self.state.substitutes:
            return self.state.substitutes[key]
        return self.link


class ReduceAnchor(Reduce):

    adapts(Anchor)

    def __init__(self, clause, state):
        super(ReduceAnchor, self).__init__(clause, state)
        self.anchor = clause

    def __call__(self):
        frame = self.state.reduce(self.anchor.frame)
        condition = (self.state.reduce(self.anchor.condition)
                     if self.anchor.condition is not None else None)
        return self.anchor.clone(frame=frame, condition=condition)


def reduce(clause, state=None):
    if state is None:
        state = ReducingState()
    reduce = Reduce(clause, state)
    return reduce()


