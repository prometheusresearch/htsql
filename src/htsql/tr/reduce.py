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
                    NullPhrase, TruePhrase, FalsePhrase,
                    EqualityPhraseBase, ConnectivePhraseBase,
                    ConjunctionPhrase, NegationPhrase,
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
        phrase = TruePhrase(self.term.expression)
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

    def prune_scalars(self, frame):
        if not frame.include:
            return frame
        include = frame.include[:]
        idx = 0
        while idx < len(include):
            anchor = include[idx]
            next_anchor = (include[idx+1] if idx+1 < len(include) else None)
            if (anchor.frame.is_scalar and anchor.is_cross
                    and (next_anchor is None or next_anchor.is_cross)):
                del include[idx]
            else:
                idx += 1
        return frame.clone(include=include)

    def collapse(self, frame):
        if not frame.include:
            return frame
        head = frame.include[0].frame
        tail = frame.include[1:]
        if not head.is_nested:
            return frame
        head = self.prune_scalars(head)
        head = self.collapse(head)
        if not head.include:
            return frame
        if head.group:
            return frame
        assert head.having is None
        if head.limit is not None or head.offset is not None:
            if frame.limit is not None or frame.offset is not None:
                return frame
            if not (head.space.conforms(frame.space) and
                        head.baseline == frame.baseline and
                        head.space.ordering() == frame.space.ordering()):
                return frame
        include = head.include+tail
        embed = head.embed+frame.embed
        for index, phrase in enumerate(head.select):
            key = (head.tag, index)
            self.state.substitutes[key] = phrase
        where = frame.where
        if head.where:
            if where is None:
                where = head.where
            else:
                where = ConjunctionPhrase([where, head.where],
                                          where.expression)
        order = head.order
        if frame.order:
            order = frame.order
        limit = head.limit
        if frame.limit is not None:
            limit = frame.limit
        offset = head.offset
        if frame.offset is not None:
            offset = frame.offset
        return frame.clone(include=include, embed=embed, where=where,
                           order=order, limit=limit, offset=offset)

    def reduce_include(self, frame):
        frame = self.prune_scalars(frame)
        frame = self.collapse(frame)
        if not frame.include:
            return frame
        include = [self.state.reduce(anchor)
                   for anchor in frame.include]
        return frame.clone(include=include)

    def reduce_embed(self, frame):
        if not frame.embed:
            return frame
        embed = [self.state.reduce(subframe)
                 for subframe in frame.embed]
        return frame.clone(embed=embed)

    def reduce_select(self, frame):
        select = [self.state.reduce(phrase)
                  for phrase in frame.select]
        return frame.clone(select=select)

    def reduce_where(self, frame):
        if frame.where is None:
            return frame
        where = self.state.reduce(frame.where)
        if isinstance(where, TruePhrase):
            where = None
        return frame.clone(where=where)

    def reduce_group(self, frame):
        if not frame.group:
            return frame
        group = [self.state.reduce(phrase)
                 for phrase in frame.group]
        return frame.clone(group=group)

    def reduce_having(self, frame):
        if frame.having is None:
            return frame
        having = self.state.reduce(frame.having)
        if isinstance(having, TruePhrase):
            having = None
        return frame.clone(having=having)

    def reduce_order(self, frame):
        if not frame.order:
            return frame
        order = [(self.state.reduce(phrase), direction)
                 for phrase, direction in frame.order]
        return frame.clone(order=order)

    def __call__(self):
        frame = self.frame
        frame = self.reduce_include(frame)
        frame = self.reduce_embed(frame)
        frame = self.reduce_select(frame)
        frame = self.reduce_where(frame)
        frame = self.reduce_group(frame)
        frame = self.reduce_having(frame)
        frame = self.reduce_order(frame)
        return frame


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
        if self.phrase.value is None:
            return NullPhrase(self.phrase.domain, self.phrase.expression)
        if isinstance(self.phrase.domain, BooleanDomain):
            if self.phrase.value is True:
                return TruePhrase(self.phrase.expression)
            if self.phrase.value is False:
                return FalsePhrase(self.phrase.expression)
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
            return self.state.reduce(self.state.substitutes[key])
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


