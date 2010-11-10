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


from ..adapter import Adapter, adapts, adapts_many
from ..domain import (Domain, BooleanDomain, IntegerDomain, FloatDomain,
                      DecimalDomain, StringDomain, EnumDomain, DateDomain)
from .coerce import coerce
from .frame import (Clause, Frame, ScalarFrame, TableFrame, BranchFrame,
                    NestedFrame, QueryFrame, Phrase, LiteralPhrase,
                    NullPhrase, TruePhrase, FalsePhrase,
                    EqualityPhraseBase, EqualityPhrase, InequalityPhrase,
                    TotalEqualityPhrase, TotalInequalityPhrase,
                    ConnectivePhraseBase, ConjunctionPhrase, NegationPhrase,
                    IsNullPhraseBase, IsNullPhrase, IsNotNullPhrase,
                    IfNullPhrase, NullIfPhrase, CastPhrase,
                    ExportPhrase, ReferencePhrase, Anchor)


class ReducingState(object):

    def __init__(self):
        self.substitutes = {}

    def reduce(self, clause):
        return reduce(clause, self)

    def collapse(self, frame):
        collapse = Collapse(frame, self)
        return collapse()


class Reduce(Adapter):

    adapts(Clause)

    def __init__(self, clause, state):
        self.clause = clause
        self.state = state

    def __call__(self):
        raise NotImplementedError()


class ReduceFrame(Reduce):

    adapts(Frame)

    def __init__(self, frame, state):
        super(ReduceFrame, self).__init__(frame, state)
        self.frame = frame

    def __call__(self):
        return self.frame


class ReduceBranch(Reduce):

    adapts(BranchFrame)

    def reduce_include(self):
        return [self.state.reduce(anchor)
                for anchor in self.frame.include]

    def reduce_embed(self):
        return [self.state.reduce(self.state.collapse(frame))
                for frame in self.frame.embed]

    def reduce_select(self):
        return [self.state.reduce(phrase)
                for phrase in self.frame.select]

    def reduce_where(self):
        if self.frame.where is None:
            return None
        where = self.state.reduce(self.frame.where)
        if isinstance(where, TruePhrase):
            where = None
        return where

    def reduce_group(self):
        group = []
        duplicates = set()
        for phrase in self.frame.group:
            phrase = self.state.reduce(phrase)
            if isinstance(phrase, LiteralPhrase):
                continue
            if phrase in duplicates:
                continue
            group.append(phrase)
            duplicates.add(phrase)
        return group

    def reduce_having(self):
        if self.frame.having is None:
            return None
        having = self.state.reduce(self.frame.having)
        if isinstance(having, TruePhrase):
            having = None
        return having

    def reduce_order(self):
        order = []
        duplicates = set()
        for phrase, direction in self.frame.order:
            phrase = self.state.reduce(phrase)
            if isinstance(phrase, LiteralPhrase):
                continue
            if phrase in duplicates:
                continue
            order.append((phrase, direction))
            duplicates.add(phrase)
        return order

    def __call__(self):
        include = self.reduce_include()
        embed = self.reduce_embed()
        select = self.reduce_select()
        where = self.reduce_where()
        group = self.reduce_group()
        having = self.reduce_having()
        order = self.reduce_order()
        return self.frame.clone(include=include, embed=embed,
                                select=select, where=where,
                                group=group, having=having,
                                order=order)



class Collapse(Adapter):

    adapts(Frame)

    def __init__(self, frame, state):
        self.frame = frame
        self.term = frame.term
        self.state = state

    def collapse(self):
        return

    def __call__(self):
        frame = self.collapse()
        if frame is None:
            return self.frame
        return self.state.collapse(frame)


class CollapseScalar(Collapse):

    adapts(ScalarFrame)

    def collapse(self):
        select = [TruePhrase(self.term.expression)]
        return NestedFrame(include=[], embed=[], select=select,
                           where=None, group=[], having=None,
                           order=[], limit=None, offset=None,
                           term=self.term)


class CollapseBranch(Collapse):

    adapts(BranchFrame)

    def collapse(self):
        if not self.frame.include:
            return
        head = self.frame.include[0].frame
        tail = self.frame.include[1:]
        if head.is_scalar:
            if tail and not tail[0].is_cross:
                return
            return self.frame.clone(include=tail)
        if not head.is_nested:
            return
        if not head.include:
            if tail and not tail[0].is_cross:
                return
        if any(anchor.is_right for anchor in tail):
            return
        if head.group:
            if not all(isinstance(phrase, LiteralPhrase)
                       for phrase in head.group):
                return
            if tail:
                return
            if not (self.frame.where is None and
                    not self.frame.group and
                    not self.frame.order and
                    self.frame.limit is None and
                    self.frame.offset is None):
                return
            if not (head.having is None and
                    not head.order and
                    head.limit is None and
                    head.offset is None):
                return
        assert head.having is None
        if not (head.limit is None and
                head.offset is None):
            if not (self.frame.limit is None and
                    self.frame.offset is None):
                return
            if not (head.space.conforms(self.frame.space) and
                    head.baseline == self.frame.baseline and
                    head.space.ordering() == self.frame.space.ordering()):
                return
        include = head.include+tail
        embed = head.embed+self.frame.embed
        assert head.tag not in self.state.substitutes
        self.state.substitutes[head.tag] = head.select
        where = self.frame.where
        if head.where:
            if where is None:
                where = head.where
            else:
                where = ConjunctionPhrase([where, head.where],
                                          where.expression)
        order = head.order
        if self.frame.order:
            order = self.frame.order
        limit = head.limit
        if self.frame.limit is not None:
            limit = self.frame.limit
        offset = head.offset
        if self.frame.offset is not None:
            offset = self.frame.offset
        return self.frame.clone(include=include, embed=embed, where=where,
                                order=order, limit=limit, offset=offset)


class ReduceAnchor(Reduce):

    adapts(Anchor)

    def __init__(self, clause, state):
        super(ReduceAnchor, self).__init__(clause, state)
        self.anchor = clause

    def __call__(self):
        frame = self.state.reduce(self.state.collapse(self.anchor.frame))
        condition = (self.state.reduce(self.anchor.condition)
                     if self.anchor.condition is not None else None)
        return self.anchor.clone(frame=frame, condition=condition)


class ReduceQuery(Reduce):

    adapts(QueryFrame)

    def __call__(self):
        if self.clause.segment is None:
            return self.clause
        segment = self.clause.segment
        segment = self.state.collapse(segment)
        segment = self.state.reduce(segment)
        return self.clause.clone(segment=segment)


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
        if isinstance(lop, NullPhrase) and isinstance(rop, NullPhrase):
            if self.phrase.is_total:
                if self.phrase.is_positive:
                    return TruePhrase(self.phrase.expression)
                if self.phrase.is_negative:
                    return FalsePhrase(self.phrase.expression)
            else:
                return NullPhrase(self.phrase.domain, self.phrase.expression)
        if isinstance(lop, NullPhrase):
            lop, rop = rop, lop
        if isinstance(rop, NullPhrase):
            if self.phrase.is_total:
                if not lop.is_nullable:
                    if self.phrase.is_positive:
                        return FalsePhrase(self.phrase.expression)
                    if self.phrase.is_negative:
                        return TruePhrase(self.phrase.expression)
                if self.phrase.is_positive:
                    return IsNullPhrase(lop, self.phrase.expression)
                if self.phrase.is_negative:
                    return IsNotNullPhrase(rop, self.phrase.expression)
            else:
                return NullPhrase(self.phrase.domain, self.phrase.expression)
        if (isinstance(lop, LiteralPhrase) and isinstance(rop, LiteralPhrase)
                and isinstance(lop.domain, BooleanDomain)
                and isinstance(rop.domain, BooleanDomain)):
            if ((self.phrase.is_positive and lop.value == rop.value)
                    or (self.phrase.is_negative and lop.value != rop.value)):
                return TruePhrase(self.phrase.expression)
            if ((self.phrase.is_positive and lop.value != rop.value)
                    or (self.phrase.is_negative and lop.value == rop.value)):
                return FalsePhrase(self.phrase.expression)
        if self.phrase.is_total and not (lop.is_nullable or rop.is_nullable):
            if self.phrase.is_positive:
                return EqualityPhrase(lop, rop, self.phrase.expression)
            if self.phrase.is_negative:
                return InequalityPhrase(lop, rop, self.phrase.expression)
        return self.phrase.clone(lop=lop, rop=rop)


class ReduceConnective(Reduce):

    adapts(ConnectivePhraseBase)

    def __call__(self):
        ops = [self.state.reduce(op) for op in self.phrase.ops]
        duplicates = set()
        orig_ops = ops
        ops = []
        for op in orig_ops:
            if op in duplicates:
                continue
            if self.phrase.is_conjunction and isinstance(op, TruePhrase):
                continue
            if self.phrase.is_disjunction and isinstance(op, FalsePhrase):
                continue
            ops.append(op)
            duplicates.add(op)
        if self.phrase.is_conjunction:
            if not ops:
                return TruePhrase(self.phrase.expression)
            if any(isinstance(op, FalsePhrase) for op in ops):
                return FalsePhrase(self.phrase.expression)
        if self.phrase.is_disjunction:
            if not ops:
                return FalsePhrase(self.phrase.expression)
            if any(isinstance(op, TruePhrase) for op in ops):
                return TruePhrase(self.phrase.expression)
        if len(ops) == 1:
            return ops[0]
        return self.phrase.clone(ops=ops)


class ReduceNegation(Reduce):

    adapts(NegationPhrase)

    def __call__(self):
        op = self.state.reduce(self.phrase.op)
        if isinstance(op, NullPhrase):
            return NullPhrase(self.phrase.domain, self.phrase.expression)
        if isinstance(op, TruePhrase):
            return FalsePhrase(self.phrase.expression)
        if isinstance(op, FalsePhrase):
            return TruePhrase(self.phrase.expression)
        if isinstance(op, EqualityPhrase):
            return InequalityPhrase(op.lop, op.rop, self.phrase.expression)
        if isinstance(op, InequalityPhrase):
            return EqualityPhrase(op.lop, op.rop, self.phrase.expression)
        if isinstance(op, TotalEqualityPhrase):
            return TotalInequalityPhrase(op.lop, op.rop,
                                         self.phrase.expression)
        if isinstance(op, TotalInequalityPhrase):
            return TotalEqualityPhrase(op.lop, op.rop, self.phrase.expression)
        if isinstance(op, IsNullPhrase):
            return IsNotNullPhrase(op.op, self.phrase.expression)
        if isinstance(op, IsNotNullPhrase):
            return IsNullPhrase(op.op, self.phrase.expression)
        return self.phrase.clone(op=op)


class ReduceIsNull(Reduce):

    adapts(IsNullPhraseBase)

    def __call__(self):
        op = self.state.reduce(self.phrase.op)
        if isinstance(op, NullPhrase):
            if self.phrase.is_positive:
                return TruePhrase(self.phrase.expression)
            if self.phrase.is_negative:
                return FalsePhrase(self.phrase.expression)
        if not op.is_nullable:
            if self.phrase.is_positive:
                return FalsePhrase(self.phrase.expression)
            if self.phrase.is_negative:
                return TruePhrase(self.phrase.expression)
        return self.phrase.clone(op=op)


class ReduceIfNull(Reduce):

    adapts(IfNullPhrase)

    def __call__(self):
        lop = self.state.reduce(self.phrase.lop)
        rop = self.state.reduce(self.phrase.rop)
        if not lop.is_nullable or isinstance(rop, NullPhrase):
            return lop
        if isinstance(lop, NullPhrase):
            return rop
        return self.phrase.clone(lop=lop, rop=rop)


class ReduceNullIf(Reduce):

    adapts(NullIfPhrase)

    def __call__(self):
        lop = self.state.reduce(self.phrase.lop)
        rop = self.state.reduce(self.phrase.rop)
        if isinstance(lop, NullPhrase) or isinstance(rop, NullPhrase):
            return lop
        if isinstance(lop, LiteralPhrase) and isinstance(rop, LiteralPhrase):
            if lop.value == rop.value:
                return NullPhrase(self.phrase.domain, self.phrase.expression)
            elif isinstance(self.phrase.domain, BooleanDomain):
                return lop
        return self.phrase.clone(lop=lop, rop=rop)


class ReduceCast(Reduce):

    adapts(CastPhrase)

    def __call__(self):
        convert = Convert(self.phrase, self.state)
        return convert()


class Convert(Adapter):

    adapts(Domain, Domain)

    @classmethod
    def dispatch(interface, phrase, *args, **kwds):
        return (type(phrase.base.domain), type(phrase.domain))

    def __init__(self, phrase, state):
        self.phrase = phrase
        self.base = phrase.base
        self.domain = phrase.domain
        self.state = state

    def __call__(self):
        base = self.state.reduce(self.base)
        return self.phrase.clone(base=base)


class ConvertToBoolean(Convert):

    adapts(Domain, BooleanDomain)

    def __call__(self):
        phrase = IsNotNullPhrase(self.base, self.phrase.expression)
        return self.state.reduce(phrase)


class ConvertStringToBoolean(Convert):

    adapts(StringDomain, BooleanDomain)

    def __call__(self):
        if isinstance(self.base, LiteralPhrase):
            if self.base.value is None or self.base.value == '':
                return FalsePhrase(self.phrase.expression)
            else:
                return TruePhrase(self.phrase.expression)
        empty = LiteralPhrase("", coerce(StringDomain()),
                              self.phrase.expression)
        if not self.base.is_nullable:
            phrase = InequalityPhrase(self.base, empty,
                                      self.phrase.expression)
        else:
            phrase = NullIfPhrase(self.base, empty, self.base.domain,
                                  self.phrase.expression)
            phrase = IsNotNullPhrase(phrase, self.phrase.expression)
        return self.state.reduce(phrase)


class ConvertDomainToItself(Convert):

    adapts_many((BooleanDomain, BooleanDomain),
                (IntegerDomain, IntegerDomain),
                (FloatDomain, FloatDomain),
                (DecimalDomain, DecimalDomain),
                (StringDomain, StringDomain),
                (EnumDomain, EnumDomain),
                (DateDomain, DateDomain))

    def __call__(self):
        return self.state.reduce(self.base)


class ReduceExport(Reduce):

    adapts(ExportPhrase)

    def __call__(self):
        return self.clause


class ReduceReference(Reduce):

    adapts(ReferencePhrase)

    def __call__(self):
        if self.clause.tag not in self.state.substitutes:
            return self.clause
        select = self.state.substitutes[self.clause.tag]
        phrase = select[self.clause.index]
        return self.state.reduce(phrase)


def reduce(clause, state=None):
    if state is None:
        state = ReducingState()
    reduce = Reduce(clause, state)
    return reduce()


