#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.assemble`
========================

This module implements the assembling process.
"""


from ..util import listof, Node, Comparable
from ..adapter import Adapter, adapts, adapts_many
from ..domain import BooleanDomain
from .coerce import coerce
from .code import (Code, LiteralCode, EqualityCode, TotalEqualityCode,
                   ConjunctionCode, DisjunctionCode, NegationCode,
                   CastCode, Unit, ColumnUnit)
from .term import (Term, RoutingTerm, UnaryTerm, BinaryTerm, TableTerm,
                   ScalarTerm, FilterTerm, JoinTerm, CorrelationTerm,
                   EmbeddingTerm, ProjectionTerm, OrderTerm, SegmentTerm,
                   QueryTerm)
from .frame import (LeafFrame, ScalarFrame, TableFrame, BranchFrame,
                    NestedFrame, SegmentFrame, QueryFrame,
                    Phrase, LiteralPhrase, NullPhrase, TruePhrase, FalsePhrase,
                    EqualityPhrase, TotalEqualityPhrase,
                    ConjunctionPhrase, DisjunctionPhrase, NegationPhrase,
                    CastPhrase, ColumnLink, ReferenceLink, EmbeddingLink,
                    Anchor)


class Claim(Comparable, Node):

    def __init__(self, unit, broker, target):
        assert isinstance(unit, Unit)
        assert isinstance(broker, int)
        assert isinstance(target, int)
        equality_vector = (unit, broker, target)
        super(Claim, self).__init__(equality_vector)
        self.unit = unit
        self.broker = broker
        self.target = target

    def __str__(self):
        return "(%s)->%s->%s" % (self.unit, self.broker, self.target)


class Gate(object):

    def __init__(self, term, is_inner, routes):
        assert isinstance(term, RoutingTerm)
        assert isinstance(is_inner, bool)
        assert isinstance(routes, dict)
        self.term = term
        self.is_inner = is_inner
        self.routes = routes


class AssemblingState(object):

    def __init__(self):
        self.gate_stack = []
        self.gate = None
        self.claims_by_broker = None
        self.claim_set = None
        self.phrase_by_claim = None

    def set_tree(self, term):
        assert isinstance(term, SegmentTerm)
        self.gate = Gate(term, True, term.routes)
        self.claim_set = set()
        self.claims_by_broker = {}
        self.phrase_by_claim = {}
        self.claims_by_broker[term.tag] = []
        for offspring_tag in term.offsprings:
            self.claims_by_broker[offspring_tag] = []

    def unset_tree(self):
        self.gate = None
        self.claim_set = None
        self.claims_by_broker = None
        self.phrase_by_claim = None

    def push_gate(self, term=None, is_inner=None, router=None):
        if term is None:
            term = self.gate.term
        if is_inner is None:
            is_inner = self.gate.is_inner
        if router is None:
            router = term
        self.gate_stack.append(self.gate)
        self.gate = Gate(term, is_inner, router.routes)

    def pop_gate(self):
        self.gate = self.gate_stack.pop()

    def assemble(self, term):
        return assemble(term, self)

    def appoint(self, unit):
        if unit.is_primitive:
            assert unit.space in self.gate.routes
            target = self.gate.routes[unit.space]
        if unit.is_compound:
            assert unit in self.gate.routes
            target = self.gate.routes[unit]
        assert target in self.gate.term.offsprings
        broker = self.gate.term.offsprings[target].tag
        return Claim(unit, broker, target)

    def forward(self, claim):
        assert claim.target in self.gate.term.offsprings
        broker = self.gate.term.offsprings[claim.target].tag
        return Claim(claim.unit, broker, claim.target)

    def schedule(self, code, router=None):
        if router is not None:
            self.push_gate(router=router)
        for unit in code.units:
            claim = self.appoint(unit)
            self.demand(claim)
        if router is not None:
            self.pop_gate()

    def evaluate(self, code, router=None):
        if router is not None:
            self.push_gate(router=router)
        evaluate = Evaluate(code, self)
        phrase = evaluate()
        if router is not None:
            self.pop_gate()
        return phrase

    def demand(self, claim):
        if not claim in self.claim_set:
            self.claim_set.add(claim)
            self.claims_by_broker[claim.broker].append(claim)

    def supply(self, claim, phrase):
        assert claim in self.claim_set
        assert claim not in self.phrase_by_claim
        self.phrase_by_claim[claim] = phrase


class Assemble(Adapter):

    adapts(Term)

    def __init__(self, term, state):
        self.term = term
        self.state = state

    def __call__(self):
        raise NotImplementedError()


class AssembleRouting(Assemble):

    adapts(RoutingTerm)

    def __init__(self, term, state):
        super(AssembleRouting, self).__init__(term, state)
        self.claims = state.claims_by_broker[term.tag]


class AssembleScalar(Assemble):

    adapts(ScalarTerm)

    def __call__(self):
        assert not self.claims
        return ScalarFrame(self.term)


class AssembleTable(Assemble):

    adapts(TableTerm)

    def __call__(self):
        table = self.term.table
        for claim in self.claims:
            assert claim.broker == self.term.tag
            assert claim.target == self.term.tag
            assert claim.unit.is_primitive
            assert isinstance(claim.unit, ColumnUnit)
            column = claim.unit.column
            assert (column.schema_name == table.schema_name and
                    column.table_name == table.name)
            is_nullable = (column.is_nullable or not self.state.gate.is_inner)
            link = ColumnLink(self.term.tag, column, is_nullable, claim.unit)
            self.state.supply(claim, link)
        return TableFrame(table, self.term)


class AssembleBranch(Assemble):

    adapts_many(UnaryTerm, BinaryTerm)

    def delegate(self):
        for claim in self.claims:
            assert claim.broker == self.term.tag
            if claim.target != self.term.tag:
                next_claim = self.state.forward(claim)
                self.state.demand(next_claim)
            else:
                assert claim.unit.is_compound
                self.state.schedule(claim.unit.code)

    def assemble_include(self):
        return []

    def assemble_embed(self):
        return []

    def assemble_select(self):
        select = []
        index_by_phrase = {}
        for claim in self.claims:
            if claim.target != self.term.tag:
                next_claim = self.state.forward(claim)
                assert next_claim in self.state.phrase_by_claim
                phrase = self.state.phrase_by_claim[next_claim]
            else:
                phrase = self.state.evaluate(claim.unit.code)
            if phrase not in index_by_phrase:
                index = len(select)
                select.append(phrase)
                index_by_phrase[phrase] = index
            index = index_by_phrase[phrase]
            domain = phrase.domain
            is_nullable = (phrase.is_nullable or not self.state.gate.is_inner)
            link = ReferenceLink(self.term.tag, index, domain, is_nullable,
                                 claim.unit)
            self.state.supply(claim, link)
        if not select:
            select = [TruePhrase(self.term.expression)]
        return select

    def assemble_where(self):
        return None

    def assemble_group(self):
        return []

    def assemble_having(self):
        return None

    def assemble_order(self):
        return []

    def assemble_limit(self):
        return None

    def assemble_offset(self):
        return None

    def assemble_frame(self, include, embed, select, where, group, having,
                      order, limit, offset):
        return NestedFrame(include, embed, select, where, group, having,
                           order, limit, offset, self.term)

    def __call__(self):
        self.delegate()
        include = self.assemble_include()
        embed = self.assemble_embed()
        select = self.assemble_select()
        where = self.assemble_where()
        group = self.assemble_group()
        having = self.assemble_having()
        order = self.assemble_order()
        limit = self.assemble_limit()
        offset = self.assemble_offset()
        return self.assemble_frame(include, embed, select, where, group, having,
                                  order, limit, offset)


class AssembleUnary(AssembleBranch):

    adapts(UnaryTerm)

    def assemble_include(self):
        include = []
        self.state.push_gate(self.term.kid, is_inner=True)
        frame = self.state.assemble(self.term.kid)
        anchor = Anchor(frame, None, False, False, self.term.kid.expression)
        include.append(anchor)
        self.state.pop_gate()
        return include


class AssembleFilter(Assemble):

    adapts(FilterTerm)

    def delegate(self):
        super(AssembleFilter, self).delegate()
        self.state.schedule(self.term.filter,
                            router=self.term.kid)

    def assemble_where(self):
        return self.state.evaluate(self.term.filter,
                                   router=self.term.kid)


class AssembleOrder(Assemble):

    adapts(OrderTerm)

    def delegate(self):
        super(AssembleOrder, self).delegate()
        for code, direction in self.term.order:
            self.state.schedule(code, router=self.term.kid)

    def assemble_order(self):
        order = []
        used_phrases = set()
        for code, direction in self.term.order:
            if not code.units:
                continue
            phrase = self.state.evaluate(code, router=self.term.kid)
            if phrase in used_phrases:
                continue
            order.append((phrase, direction))
            used_phrases.add(phrase)
        return order

    def assemble_limit(self):
        return self.term.limit

    def assemble_offset(self):
        return self.term.offset


class AssembleProjection(Assemble):

    adapts(ProjectionTerm)

    def delegate(self):
        self.state.push_gate(router=self.term.kid)
        super(AssembleProjection, self).delegate()
        for code in self.term.kernel:
            self.state.schedule(code)
        self.state.pop_gate()

    def assemble_select(self):
        self.state.push_gate(router=self.term.kid)
        select = super(AssembleProjection, self).assemble_select()
        self.state.pop_gate()
        return select

    def assemble_group(self):
        group = []
        used_phrases = set()
        for code in self.term.kernel:
            if not code.units:
                continue
            phrase = self.state.evaluate(code, router=self.term.kid)
            if phrase in used_phrases:
                continue
            group.append(phrase)
            used_phrases.add(phrase)
        return group


class AssembleJoin(Assemble):

    adapts(JoinTerm)

    def delegate(self):
        super(AssembleJoin, self).delegate()
        for lop, rop in self.term.joints:
            self.state.schedule(lop, router=self.term.lkid)
            self.state.schedule(rop, router=self.term.rkid)

    def assemble_include(self):
        self.state.push_gate(self.term.lkid, is_inner=(not self.term.is_right))
        lframe = self.state.assemble(self.term.lkid)
        lanchor = Anchor(lframe, None, False, False, self.term.lkid.expression)
        self.state.pop_gate()
        self.state.push_gate(self.term.rkid, is_inner=(not self.term.is_left))
        rframe = self.state.assemble(self.term.rkid)
        self.state.pop_gate()
        equalities = []
        for lop, rop in self.term.joints:
            lop = self.state.evaluate(lop, router=self.term.lkid)
            rop = self.state.evaluate(rop, router=self.term.rkid)
            equality = EqualityPhrase(lop, rop, self.term.expression)
            equalities.append(equality)
        condition = None
        if equalities:
            condition = ConjunctionPhrase(equalities, self.term.expression)
        ranchor = Anchor(rframe, condition,
                         self.term.is_left,
                         self.term.is_right,
                         self.term.expression)
        return [lanchor, ranchor]


class AssembleEmbedding(Assemble):

    adapts(EmbeddingTerm)

    def delegate(self):
        super(AssembleEmbedding, self).delegate()
        correlation = self.term.rkid
        for lop, rop in correlation.joints:
            self.state.schedule(lop, router=correlation.link)
            self.state.push_gate(correlation)
            self.state.schedule(rop)
            self.state.pop_gate()

    def assemble_include(self):
        self.state.push_gate(self.term.lkid, is_inner=True)
        frame = self.state.assemble(self.term.lkid)
        anchor = Anchor(frame, None, False, False, self.term.lkid.expression)
        self.state.pop_gate()
        return [anchor]

    def assemble_embed(self):
        self.state.push_gate(self.term.rkid, is_inner=False)
        frame = self.state.assemble(self.term.rkid)
        self.state.pop_gate()
        return [frame]


class AssembleCorrelation(Assemble):

    adapts(CorrelationTerm)

    def delegate(self):
        assert len(self.claims) == 1
        claim = self.claims[0]
        assert claim.target == self.term.tag
        assert claim.unit.is_compound
        self.state.schedule(claim.unit.code)

    def assemble_select(self):
        claim = self.claims[0]
        phrase = self.state.evaluate(claim.unit.code)
        domain = phrase.domain
        is_nullable = True
        link = EmbeddingLink(self.term.tag, domain, is_nullable, claim.unit)
        self.state.supply(claim, link)
        return [phrase]

    def assemble_where(self):
        equalities = []
        for lop, rop in self.term.joints:
            self.state.pop_gate()
            lop = self.state.evaluate(lop, router=self.term.link)
            self.state.push_gate(self.term, is_inner=False)
            rop = self.state.evaluate(rop)
            equality = EqualityPhrase(lop, rop, self.term.expression)
            equalities.append(equality)
        condition = None
        if equalities:
            condition = ConjunctionPhrase(equalities, self.term.expression)
        return condition


class AssembleSegment(Assemble):

    adapts(SegmentTerm)

    def delegate(self):
        assert not self.claims
        for element in self.term.elements:
            self.state.schedule(element)

    def assemble_select(self):
        select = []
        for element in self.term.elements:
            phrase = self.state.evaluate(element)
            select.append(phrase)
        return select

    def assemble_frame(self, include, embed, select, where, group, having,
                      order, limit, offset):
        return SegmentFrame(include, embed, select, where, group, having,
                            order, limit, offset, self.term)


class AssembleQuery(Assemble):

    adapts(QueryTerm)

    def __call__(self):
        segment = None
        if self.term.segment is not None:
            self.state.set_tree(self.term.segment)
            segment = self.state.assemble(self.term.segment)
            self.state.unset_tree()
        return QueryFrame(segment, self.term)


class Evaluate(Adapter):

    adapts(Code)

    def __init__(self, code, state):
        self.code = code
        self.state = state

    def __call__(self):
        raise NotImplementedError()


class EvaluateLiteral(Evaluate):

    adapts(LiteralCode)

    def __call__(self):
        return LiteralPhrase(self.code.value, self.code.domain, self.code)


class EvaluateEquality(Evaluate):

    adapts(EqualityCode)

    def __call__(self):
        lop = self.state.evaluate(self.code.lop)
        rop = self.state.evaluate(self.code.rop)
        return EqualityPhrase(lop, rop, self.code)


class EvaluateTotalEquality(Evaluate):

    adapts(TotalEqualityCode)

    def __call__(self):
        lop = self.state.evaluate(self.code.lop)
        rop = self.state.evaluate(self.code.rop)
        return TotalEqualityPhrase(lop, rop, self.code)


class EvaluateConjunction(Evaluate):

    adapts(ConjunctionCode)

    def __call__(self):
        ops = [self.state.evaluate(op) for op in self.code.ops]
        return ConjunctionPhrase(ops, self.code)


class EvaluateDisjunction(Evaluate):

    adapts(DisjunctionCode)

    def __call__(self):
        ops = [self.state.evaluate(op) for op in self.code.ops]
        return DisjunctionPhrase(ops, self.code)


class EvaluateNegation(Evaluate):

    adapts(NegationCode)

    def __call__(self):
        op = self.state.evaluate(self.code.op)
        return NegationPhrase(op, self.code)


class EvaluateCast(Evaluate):

    adapts(CastCode)

    def __call__(self):
        base = self.state.evaluate(self.code.base)
        return CastPhrase(base, self.code.domain, base.is_nullable, self.code)


class EvaluateUnit(Evaluate):

    adapts(Unit)

    def __call__(self):
        claim = self.state.appoint(self.code)
        assert claim in self.state.phrase_by_claim
        return self.state.phrase_by_claim[claim]


def assemble(term, state=None):
    if state is None:
        state = AssemblingState()
    assemble = Assemble(term, state)
    return assemble()


