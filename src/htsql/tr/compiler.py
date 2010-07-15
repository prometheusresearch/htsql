#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.compile`
=======================

This module implements the compile adapter.
"""


from ..util import listof
from ..adapter import Adapter, adapts, find_adapters
from .code import (Expression, LiteralExpression, EqualityExpression,
                   InequalityExpression, ConjunctionExpression,
                   DisjunctionExpression, NegationExpression, Unit)
from .sketch import (Sketch, LeafSketch, ScalarSketch, BranchSketch,
                     SegmentSketch, QuerySketch, Demand,
                     LeafAppointment, BranchAppointment, FrameAppointment)
from .frame import (LeafFrame, ScalarFrame, BranchFrame, CorrelatedFrame,
                    SegmentFrame, QueryFrame, Link, Phrase, EqualityPhrase,
                    InequalityPhrase, ConjunctionPhrase, DisjunctionPhrase,
                    NegationPhrase, LiteralPhrase,
                    LeafReferencePhrase, BranchReferencePhrase)


class Compiler(object):

    def compile(self, sketch, *args, **kwds):
        compile = Compile(sketch, self)
        return compile.compile(*args, **kwds)

    def evaluate(self, expression, references):
        evaluate = Evaluate(expression, self)
        return evaluate.evaluate(references)


class Compile(Adapter):

    adapts(Sketch, Compiler)

    def __init__(self, sketch, compiler):
        self.sketch = sketch
        self.compiler = compiler

    def compile(self, *args, **kwds):
        raise NotImplementedError()


class CompileLeaf(Compile):

    adapts(LeafSketch, Compiler)

    def compile(self, demands):
        assert isinstance(demands, listof(Demand))
        assert all(demand.sketch in self.sketch.absorbed and
                   isinstance(demand.appointment, (LeafAppointment,
                                                   FrameAppointment))
                   for demand in demands)
        frame = LeafFrame(self.sketch.table, self.sketch.mark)
        supplies = {}
        phrase_by_column = {}
        for demand in demands:
            appointment = demand.appointment
            if appointment.is_leaf:
                column = appointment.column
                if column not in phrase_by_column:
                    phrase = LeafReferencePhrase(frame,
                            self.sketch.is_inner, column, appointment.mark)
                    phrase_by_column[column] = phrase
                supplies[demand] = phrase_by_column[column]
            elif appointment.is_frame:
                supplies[demand] = frame
            else:
                assert False
        return supplies


class CompileScalar(Compile):

    adapts(ScalarSketch, Compiler)

    def compile(self, demands):
        assert isinstance(demands, listof(Demand))
        assert all(demand.sketch in self.sketch.absorbed and
                   isinstance(demand.appointment, FrameAppointment)
                   for demand in demands)
        frame = ScalarFrame(self.sketch.mark)
        supplies = {}
        for demand in demands:
            appointment = demand.appointment
            if appointment.is_frame:
                supplies[demand] = frame
        return supplies


class CompileBranch(Compile):

    adapts(BranchSketch, Compiler)

    def compile(self, demands, BranchFrame=BranchFrame):
        assert isinstance(demands, listof(Demand))
        assert all((demand.sketch in self.sketch.absorbed or
                    demand.sketch in self.sketch.descended)
                   for demand in demands)

        child_by_sketch = {}
        for sketch in self.sketch.absorbed:
            child_by_sketch[sketch] = None
        for attachment in self.sketch.linkage:
            for sketch in attachment.sketch.absorbed:
                child_by_sketch[sketch] = attachment.sketch
            for sketch in attachment.sketch.descended:
                child_by_sketch[sketch] = attachment.sketch

        inner_demands = []
        for demand in demands:
            if demand.sketch not in self.sketch.absorbed:
                inner_demands.append(demand)
            else:
                inner_demands.extend(demand.get_demands())
        for appointment in self.sketch.select:
            inner_demands.extend(appointment.get_demands())
        for attachment in self.sketch.linkage:
            inner_demands.extend(attachment.get_demands())
        for appointment in self.sketch.filter:
            inner_demands.extend(appointment.get_demands())
        for appointment in self.sketch.group:
            inner_demands.extend(appointment.get_demands())
        for appointment in self.sketch.group_filter:
            inner_demands.extend(appointment.get_demands())
        for appointment, dir in self.sketch.order:
            inner_demands.extend(appointment.get_demands())

        idx = 0
        while idx < len(inner_demands):
            demand = inner_demands[idx]
            if demand.sketch in self.sketch.absorbed:
                inner_demands.extend(demand.get_demands())
            idx += 1

        demands_by_child = {}
        demands_by_child[None] = []
        for attachment in self.sketch.linkage:
            demands_by_child[attachment.sketch] = []
        for demand in inner_demands:
            child = child_by_sketch[demand.sketch]
            demands_by_child[child].append(demand)

        inner_supplies = {}
        for attachment in self.sketch.linkage:
            child = attachment.sketch
            child_demands = demands_by_child[child]
            child_supplies = self.compiler.compile(child, child_demands)
            inner_supplies.update(child_supplies)

        branch_demands = reversed(demands_by_child[None])
        for demand in branch_demands:
            phrase = self.meet(demand.appointment, inner_supplies)
            inner_supplies[demand] = phrase

        select = []
        phrase_by_expression = {}
        for appointment in self.sketch.select:
            if appointment.expression not in phrase_by_expression:
                phrase = self.meet(appointment, inner_supplies)
                phrase_by_expression[appointment.expression] = phrase
            phrase = phrase_by_expression[appointment.expression]
            select.append(phrase)
        position_by_demand = {}
        position_by_phrase = {}
        for demand in demands:
            if demand.appointment.is_frame:
                continue
            if demand.sketch in self.sketch.absorbed:
                appointment = demand.appointment
                if appointment.expression in phrase_by_expression:
                    phrase = phrase_by_expression[appointment.expression]
                else:
                    phrase = self.meet(appointment, inner_supplies)
                    phrase_by_expression[appointment.expression] = phrase
            else:
                phrase = inner_supplies[demand]
            if phrase not in position_by_phrase:
                position_by_phrase[phrase] = len(select)
                select.append(phrase)
            position_by_demand[demand] = position_by_phrase[phrase]

        linkage = []
        for attachment in self.sketch.linkage:
            if not attachment.sketch.is_proper:
                continue
            link = self.link(attachment, inner_supplies)
            linkage.append(link)

        filter = None
        conditions = []
        for appointment in self.sketch.filter:
            phrase = self.meet(appointment, inner_supplies)
            conditions.append(phrase)
        if len(conditions) == 1:
            filter = conditions[0]
        elif len(conditions) > 1:
            filter = ConjunctionPhrase(conditions, self.sketch.mark)

        group = []
        for appointment in self.sketch.group:
            if appointment.expression in phrase_by_expression:
                phrase = phrase_by_expression[appointment.expression]
            else:
                phrase = self.meet(appointment, inner_supplies)
            group.append(phrase)

        group_filter = None
        conditions = []
        for appointment in self.sketch.group_filter:
            phrase = self.meet(appointment, inner_supplies)
            conditions.append(phrase)
        if len(conditions) == 1:
            group_filter = conditions[0]
        elif len(conditions) > 1:
            group_filter = ConjunctionPhrase(conditions, self.sketch.mark)

        order = []
        for appointment, dir in self.sketch.order:
            if appointment.expression in phrase_by_expression:
                phrase = phrase_by_expression[appointment.expression]
            else:
                phrase = self.meet(appointment, inner_supplies)
            order.append((phrase, dir))

        limit = self.sketch.limit
        offset = self.sketch.offset

        frame = BranchFrame(select=select,
                            linkage=linkage,
                            filter=filter,
                            group=group,
                            group_filter=group_filter,
                            order=order,
                            limit=limit,
                            offset=offset,
                            mark=self.sketch.mark)

        supplies = {}
        reference_by_position = {}
        for demand in demands:
            if demand.appointment.is_frame:
                supplies[demand] = frame
            else:
                position = position_by_demand[demand]
                if position in reference_by_position:
                    phrase = reference_by_position[position]
                else:
                    mark = select[position].mark
                    phrase = BranchReferencePhrase(frame,
                            self.sketch.is_inner, position, mark)
                    reference_by_position[position] = phrase
                supplies[demand] = phrase

        return supplies

    def meet(self, appointment, inner_supplies):
        references = {}
        for unit in appointment.expression.get_units():
            demand = appointment.demand_by_unit[unit]
            phrase = inner_supplies[demand]
            references[unit] = phrase
        return self.compiler.evaluate(appointment.expression, references)

    def link(self, attachment, inner_supplies):
        frame = inner_supplies[attachment.demand]
        conditions = []
        for connection in attachment.connections:
            left = self.meet(connection.left, inner_supplies)
            right = self.meet(connection.right, inner_supplies)
            condition = EqualityPhrase(left, right, attachment.sketch.mark)
            conditions.append(condition)
        condition = None
        if len(conditions) == 1:
            condition = conditions[0]
        elif len(conditions) > 1:
            condition = ConjunctionPhrase(conditions, attachment.sketch.mark)
        is_inner = attachment.sketch.is_inner
        return Link(frame, condition, is_inner)


class CompileSegment(Compile):

    adapts(SegmentSketch, Compiler)

    def compile(self):
        appointment = FrameAppointment(self.sketch.mark)
        demand = Demand(self.sketch, appointment)
        supplies = super(CompileSegment, self).compile([demand],
                                        BranchFrame=SegmentFrame)
        frame = supplies[demand]
        return frame


class CompileQuery(Compile):

    adapts(QuerySketch, Compiler)

    def compile(self):
        segment = None
        if self.sketch.segment is not None:
            segment = self.compiler.compile(self.sketch.segment)
        return QueryFrame(self.sketch, segment, self.sketch.mark)


class Evaluate(Adapter):

    adapts(Expression, Compiler)

    def __init__(self, expression, compiler):
        self.expression = expression
        self.compiler = compiler

    def evaluate(self, references):
        raise NotImplementedError(self.expression)


class EvaluateLiteral(Evaluate):

    adapts(LiteralExpression, Compiler)

    def evaluate(self, references):
        return LiteralPhrase(self.expression.value,
                             self.expression.domain,
                             self.expression.mark)


class EvaluateEquality(Evaluate):

    adapts(EqualityExpression, Compiler)

    def evaluate(self, references):
        left = self.compiler.evaluate(self.expression.left, references)
        right = self.compiler.evaluate(self.expression.right, references)
        return EqualityPhrase(left, right, self.expression.mark)


class EvaluateInequality(Evaluate):

    adapts(InequalityExpression, Compiler)

    def evaluate(self, references):
        left = self.compiler.evaluate(self.expression.left, references)
        right = self.compiler.evaluate(self.expression.right, references)
        return InequalityPhrase(left, right, self.expression.mark)


class EvaluateConjunction(Evaluate):

    adapts(ConjunctionExpression, Compiler)

    def evaluate(self, references):
        terms = [self.compiler.evaluate(term, references)
                 for term in self.expression.terms]
        return ConjunctionPhrase(terms, self.expression.mark)


class EvaluateDisjunction(Evaluate):

    adapts(DisjunctionExpression, Compiler)

    def evaluate(self, references):
        terms = [self.compiler.evaluate(term, references)
                 for term in self.expression.terms]
        return DisjunctionPhrase(terms, self.expression.mark)


class EvaluateUnit(Evaluate):

    adapts(Unit, Compiler)

    def evaluate(self, references):
        return references[self.expression]


compile_adapters = find_adapters()


