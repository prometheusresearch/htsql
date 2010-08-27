#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.outline`
=======================

This module implements outline adapters.
"""


from ..adapter import Adapter, adapts
from .term import (Term, TableTerm, ScalarTerm, FilterTerm, JoinTerm,
                   CorrelationTerm, ProjectionTerm, OrderingTerm, HangingTerm,
                   SegmentTerm, QueryTerm, Tie, ParallelTie, SeriesTie)
from .code import (Unit, ColumnUnit, AggregateUnit, CorrelatedUnit,
                   Space, ScalarSpace, FreeTableSpace, JoinedTableSpace)
from .sketch import (Sketch, LeafSketch, ScalarSketch, BranchSketch,
                     SegmentSketch, QuerySketch, Demand, LeafAppointment,
                     BranchAppointment, Connection, Attachment)


class Outliner(object):

    def outline(self, sketch, *args, **kwds):
        outline = Outline(sketch, self)
        return outline.outline(*args, **kwds)

    def delegate(self, unit, sketch, term):
        delegate = Delegate(unit, self)
        return delegate.delegate(sketch, term)

    def appoint(self, expression, sketch, term):
        demand_by_unit = {}
        for unit in expression.get_units():
            demand = self.delegate(unit, sketch, term)
            demand_by_unit[unit] = demand
        return BranchAppointment(expression, demand_by_unit)

    def connect(self, tie):
        connect = Connect(tie.space, self, tie)
        return connect.connect()

    def flatten(self, sketch):
        flatten = Flatten(sketch, self)
        return flatten.flatten()


class Outline(Adapter):

    adapts(Term, Outliner)

    def __init__(self, term, outliner):
        self.term = term
        self.outliner = outliner

    def outline(self, is_inner=True, is_proper=True):
        raise NotImplementedError()
    

class OutlineTable(Outline):

    adapts(TableTerm, Outliner)

    def outline(self, is_inner=True):
        return LeafSketch(self.term.table,
                          is_inner=is_inner,
                          mark=self.term.mark)


class OutlineScalar(Outline):

    adapts(ScalarTerm, Outliner)

    def outline(self, is_inner=True):
        return ScalarSketch(is_inner=is_inner,
                            mark=self.term.mark)


class OutlineFilter(Outline):

    adapts(FilterTerm, Outliner)

    def outline(self, is_inner=True, is_proper=True):
        child = self.outliner.outline(self.term.child)
        attachment = Attachment(child)
        linkage = [attachment]
        appointment = self.outliner.appoint(self.term.filter, child,
                                            self.term.child)
        filter = [appointment]
        return BranchSketch(linkage=linkage,
                            filter=filter,
                            is_inner=is_inner,
                            is_proper=is_proper,
                            mark=self.term.mark)


class OutlineJoin(Outline):

    adapts(JoinTerm, Outliner)

    def outline(self, is_inner=True, is_proper=True):
        left_child = self.outliner.outline(self.term.left_child)
        left_attachment = Attachment(left_child)
        right_child = self.outliner.outline(self.term.right_child,
                                            is_inner=self.term.is_inner)
        connections = []
        for tie in self.term.ties:
            for left_unit, right_unit in self.outliner.connect(tie):
                left_appointment = self.outliner.appoint(left_unit,
                                    left_child, self.term.left_child)
                right_appointment = self.outliner.appoint(right_unit,
                                    right_child, self.term.right_child)
                connection = Connection(left_appointment, right_appointment)
                connections.append(connection)
        right_attachment = Attachment(right_child, connections)
        linkage = [left_attachment, right_attachment]
        return BranchSketch(linkage=linkage,
                            is_inner=is_inner,
                            is_proper=is_proper,
                            mark=self.term.mark)


class OutlineCorrelation(Outline):

    adapts(CorrelationTerm, Outliner)

    def outline(self, is_inner=True, is_proper=True):
        left_child = self.outliner.outline(self.term.left_child)
        left_attachment = Attachment(left_child)
        right_child = self.outliner.outline(self.term.right_child,
                                            is_inner=False,
                                            is_proper=False)
        connections = []
        for tie in self.term.ties:
            for left_code, right_code in self.outliner.connect(tie):
                left_appointment = self.outliner.appoint(left_code,
                                    left_child, self.term.left_child)
                right_appointment = self.outliner.appoint(right_code,
                                    right_child, self.term.right_child)
                connection = Connection(left_appointment, right_appointment)
                connections.append(connection)
        right_attachment = Attachment(right_child, connections)
        linkage = [left_attachment, right_attachment]
        return BranchSketch(linkage=linkage,
                            is_inner=is_inner,
                            is_proper=is_proper,
                            mark=self.term.mark)


class OutlineProjection(Outline):

    adapts(ProjectionTerm, Outliner)

    def outline(self, is_inner=True, is_proper=True):
        child = self.outliner.outline(self.term.child)
        attachment = Attachment(child)
        linkage = [attachment]
        group = []
        for tie in self.term.ties:
            for left_code, right_code in self.outliner.connect(tie):
                appointment = self.outliner.appoint(right_code, child,
                                                    self.term.child)
                group.append(appointment)
        return BranchSketch(linkage=linkage,
                            group=group,
                            is_inner=is_inner,
                            is_proper=is_proper,
                            mark=self.term.mark)


class OutlineOrdering(Outline):

    adapts(OrderingTerm, Outliner)

    def outline(self, is_inner=True, is_proper=True):
        child = self.outliner.outline(self.term.child)
        attachment = Attachment(child)
        linkage = [attachment]
        order = []
        for code, dir in self.term.order:
            appointment = self.outliner.appoint(code, child, self.term.child)
            order.append((appointment, dir))
        return BranchSketch(linkage=linkage,
                            order=order,
                            limit=self.term.limit,
                            offset=self.term.offset,
                            is_inner=is_inner,
                            is_proper=is_proper,
                            mark=self.term.mark)


class OutlineHanging(Outline):

    adapts(HangingTerm, Outliner)

    def outline(self, is_inner=True, is_proper=True):
        child = self.outliner.outline(self.term.child)
        attachment = Attachment(child)
        linkage = [attachment]
        return BranchSketch(linkage=linkage,
                            is_inner=is_inner,
                            is_proper=is_proper,
                            mark=self.term.mark)


class OutlineSegment(Outline):

    adapts(SegmentTerm, Outliner)

    def outline(self):
        child = self.outliner.outline(self.term.child)
        attachment = Attachment(child)
        linkage = [attachment]
        select = []
        for code in self.term.select:
            appointment = self.outliner.appoint(code, child, self.term.child)
            select.append(appointment)
        return SegmentSketch(select=select,
                             linkage=linkage,
                             mark=self.term.mark)


class OutlineQuery(Outline):

    adapts(QueryTerm, Outliner)

    def outline(self):
        segment = None
        if self.term.segment is not None:
            segment = self.outliner.outline(self.term.segment)
            segment = self.outliner.flatten(segment)
        return QuerySketch(term=self.term,
                           segment=segment,
                           mark=self.term.mark)


class Delegate(Adapter):

    adapts(Unit, Outliner)

    def __init__(self, unit, outliner):
        self.unit = unit
        self.outliner = outliner

    def delegate(self, sketch, term):
        raise NotImplementedError()


class DelegateColumn(Delegate):

    adapts(ColumnUnit, Outliner)

    def delegate(self, sketch, term):
        route = term.routes[self.unit.space]
        for idx in route:
            sketch = sketch.linkage[idx].sketch
        appointment = LeafAppointment(self.unit.column,
                                      self.unit.mark)
        return Demand(sketch, appointment)


class DelegateAggregate(Delegate):

    adapts(AggregateUnit, Outliner)

    def delegate(self, sketch, term):
        route = term.routes[self.unit]
        for idx in route:
            sketch = sketch.linkage[idx].sketch
            term = term.children[idx]
        appointment = self.outliner.appoint(self.unit.expression,
                                            sketch.linkage[0].sketch,
                                            term.children[0])
        return Demand(sketch, appointment)


class DelegateCorrelated(Delegate):

    adapts(CorrelatedUnit, Outliner)

    def delegate(self, sketch, term):
        route = term.routes[self.unit]
        for idx in route:
            sketch = sketch.linkage[idx].sketch
            term = term.children[idx]
        appointment = self.outliner.appoint(self.unit.expression,
                                            sketch, term)
        return Demand(sketch, appointment)


class Flatten(Adapter):

    adapts(Sketch, Outliner)

    def __init__(self, sketch, outliner):
        self.sketch = sketch
        self.outliner = outliner

    def flatten(self):
        return self.sketch


class FlattenBranch(Flatten):

    adapts(BranchSketch, Outliner)

    def flatten(self):
        linkage = []
        for attachment in self.sketch.linkage:
            replacement = self.outliner.flatten(attachment.sketch)
            if replacement is not attachment.sketch:
                attachment = attachment.clone(sketch=replacement)
            linkage.append(attachment)
        replaced = self.sketch.replaced[:]
        replaced.append(self.sketch)
        if (len(linkage) > 1 and linkage[0].sketch.is_scalar
                             and linkage[1].sketch.is_inner):
            linkage = linkage[1:]
        sketch = self.sketch
        if linkage != self.sketch.linkage:
            sketch = self.sketch.clone(linkage=linkage, replaced=replaced)
        if not sketch.linkage:
            return sketch
        child = sketch.linkage[0].sketch
        if not child.is_branch:
            return sketch
        if child.select or child.group or child.group_filter:
            return sketch
        if ((child.order or child.limit is not None or
            child.offset is not None) and (sketch.order or
                sketch.limit is not None or
                sketch.offset is not None)):
            return sketch
        select = sketch.select
        linkage = child.linkage + sketch.linkage[1:]
        filter = child.filter + sketch.filter
        group = sketch.group
        group_filter = sketch.group_filter
        order = child.order + sketch.order
        limit = child.limit
        if sketch.limit is not None:
            limit = sketch.limit
        offset = child.offset
        if sketch.offset is not None:
            offset = sketch.offset
        replaced.append(child)
        replaced.extend(child.replaced)
        return sketch.clone(select=select,
                            linkage=linkage,
                            filter=filter,
                            group=group,
                            group_filter=group_filter,
                            order=order,
                            limit=limit,
                            offset=offset,
                            replaced=replaced)


class Connect(Adapter):

    adapts(Space, Outliner)

    def __init__(self, space, outliner, tie):
        self.space = space
        self.outliner = outliner
        self.tie = tie

    def connect(self):
        if self.tie.is_parallel:
            return self.connect_parallel()
        if self.tie.is_series:
            return self.connect_series()

    def connect_parallel(self):
        raise NotImplementedError()

    def connect_series(self):
        raise NotImplementedError()


class ConnectScalar(Connect):

    adapts(ScalarSpace, Outliner)

    def connect_parallel(self):
        return []


class ConnectFreeTable(Connect):

    adapts(FreeTableSpace, Outliner)

    def connect_parallel(self):
        table = self.tie.space.table
        if table.primary_key is None:
            raise InvalidArgumentError()
        for name in table.primary_key.origin_column_names:
            column = table.columns[name]
            code = ColumnUnit(column, self.tie.space, self.tie.mark)
            yield (code, code)

    def connect_series(self):
        return []


class ConnectJoinedTable(Connect):

    adapts(JoinedTableSpace, Outliner)

    def connect_parallel(self):
        table = self.tie.space.join.target
        if table.primary_key is None:
            raise InvalidArgumentError()
        for name in table.primary_key.origin_column_names:
            column = table.columns[name]
            code = ColumnUnit(column, self.tie.space, self.tie.mark)
            yield (code, code)

    def connect_series(self):
        join = self.tie.space.join
        left_codes = []
        right_codes = []
        for column in join.origin_columns:
            code = ColumnUnit(column, self.tie.space.parent, self.tie.mark)
            left_codes.append(code)
        for column in join.target_columns:
            code = ColumnUnit(column, self.tie.space, self.tie.mark)
            right_codes.append(code)
        if self.tie.is_reverse:
            left_codes, right_codes = right_codes, left_codes
        return zip(left_codes, right_codes)


