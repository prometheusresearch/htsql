#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.sketch`
======================

This module declares sketch nodes.
"""


from ..util import listof, tupleof, dictof, maybe, Node
from ..mark import Mark
from ..domain import Domain
from ..entity import TableEntity, ColumnEntity
from .code import Expression, Unit
from .term import QueryTerm


class Sketch(Node):

    is_leaf = False
    is_scalar = False
    is_branch = False
    is_segment = False
    is_query = False

    def __init__(self, mark):
        assert isinstance(mark, Mark)
        self.mark = mark


class AttachedSketch(Sketch):

    def __init__(self, is_inner=True, is_proper=True,
                 replaced=[], mark=None):
        assert isinstance(is_inner, bool)
        assert isinstance(is_proper, bool)
        assert isinstance(replaced, listof(Sketch))
        super(AttachedSketch, self).__init__(mark)
        self.is_inner = is_inner
        self.is_proper = is_proper
        self.replaced = replaced
        self.absorbed = set([self]+replaced)
        for sketch in replaced:
            self.absorbed |= sketch.absorbed
        self.descended = set()
        self.mark = mark


class LeafSketch(AttachedSketch):

    is_leaf = True

    def __init__(self, table, is_inner=True, replaced=[], mark=None):
        assert isinstance(table, TableEntity)
        super(LeafSketch, self).__init__(is_inner=is_inner,
                                         replaced=replaced,
                                         mark=mark)
        self.table = table


class ScalarSketch(AttachedSketch):

    is_scalar = True


class BranchSketch(AttachedSketch):

    is_branch = True

    def __init__(self, select=[], linkage=[], filter=[],
                 group=[], group_filter=[], order=[],
                 limit=None, offset=None,
                 is_inner=True, is_proper=True,
                 replaced=[], mark=None):
        assert isinstance(select, listof(BranchAppointment))
        assert isinstance(linkage, listof(Attachment))
        assert isinstance(filter, listof(BranchAppointment))
        assert isinstance(group, listof(BranchAppointment))
        assert isinstance(group_filter, listof(BranchAppointment))
        assert isinstance(order, listof(tupleof(BranchAppointment, int)))
        assert isinstance(limit, maybe(int))
        assert isinstance(offset, maybe(int))
        super(BranchSketch, self).__init__(is_inner=is_inner,
                                           is_proper=is_proper,
                                           replaced=replaced,
                                           mark=mark)
        self.select = select
        self.linkage = linkage
        self.filter = filter
        self.group = group
        self.group_filter = group_filter
        self.order = order
        self.limit = limit
        self.offset = offset
        for attachment in linkage:
            self.descended |= attachment.sketch.absorbed
            self.descended |= attachment.sketch.descended


class SegmentSketch(BranchSketch):

    is_segment = True


class QuerySketch(Sketch):

    is_query = True

    def __init__(self, term, segment, mark):
        assert isinstance(term, QueryTerm)
        assert isinstance(segment, maybe(SegmentSketch))
        super(QuerySketch, self).__init__(mark)
        self.term = term
        self.code = term.code
        self.binding = term.binding
        self.syntax = term.syntax
        self.segment = segment


class Demand(Node):

    def __init__(self, sketch, appointment):
        assert isinstance(sketch, Sketch)
        assert isinstance(appointment, Appointment)
        self.sketch = sketch
        self.appointment = appointment

    def get_demands(self):
        return self.appointment.get_demands()


class Appointment(Node):

    is_leaf = False
    is_branch = False
    is_frame = False

    def __init__(self, mark):
        assert isinstance(mark, Mark)
        self.mark = mark

    def get_demands(self):
        return []


class LeafAppointment(Appointment):

    is_leaf = True

    def __init__(self, column, mark):
        assert isinstance(column, ColumnEntity)
        assert isinstance(mark, Mark)
        super(LeafAppointment, self).__init__(mark)
        self.column = column


class BranchAppointment(Appointment):

    is_branch = True

    def __init__(self, expression, demand_by_unit):
        assert isinstance(expression, Expression)
        assert isinstance(demand_by_unit, dictof(Unit, Demand))
        super(BranchAppointment, self).__init__(expression.mark)
        self.expression = expression
        self.demand_by_unit = demand_by_unit

    def get_demands(self):
        for unit in self.expression.units:
            demand = self.demand_by_unit[unit]
            yield demand


class FrameAppointment(Appointment):

    is_frame = True


class Connection(Node):

    def __init__(self, left, right):
        assert isinstance(left, Appointment)
        assert isinstance(right, Appointment)
        self.left = left
        self.right = right

    def get_demands(self):
        for appointment in [self.left, self.right]:
            for demand in appointment.get_demands():
                yield demand


class Attachment(Node):

    def __init__(self, sketch, connections=[]):
        assert isinstance(sketch, Sketch)
        assert isinstance(connections, listof(Connection))
        self.sketch = sketch
        self.is_inner = sketch.is_inner
        self.is_proper = sketch.is_proper
        self.connections = connections
        self.demand = Demand(sketch, FrameAppointment(self.sketch.mark))

    def get_demands(self):
        if self.is_proper:
            yield self.demand
            for connection in self.connections:
                for demand in connection.get_demands():
                    yield demand
        else:
            for connection in self.connections:
                for demand in connection.left.get_demands():
                    yield demand


