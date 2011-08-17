#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql.tr.stitch`
======================

This module implements stitching utilities over flow nodes.
"""


from ..util import maybe, listof
from ..adapter import Adapter, adapts
from .error import CompileError
from .syntax import IdentifierSyntax
from .flow import (Flow, RootFlow, ScalarFlow, TableFlow,
                   DirectTableFlow, FiberTableFlow,
                   QuotientFlow, ComplementFlow, MonikerFlow, ForkedFlow,
                   LinkedFlow, FilteredFlow, OrderedFlow,
                   ColumnUnit, KernelUnit, CoveringUnit)
from .term import Joint


class Arrange(Adapter):

    adapts(Flow)

    def __init__(self, flow, with_strong=True, with_weak=True):
        assert isinstance(flow, Flow)
        assert isinstance(with_strong, bool)
        assert isinstance(with_weak, bool)
        self.flow = flow
        self.with_strong = with_strong
        self.with_weak = with_weak

    def __call__(self):
        return arrange(self.flow.base, self.with_strong, self.with_weak)


class Spread(Adapter):

    adapts(Flow)

    def __init__(self, flow):
        assert isinstance(flow, Flow)
        self.flow = flow

    def __call__(self):
        if not self.flow.is_axis:
            return spread(self.flow.base)
        return []


class Sew(Adapter):

    adapts(Flow)

    def __init__(self, flow):
        assert isinstance(flow, Flow)
        self.flow = flow

    def __call__(self):
        if not self.flow.is_axis:
            return sew(self.flow.base)
        return []


class Tie(Adapter):

    adapts(Flow)

    def __init__(self, flow):
        assert isinstance(flow, Flow)
        self.flow = flow

    def __call__(self):
        if not self.flow.is_axis:
            return tie(self.flow.base)
        return []


class ArrangeRoot(Arrange):

    adapts(RootFlow)

    def __call__(self):
        return []


class ArrangeScalar(Arrange):

    adapts(ScalarFlow)

    def __call__(self):
        return arrange(self.flow.base, with_strong=self.with_strong,
                                       with_weak=self.with_weak)


class ArrangeTable(Arrange):

    adapts(TableFlow)

    def __call__(self):
        # A table flow complements the weak ordering of its base with
        # implicit table ordering.

        for code, direction in arrange(self.flow.base,
                                       with_strong=self.with_strong,
                                       with_weak=self.with_weak):
            yield (code, direction)

        if self.with_weak:
            # Complement the weak ordering with the table ordering (but only
            # if the cardinality of the flow may increase).
            if not self.flow.is_contracting:
                # List of columns which provide the default table ordering.
                columns = []
                # When possible, we take the columns from the primary key
                # of the table.
                table = self.flow.family.table
                if table.primary_key is not None:
                    column_names = table.primary_key.origin_column_names
                    columns = [table.columns[column_name]
                               for column_name in column_names]
                # However when the primary key does not exist, we use columns
                # of the first unique key comprised of non-nullable columns.
                else:
                    for key in table.unique_keys:
                        column_names = key.origin_column_names
                        key_columns = [table.columns[column_name]
                                       for column_name in column_names]
                        if all(not column.is_nullable
                               for column in key_columns):
                            columns = key_columns
                            break
                # If neither the primary key nor unique keys with non-nullable
                # columns exist, we have one option left: sort by all columns
                # of the table.
                if not columns:
                    columns = list(table.columns)
                # We assign the column units to the inflated flow: it makes
                # it easier to find and eliminate duplicates.
                flow = self.flow.inflate()
                # Add weak table ordering.
                for column in columns:
                    # We need to associate the newly generated column unit
                    # with some binding node.  We use the binding of the flow,
                    # but in order to produce a better string representation,
                    # we replace the associated syntax node with a new
                    # identifier named after the column.
                    identifier = IdentifierSyntax(column.name, self.flow.mark)
                    binding = self.flow.binding.clone(syntax=identifier)
                    code = ColumnUnit(column, flow, binding)
                    yield (code, +1)


class SpreadTable(Spread):

    adapts(TableFlow)

    def __call__(self):
        flow = self.flow.inflate()
        for column in flow.family.table.columns:
            yield ColumnUnit(column, flow, self.flow.binding)


class SewTable(Sew):

    adapts(TableFlow)

    def __call__(self):
        # Connect a table axis to itself using the primary key of the table.

        # The table entity.
        table = self.flow.family.table
        # The columns that constitute the primary key (if we have one).
        connect_columns = None
        # If the table has a primary key, extract the columns.
        if table.primary_key is not None:
            column_names = table.primary_key.origin_column_names
            connect_columns = [table.columns[column_name]
                               for column_name in column_names]
        # The table lacks a primary key, in this case, search for a unique
        # key which could replace it.
        if connect_columns is None:
            # Iterate over all unique keys of the table.
            for key in table.unique_keys:
                # Extract the columns of the key.
                column_names = key.origin_column_names
                key_columns = [table.columns[column_name]
                               for column_name in column_names]
                # Check that no columns of the key are nullable,
                # in this case, they uniquely identify a row of the table,
                # and thus, could serve as the primary key.
                if all(not column.is_nullable for column in key_columns):
                    connect_columns = key_columns
                    break
        # No primary key, we don't have other choice but to report an error.
        if connect_columns is None:
            raise CompileError("unable to connect a table"
                               " lacking a primary key", self.flow.mark)
        # Generate joints that represent a connection by the primary key.
        flow = self.flow.inflate()
        for column in connect_columns:
            unit = ColumnUnit(column, flow, self.flow.binding)
            yield Joint(unit, unit)


class TieFiberTable(Tie):

    adapts(FiberTableFlow)

    def __call__(self):
        # Generate a list of joints corresponding to a connection by
        # a foreign key.  Note that the left unit must belong to the base
        # of the term axis while the right unit belongs to the axis itself.
        flow = self.flow.inflate()
        for lcolumn, rcolumn in zip(flow.join.origin_columns,
                                    flow.join.target_columns):
            lunit = ColumnUnit(lcolumn, flow.base, self.flow.binding)
            runit = ColumnUnit(rcolumn, flow, self.flow.binding)
            yield Joint(lunit, runit)


class ArrangeQuotient(Arrange):

    adapts(QuotientFlow)

    def __call__(self):
        for code, direction in arrange(self.flow.base,
                                       with_strong=self.with_strong,
                                       with_weak=self.with_weak):
            yield (code, direction)
        if self.with_weak:
            flow = self.flow.inflate()
            for code in self.flow.family.kernels:
                code = KernelUnit(code, flow, code.binding)
                yield (code, +1)


class SpreadQuotient(Spread):

    adapts(QuotientFlow)

    def __call__(self):
        flow = self.flow.inflate()
        for lunit, runit in tie(flow.family.ground):
            yield KernelUnit(runit, flow, runit.binding)
        for code in self.flow.family.kernels:
            yield KernelUnit(code, flow, code.binding)


class SewQuotient(Sew):

    adapts(QuotientFlow)

    def __call__(self):
        flow = self.flow.inflate()
        for joint in tie(flow.family.ground):
            op = KernelUnit(joint.rop, flow, joint.rop.binding)
            yield joint.clone(lop=op, rop=op)
        for code in flow.family.kernels:
            unit = KernelUnit(code, flow, code.binding)
            yield Joint(unit, unit)


class TieQuotient(Tie):

    adapts(QuotientFlow)

    def __call__(self):
        flow = self.flow.inflate()
        for joint in tie(flow.family.ground):
            rop = KernelUnit(joint.rop, flow, joint.rop.binding)
            yield joint.clone(rop=rop)


class ArrangeComplement(Arrange):

    adapts(ComplementFlow)

    def __call__(self):
        for code, direction in arrange(self.flow.base,
                                       with_strong=self.with_strong,
                                       with_weak=self.with_weak):
            yield (code, direction)
        if self.with_weak:
            flow = self.flow.inflate()
            for code, direction in arrange(self.flow.base.family.seed):
                if any(not self.flow.base.spans(unit.flow)
                       for unit in code.units):
                    code = CoveringUnit(code, flow, code.binding)
                    yield (code, direction)


class SpreadComplement(Spread):

    adapts(ComplementFlow)

    def __call__(self):
        flow = self.flow.inflate()
        seed = self.flow.base.family.seed.inflate()
        for unit in spread(seed):
            yield unit.clone(flow=flow)


class SewComplement(Sew):

    adapts(ComplementFlow)

    def __call__(self):
        flow = self.flow.inflate()
        seed = self.flow.base.family.seed.inflate()
        baseline = self.flow.base.family.ground.inflate()
        axes = []
        axis = seed
        while axis is not None and axis.concludes(baseline):
            axes.append(axis)
            axis = axis.base
        axes.reverse()
        for axis in axes:
            if not axis.is_contracting or axis == baseline:
                for joint in sew(axis):
                    op = CoveringUnit(joint.lop, flow, joint.lop.binding)
                    yield joint.clone(lop=op, rop=op)


class TieComplement(Tie):

    adapts(ComplementFlow)

    def __call__(self):
        flow = self.flow.inflate()
        for joint in tie(flow.base.family.ground):
            lop = KernelUnit(joint.rop, flow.base, joint.rop.binding)
            rop = CoveringUnit(joint.rop, flow, joint.rop.binding)
            yield joint.clone(lop=lop, rop=rop)
        for code in flow.base.family.kernels:
            lop = KernelUnit(code, flow.base, code.binding)
            rop = CoveringUnit(code, flow, code.binding)
            yield Joint(lop=lop, rop=rop)


class ArrangeMoniker(Arrange):

    adapts(MonikerFlow)

    def __call__(self):
        for code, direction in arrange(self.flow.base,
                                       with_strong=self.with_strong,
                                       with_weak=self.with_weak):
            yield (code, direction)
        if self.with_weak:
            flow = self.flow.inflate()
            for code, direction in arrange(self.flow.seed):
                if any(not self.flow.base.spans(unit.flow)
                       for unit in code.units):
                    code = CoveringUnit(code, flow, code.binding)
                    yield (code, direction)


class SpreadMoniker(Spread):

    adapts(MonikerFlow)

    def __call__(self):
        flow = self.flow.inflate()
        seed = self.flow.seed.inflate()
        for unit in spread(seed):
            yield unit.clone(flow=flow)


class SewMoniker(Sew):

    adapts(MonikerFlow)

    def __call__(self):
        flow = self.flow.inflate()
        seed = self.flow.seed.inflate()
        baseline = self.flow.ground.inflate()
        axes = []
        axis = seed
        while axis is not None and axis.concludes(baseline):
            axes.append(axis)
            axis = axis.base
        axes.reverse()
        for axis in axes:
            if not axis.is_contracting or axis == baseline:
                for joint in sew(axis):
                    op = CoveringUnit(joint.lop, flow, joint.lop.binding)
                    yield joint.clone(lop=op, rop=op)


class TieMoniker(Tie):

    adapts(MonikerFlow)

    def __call__(self):
        flow = self.flow.inflate()
        for joint in tie(flow.ground):
            rop = CoveringUnit(joint.rop, flow, joint.rop.binding)
            yield joint.clone(rop=rop)


class ArrangeForked(Arrange):

    adapts(ForkedFlow)

    def __call__(self):
        for code, direction in arrange(self.flow.base,
                                       with_strong=self.with_strong,
                                       with_weak=self.with_weak):
            yield (code, direction)
        if self.with_weak and not self.flow.is_contracting:
            flow = self.flow.inflate()
            for code, direction in arrange(self.flow.seed):
                if all(self.flow.ground.base.spans(unit.flow)
                       for unit in code.units):
                    continue
                code = CoveringUnit(code, flow, code.binding)
                yield (code, direction)


class SpreadForked(Spread):

    adapts(ForkedFlow)

    def __call__(self):
        flow = self.flow.inflate()
        seed = self.flow.seed.inflate()
        for unit in spread(seed):
            yield unit.clone(flow=flow)


class SewForked(Sew):

    adapts(ForkedFlow)

    def __call__(self):
        flow = self.flow.inflate()
        seed = self.flow.seed.inflate()
        for joint in sew(seed):
            op = CoveringUnit(joint.lop, flow, joint.lop.binding)
            yield joint.clone(lop=op, rop=op)


class TieForked(Tie):

    adapts(ForkedFlow)

    def __call__(self):
        flow = self.flow.inflate()
        seed = self.flow.seed.inflate()
        for joint in tie(seed):
            lop = joint.rop
            rop = CoveringUnit(lop, flow, lop.binding)
            yield joint.clone(lop=lop, rop=rop)
        for code in self.flow.kernels:
            lop = code
            rop = CoveringUnit(code, flow, code.binding)
            yield Joint(lop, rop)


class ArrangeLinked(Arrange):

    adapts(LinkedFlow)

    def __call__(self):
        for code, direction in arrange(self.flow.base,
                                       with_strong=self.with_strong,
                                       with_weak=self.with_weak):
            yield (code, direction)
        if self.with_weak:
            flow = self.flow.inflate()
            for code, direction in arrange(self.flow.seed):
                if any(not self.flow.base.spans(unit.flow)
                       for unit in code.units):
                    code = CoveringUnit(code, flow, code.binding)
                    yield (code, direction)


class SpreadLinked(Spread):

    adapts(LinkedFlow)

    def __call__(self):
        flow = self.flow.inflate()
        seed = self.flow.seed.inflate()
        for unit in spread(seed):
            yield unit.clone(flow=flow)


class SewLinked(Sew):

    adapts(LinkedFlow)

    def __call__(self):
        flow = self.flow.inflate()
        seed = self.flow.seed.inflate()
        baseline = self.flow.ground.inflate()
        axes = []
        axis = seed
        while axis is not None and axis.concludes(baseline):
            axes.append(axis)
            axis = axis.base
        axes.reverse()
        for axis in axes:
            if not axis.is_contracting or axis == baseline:
                for joint in sew(axis):
                    op = CoveringUnit(joint.lop, flow, joint.lop.binding)
                    yield joint.clone(lop=op, rop=op)


class TieLinked(Tie):

    adapts(LinkedFlow)

    def __call__(self):
        flow = self.flow.inflate()
        for lop, rop in flow.images:
            rop = CoveringUnit(rop, flow, rop.binding)
            yield Joint(lop, rop)


class ArrangeOrdered(Arrange):

    adapts(OrderedFlow)

    def __call__(self):
        if self.with_strong:
            for code, direction in arrange(self.flow.base,
                                           with_strong=True, with_weak=False):
                yield (code, direction)
            for code, direction in self.flow.order:
                yield (code, direction)
        if self.with_weak:
            for code, direction in arrange(self.flow.base,
                                           with_strong=False, with_weak=True):
                yield (code, direction)


def arrange(flow, with_strong=True, with_weak=True):
    arrange = Arrange(flow, with_strong, with_weak)
    return list(arrange())


def spread(flow):
    spread = Spread(flow)
    return list(spread())


def sew(flow):
    sew = Sew(flow)
    return list(sew())


def tie(flow):
    tie = Tie(flow)
    return list(tie())


