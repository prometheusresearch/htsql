#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql.core.tr.stitch`
===========================

This module implements stitching utilities over flow nodes.
"""


from ..adapter import Adapter, adapt, adapt_many
from ..model import TableNode, ColumnArc, ChainArc
from ..classify import normalize, localize
from .error import CompileError
from .syntax import IdentifierSyntax
from .flow import (Flow, ScalarFlow, TableFlow, FiberTableFlow, QuotientFlow,
        ComplementFlow, MonikerFlow, ForkedFlow, LinkedFlow, ClippedFlow,
        LocatorFlow, OrderedFlow, ColumnUnit, KernelUnit, CoveringUnit)
from .term import Joint


class Arrange(Adapter):
    """
    Produces the ordering of the given flow.

    Returns a list of pairs `(code, direction)`, where `code` is an instance
    of :class:`htsql.core.tr.flow.Code` and `direction` is ``+1`` or ``-1``.
    This list uniquely identifies sorting order of the flow elements.

    This is an interface adapter with a signature::

        Arrange: (Flow, bool, bool) -> [(Code, int), ...]

    The adapter is polymorphic on the first argument.

    `flow` (:class:`htsql.core.tr.flow.Flow`)
        The flow to order.

    `with_strong` (Boolean)
        If set, include explicit flow ordering.

    `with_weak` (Boolean)
        If set, include implicit flow ordering.
    """

    adapt(Flow)

    def __init__(self, flow, with_strong=True, with_weak=True):
        assert isinstance(flow, Flow)
        assert isinstance(with_strong, bool)
        assert isinstance(with_weak, bool)
        self.flow = flow
        self.with_strong = with_strong
        self.with_weak = with_weak

    def __call__(self):
        # The default implementation works for the root flow and non-axial
        # flows.
        if self.flow.base is not None:
            return arrange(self.flow.base, self.with_strong, self.with_weak)
        return []


class Spread(Adapter):
    """
    Produces native units of the given flow.

    This is an interface adapter with a singlature::

        Spread: Flow -> (Unit, ...)

    Native units of the flow are units which are exported by any term
    representing the flow.  Note that together with native units generated
    by this adapter, a flow term should also export same units reparented
    against an inflated flow.

    `flow` (:class:`htsql.core.tr.flow.Flow`)
        The flow node to spread.
    """

    adapt(Flow)

    def __init__(self, flow):
        assert isinstance(flow, Flow)
        self.flow = flow

    def __call__(self):
        # If the flow is not axial, use the native units of the parental flow,
        # but reparent them to the given flow.
        if not self.flow.is_axis:
            for unit in spread(self.flow.base):
                yield unit.clone(flow=self.flow)
        # Otherwise, do not produce any units; must be overriden for axial
        # flows with native units.


class Sew(Adapter):
    """
    Generates joints connecting two parallel flows.

    This is an interface adapter with a singlature::

        Sew: Flow -> (Joint, ...)

    The joints produced by the :class:`Sew` adapter could be used to
    attach together two term nodes represending the same flow node.

    Units in the joints always belong to an inflated flow.

    `flow` (:class:`htsql.core.tr.flow.Flow`)
        The flow node to sew.
    """

    adapt(Flow)

    def __init__(self, flow):
        assert isinstance(flow, Flow)
        self.flow = flow

    def __call__(self):
        # Non-axial flows should use the joints of the closest axis.
        if not self.flow.is_axis:
            return sew(self.flow.base)
        # The default implementation is suitable for scalar flows;
        # must be overriden for other axial flows.
        return []


class Tie(Adapter):
    """
    Generates joints connecting the given flow to its parent.

    This is an interface adapter with a singlature::

        Tie: Flow -> (Joint, ...)

    The joints produced by the :class:`Tie` adapter are used to attach
    a term node representing the flow to a term node representing the
    origin flow.

    Units in the joints always belong to an inflated flow.

    `flow` (:class:`htsql.core.tr.flow.Flow`)
        The flow node to sew.
    """

    adapt(Flow)

    def __init__(self, flow):
        assert isinstance(flow, Flow)
        self.flow = flow

    def __call__(self):
        # Non-axial flows should use the joints of the closest axis.
        if not self.flow.is_axis:
            return tie(self.flow.base)
        # The default implementation is suitable for scalar flows;
        # must be overriden for other axial flows.
        return []


class ArrangeScalar(Arrange):

    adapt(ScalarFlow)

    def __call__(self):
        # A scalar flow inherits its ordering from the parent flow.
        return arrange(self.flow.base, with_strong=self.with_strong,
                                       with_weak=self.with_weak)


class ArrangeTable(Arrange):

    adapt(TableFlow)

    def __call__(self):
        # A table flow complements the ordering of its parent with
        # implicit table ordering.

        for code, direction in arrange(self.flow.base,
                                       with_strong=self.with_strong,
                                       with_weak=self.with_weak):
            yield (code, direction)

        if self.with_weak:
            # Augment the parent ordering with ordering by the primary key
            # of the table (but only if the cardinality of the flow grows).

            # FIXME: the binding tree should pass the ordering information
            # to the flow tree.
            def chain(flow):
                node = TableNode(flow.family.table)
                arcs = localize(node)
                if arcs is None:
                    return None
                units = []
                for arc in arcs:
                    if isinstance(arc, ColumnArc):
                        code = ColumnUnit(arc.column, flow, flow.binding)
                        units.append(code)
                    elif isinstance(arc, ChainArc):
                        subflow = flow
                        for join in arc.joins:
                            subflow = FiberTableFlow(subflow, join,
                                                     flow.binding)
                        subunits = chain(subflow)
                        assert subunits is not None
                        units.extend(subunits)
                    else:
                        assert False, arc
                return units
            if not self.flow.is_contracting:
                flow = self.flow.inflate()
                units = chain(flow)
                if units is not None:
                    for unit in units:
                        flow = unit.flow
                        column = unit.column
                        while (isinstance(flow, FiberTableFlow) and
                               flow.join.is_direct and
                               flow.is_expanding and flow.is_contracting):
                            for origin_column, target_column in \
                                    zip(flow.join.origin_columns,
                                        flow.join.target_columns):
                                if column is target_column:
                                    flow = flow.base
                                    column = origin_column
                                    break
                            else:
                                break
                        unit = unit.clone(flow=flow, column=column)
                        yield (unit, +1)
                    return
                # List of columns which provide the default table ordering.
                columns = []
                # When possible, we take the columns from the primary key
                # of the table.
                table = self.flow.family.table
                if table.primary_key is not None:
                    columns = table.primary_key.origin_columns
                # However when the primary key does not exist, we use columns
                # of the first unique key comprised of non-nullable columns.
                else:
                    for key in table.unique_keys:
                        # Ignore partial keys.
                        if key.is_partial:
                            continue
                        if all(not column.is_nullable
                               for column in key.origin_columns):
                            columns = key.origin_columns
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
                    identifier = IdentifierSyntax(normalize(column.name),
                                                  self.flow.mark)
                    binding = self.flow.binding.clone(syntax=identifier)
                    code = ColumnUnit(column, flow, binding)
                    yield (code, +1)


class SpreadTable(Spread):

    adapt(TableFlow)

    def __call__(self):
        # A term representing a table flow exports all columns of the table.
        for column in self.flow.family.table.columns:
            yield ColumnUnit(column, self.flow, self.flow.binding)


class SewTable(Sew):

    adapt(TableFlow)

    def __call__(self):
        # Connect a table axis to itself using the primary key of the table.

        # The table entity.
        table = self.flow.family.table
        # The columns that constitute the primary key (if we have one).
        connect_columns = None
        # If the table has a primary key, extract the columns.
        if table.primary_key is not None:
            connect_columns = table.primary_key.origin_columns
        # The table lacks a primary key, in this case, search for a unique
        # key which could replace it.
        if connect_columns is None:
            # Iterate over all unique keys of the table.
            for key in table.unique_keys:
                # Ignore partial keys.
                if key.is_partial:
                    continue
                # Check that no columns of the key are nullable,
                # in this case, they uniquely identify a row of the table,
                # and thus, could serve as the primary key.
                if all(not column.is_nullable
                       for column in key.origin_columns):
                    connect_columns = key.origin_columns
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

    adapt(FiberTableFlow)

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

    adapt(QuotientFlow)

    def __call__(self):
        # Start with the parent ordering.
        for code, direction in arrange(self.flow.base,
                                       with_strong=self.with_strong,
                                       with_weak=self.with_weak):
            yield (code, direction)

        # Augment the parent ordering with implicit ordering by
        # the kernel expressions.
        if self.with_weak:
            # We use inflated flows for ordering units.
            flow = self.flow.inflate()
            for code in self.flow.family.kernels:
                code = KernelUnit(code, flow, code.binding)
                yield (code, +1)


class SpreadQuotient(Spread):

    adapt(QuotientFlow)

    def __call__(self):
        # Expressions attaching the quotient to the parent flow.
        # We take a tie between the seed ground and its parent;
        # the left side of the tie belongs to the seed ground
        # and must be exported by any term representing the quotient.
        for lunit, runit in tie(self.flow.family.ground):
            yield KernelUnit(runit, self.flow, runit.binding)
        # The kernel expressions of the quotient.
        for code in self.flow.family.kernels:
            yield KernelUnit(code, self.flow, code.binding)


class SewQuotient(Sew):

    adapt(QuotientFlow)

    def __call__(self):
        # Use an inflated flow for joints.
        flow = self.flow.inflate()
        # The ground base units attaching the quotient to
        # the parent flow.  FIXME: not needed when the parent
        # flow is also sewn.
        for joint in tie(flow.family.ground):
            op = KernelUnit(joint.rop, flow, joint.rop.binding)
            yield joint.clone(lop=op, rop=op)
        # The kernel expressions.
        for code in flow.family.kernels:
            unit = KernelUnit(code, flow, code.binding)
            yield Joint(unit, unit)


class TieQuotient(Tie):

    adapt(QuotientFlow)

    def __call__(self):
        # Use an inflated flow for joints.
        flow = self.flow.inflate()
        # Use the joints attaching the seed ground to its parent,
        # but wrap the ground units so they belong to the quotient flow.
        for joint in tie(flow.family.ground):
            rop = KernelUnit(joint.rop, flow, joint.rop.binding)
            yield joint.clone(rop=rop)


class ArrangeCovering(Arrange):

    # The implementation is shared by all covering flows.
    adapt_many(ComplementFlow,
               MonikerFlow,
               ForkedFlow,
               LinkedFlow,
               ClippedFlow,
               LocatorFlow)

    def __call__(self):
        # Start with the parent ordering.
        for code, direction in arrange(self.flow.base,
                                       with_strong=self.with_strong,
                                       with_weak=self.with_weak):
            yield (code, direction)

        # Add ordering of the seed flow.
        if self.with_weak:
            # We use inflated flows for ordering.
            flow = self.flow.inflate()
            # We use this flow to filter out expressions singular against
            # the parent flow; could be `None` (only for `ForkedFlow`
            # and `MonikerFlow`).  Note that we could have used
            # `self.flow.base` for all but `ForkedFlow`.
            base = self.flow.ground.base
            # Emit ordering of the seed, but ignore expressions
            # that are singular against the parent flow --
            # they cannot affect the ordering.
            # Note that both weak and strong ordering of the seed
            # become weak ordering of the covering flow.
            for code, direction in arrange(self.flow.seed):
                if base is None or any(not base.spans(unit.flow)
                                       for unit in code.units):
                    code = CoveringUnit(code, flow, code.binding)
                    yield (code, direction)


class SpreadCovering(Spread):

    # The implementation is shared by all covering flows.
    adapt_many(ComplementFlow,
               MonikerFlow,
               ForkedFlow,
               LinkedFlow,
               ClippedFlow,
               LocatorFlow)

    def __call__(self):
        # Native units of the complement are inherited from the seed flow.

        # Use an inflated seed to reduce the number of variations.
        seed = self.flow.seed.inflate()
        for unit in spread(seed):
            yield unit.clone(flow=self.flow)


class SewCovering(Sew):

    # The implementation is shared by all covering flows.
    adapt_many(ComplementFlow,
               MonikerFlow,
               LinkedFlow,
               ForkedFlow,
               ClippedFlow,
               LocatorFlow)

    def __call__(self):
        # To sew two terms representing a covering flow, we sew all axial flows
        # from the seed to the ground.

        # Use an inflated flow for joints.
        flow = self.flow.inflate()

        # The top axis.
        seed = self.flow.seed.inflate()
        # The last axis to use (for `ForkedFlow`, same as `seed`).
        baseline = self.flow.ground.inflate()
        # Gather all axes from the seed to the baseline.
        axes = []
        axis = seed
        while axis is not None and axis.concludes(baseline):
            axes.append(axis)
            axis = axis.base
        # Start from the shortest axis.
        axes.reverse()
        # Combine joints from all the axes.
        for axis in axes:
            # We can skip non-expanding axes, but must always
            # include the baseline.
            if not axis.is_contracting or axis == baseline:
                # Wrap and emit the axis joints.
                for joint in sew(axis):
                    op = CoveringUnit(joint.lop, flow, joint.lop.binding)
                    yield joint.clone(lop=op, rop=op)


class TieComplement(Tie):

    adapt(ComplementFlow)

    def __call__(self):
        # Use an inflated flow for joints.
        flow = self.flow.inflate()
        # Units attaching the seed ground to its parent.
        for joint in tie(flow.ground):
            # The ground base expression.
            op = joint.rop
            # The expression embedded in the quotient.
            lop = KernelUnit(op, flow.base, op.binding)
            # The expression embedded in the complement.
            rop = CoveringUnit(op, flow, op.binding)
            yield joint.clone(lop=lop, rop=rop)
        # The kernel expressions.
        for code in flow.kernels:
            # The quotient kernel.
            lop = KernelUnit(code, flow.base, code.binding)
            # The same kernel embedded in the complement space.
            rop = CoveringUnit(code, flow, code.binding)
            yield Joint(lop=lop, rop=rop)


class TieMoniker(Tie):

    adapt(MonikerFlow)

    def __call__(self):
        # Use an inflated flow for joints.
        flow = self.flow.inflate()
        # Normally, use the serial joints of the seed ground, but if
        # the ground (as well as the flow itself) is singular against
        # the parent flow, use parallel joints.
        if flow.is_contracting:
            joints = sew(flow.ground)
        else:
            joints = tie(flow.ground)
        # Wrap the ground joints.
        for joint in joints:
            rop = CoveringUnit(joint.rop, flow, joint.rop.binding)
            yield joint.clone(rop=rop)


class TieLocator(Tie):

    adapt(LocatorFlow)

    def __call__(self):
        flow = self.flow.inflate()
        for joint in tie(flow.ground):
            rop = CoveringUnit(joint.rop, flow, joint.rop.binding)
            yield joint.clone(rop=rop)


class TieClipped(Tie):

    adapt(ClippedFlow)

    def __call__(self):
        flow = self.flow.inflate()
        joints = tie(flow.ground)
        for joint in joints:
            rop = CoveringUnit(joint.rop, flow, joint.rop.binding)
            yield joint.clone(rop=rop)


class TieForked(Tie):

    adapt(ForkedFlow)

    def __call__(self):
        # Use an inflated flow for joints.
        flow = self.flow.inflate()
        # Attach the seed ground to its parent.
        for joint in tie(flow.seed):
            lop = joint.rop
            rop = CoveringUnit(lop, flow, lop.binding)
            yield joint.clone(lop=lop, rop=rop)
        # Attach the seed to itself by the kernel expressions.
        for code in self.flow.kernels:
            lop = code
            rop = CoveringUnit(code, flow, code.binding)
            yield Joint(lop, rop)


class TieLinked(Tie):

    adapt(LinkedFlow)

    def __call__(self):
        # Use an inflated flow for joints.
        flow = self.flow.inflate()
        # Attach the seed to the base flow using the fiber conditions.
        for lop, rop in flow.images:
            rop = CoveringUnit(rop, flow, rop.binding)
            yield Joint(lop, rop)


class ArrangeOrdered(Arrange):

    adapt(OrderedFlow)

    def __call__(self):
        # Start with strong ordering of the parent flow.
        if self.with_strong:
            for code, direction in arrange(self.flow.base,
                                           with_strong=True, with_weak=False):
                yield (code, direction)
            # Emit the ordering specified by the node itself.
            for code, direction in self.flow.order:
                yield (code, direction)
        # Conclude with weak ordering of the parent flow.
        if self.with_weak:
            for code, direction in arrange(self.flow.base,
                                           with_strong=False, with_weak=True):
                yield (code, direction)


def arrange(flow, with_strong=True, with_weak=True):
    """
    Returns the ordering of the given flow.

    `flow` (:class:`htsql.core.tr.flow.Flow`)
        The flow to order.

    `with_strong` (Boolean)
        If set, include explicit flow ordering.

    `with_weak` (Boolean)
        If set, include implicit flow ordering.
    """
    order = []
    duplicates = set()
    for code, direction in Arrange.__invoke__(flow, with_strong, with_weak):
        if code in duplicates:
            continue
        order.append((code, direction))
        duplicates.add(code)
    return order


def spread(flow):
    """
    Returns native units of the given flow.
    """
    return list(Spread.__invoke__(flow))


def sew(flow):
    return list(Sew.__invoke__(flow))


def tie(flow):
    return list(Tie.__invoke__(flow))


