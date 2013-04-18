#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ..adapter import Adapter, adapt, adapt_many
from ..model import TableNode, ColumnArc, ChainArc
from ..classify import normalize, localize
from ..error import Error, translate_guard
from ..syn.syntax import IdentifierSyntax
from .space import (Space, ScalarSpace, TableSpace, FiberTableSpace,
        QuotientSpace, ComplementSpace, MonikerSpace, ForkedSpace, AttachSpace,
        ClippedSpace, LocatorSpace, OrderedSpace, ColumnUnit, KernelUnit,
        CoveringUnit)
from .term import Joint


class Arrange(Adapter):
    """
    Produces the ordering of the given space.

    Returns a list of pairs `(code, direction)`, where `code` is an instance
    of :class:`htsql.core.tr.space.Code` and `direction` is ``+1`` or ``-1``.
    This list uniquely identifies sorting order of the space elements.

    This is an interface adapter with a signature::

        Arrange: (Space, bool, bool) -> [(Code, int), ...]

    The adapter is polymorphic on the first argument.

    `space` (:class:`htsql.core.tr.space.Space`)
        The space to order.

    `with_strong` (Boolean)
        If set, include explicit space ordering.

    `with_weak` (Boolean)
        If set, include implicit space ordering.
    """

    adapt(Space)

    def __init__(self, space, with_strong=True, with_weak=True):
        assert isinstance(space, Space)
        assert isinstance(with_strong, bool)
        assert isinstance(with_weak, bool)
        self.space = space
        self.with_strong = with_strong
        self.with_weak = with_weak

    def __call__(self):
        # The default implementation works for the root space and non-axial
        # spaces.
        if self.space.base is not None:
            return arrange(self.space.base, self.with_strong, self.with_weak)
        return []


class Spread(Adapter):
    """
    Produces native units of the given space.

    This is an interface adapter with a singlature::

        Spread: Space -> (Unit, ...)

    Native units of the space are units which are exported by any term
    representing the space.  Note that together with native units generated
    by this adapter, a space term should also export same units reparented
    against an inflated space.

    `space` (:class:`htsql.core.tr.space.Space`)
        The space node to spread.
    """

    adapt(Space)

    def __init__(self, space):
        assert isinstance(space, Space)
        self.space = space

    def __call__(self):
        # If the space is not axial, use the native units of the parental space,
        # but reparent them to the given space.
        if not self.space.is_axis:
            for unit in spread(self.space.base):
                yield unit.clone(space=self.space)
        # Otherwise, do not produce any units; must be overriden for axial
        # spaces with native units.


class Sew(Adapter):
    """
    Generates joints connecting two parallel spaces.

    This is an interface adapter with a singlature::

        Sew: Space -> (Joint, ...)

    The joints produced by the :class:`Sew` adapter could be used to
    attach together two term nodes represending the same space node.

    Units in the joints always belong to an inflated space.

    `space` (:class:`htsql.core.tr.space.Space`)
        The space node to sew.
    """

    adapt(Space)

    def __init__(self, space):
        assert isinstance(space, Space)
        self.space = space

    def __call__(self):
        # Non-axial spaces should use the joints of the closest axis.
        if not self.space.is_axis:
            return sew(self.space.base)
        # The default implementation is suitable for scalar spaces;
        # must be overriden for other axial spaces.
        return []


class Tie(Adapter):
    """
    Generates joints connecting the given space to its parent.

    This is an interface adapter with a singlature::

        Tie: Space -> (Joint, ...)

    The joints produced by the :class:`Tie` adapter are used to attach
    a term node representing the space to a term node representing the
    origin space.

    Units in the joints always belong to an inflated space.

    `space` (:class:`htsql.core.tr.space.Space`)
        The space node to sew.
    """

    adapt(Space)

    def __init__(self, space):
        assert isinstance(space, Space)
        self.space = space

    def __call__(self):
        # Non-axial spaces should use the joints of the closest axis.
        if not self.space.is_axis:
            return tie(self.space.base)
        # The default implementation is suitable for scalar spaces;
        # must be overriden for other axial spaces.
        return []


class ArrangeScalar(Arrange):

    adapt(ScalarSpace)

    def __call__(self):
        # A scalar space inherits its ordering from the parent space.
        return arrange(self.space.base, with_strong=self.with_strong,
                                       with_weak=self.with_weak)


class ArrangeTable(Arrange):

    adapt(TableSpace)

    def __call__(self):
        # A table space complements the ordering of its parent with
        # implicit table ordering.

        for code, direction in arrange(self.space.base,
                                       with_strong=self.with_strong,
                                       with_weak=self.with_weak):
            yield (code, direction)

        if self.with_weak:
            # Augment the parent ordering with ordering by the primary key
            # of the table (but only if the cardinality of the space grows).

            # FIXME: the binding tree should pass the ordering information
            # to the space tree.
            def chain(space):
                node = TableNode(space.family.table)
                arcs = localize(node)
                if arcs is None:
                    return None
                units = []
                for arc in arcs:
                    if isinstance(arc, ColumnArc):
                        code = ColumnUnit(arc.column, space, space.binding)
                        units.append(code)
                    elif isinstance(arc, ChainArc):
                        subspace = space
                        for join in arc.joins:
                            subspace = FiberTableSpace(subspace, join,
                                                     space.binding)
                        subunits = chain(subspace)
                        assert subunits is not None
                        units.extend(subunits)
                    else:
                        assert False, arc
                return units
            if not self.space.is_contracting:
                space = self.space.inflate()
                units = chain(space)
                if units is not None:
                    for unit in units:
                        space = unit.space
                        column = unit.column
                        while (isinstance(space, FiberTableSpace) and
                               space.join.is_direct and
                               space.is_expanding and space.is_contracting):
                            for origin_column, target_column in \
                                    zip(space.join.origin_columns,
                                        space.join.target_columns):
                                if column is target_column:
                                    space = space.base
                                    column = origin_column
                                    break
                            else:
                                break
                        unit = unit.clone(space=space, column=column)
                        yield (unit, +1)
                    return
                # List of columns which provide the default table ordering.
                columns = []
                # When possible, we take the columns from the primary key
                # of the table.
                table = self.space.family.table
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
                # We assign the column units to the inflated space: it makes
                # it easier to find and eliminate duplicates.
                space = self.space.inflate()
                # Add weak table ordering.
                for column in columns:
                    # We need to associate the newly generated column unit
                    # with some binding node.  We use the binding of the space,
                    # but in order to produce a better string representation,
                    # we replace the associated syntax node with a new
                    # identifier named after the column.
                    identifier = IdentifierSyntax(normalize(column.name))
                    binding = self.space.binding.clone(syntax=identifier)
                    code = ColumnUnit(column, space, binding)
                    yield (code, +1)


class SpreadTable(Spread):

    adapt(TableSpace)

    def __call__(self):
        # A term representing a table space exports all columns of the table.
        for column in self.space.family.table.columns:
            yield ColumnUnit(column, self.space, self.space.binding)


class SewTable(Sew):

    adapt(TableSpace)

    def __call__(self):
        # Connect a table axis to itself using the primary key of the table.

        # The table entity.
        table = self.space.family.table
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
            with translate_guard(self.space):
                raise Error("Unable to connect a table"
                            " lacking a primary key")
        # Generate joints that represent a connection by the primary key.
        space = self.space.inflate()
        for column in connect_columns:
            unit = ColumnUnit(column, space, self.space.binding)
            yield Joint(unit, unit)


class TieFiberTable(Tie):

    adapt(FiberTableSpace)

    def __call__(self):
        # Generate a list of joints corresponding to a connection by
        # a foreign key.  Note that the left unit must belong to the base
        # of the term axis while the right unit belongs to the axis itself.
        space = self.space.inflate()
        for lcolumn, rcolumn in zip(space.join.origin_columns,
                                    space.join.target_columns):
            lunit = ColumnUnit(lcolumn, space.base, self.space.binding)
            runit = ColumnUnit(rcolumn, space, self.space.binding)
            yield Joint(lunit, runit)


class ArrangeQuotient(Arrange):

    adapt(QuotientSpace)

    def __call__(self):
        # Start with the parent ordering.
        for code, direction in arrange(self.space.base,
                                       with_strong=self.with_strong,
                                       with_weak=self.with_weak):
            yield (code, direction)

        # Augment the parent ordering with implicit ordering by
        # the kernel expressions.
        if self.with_weak:
            # We use inflated spaces for ordering units.
            space = self.space.inflate()
            for code in self.space.family.kernels:
                code = KernelUnit(code, space, code.binding)
                yield (code, +1)


class SpreadQuotient(Spread):

    adapt(QuotientSpace)

    def __call__(self):
        # Expressions attaching the quotient to the parent space.
        # We take a tie between the seed ground and its parent;
        # the left side of the tie belongs to the seed ground
        # and must be exported by any term representing the quotient.
        for lunit, runit in tie(self.space.family.ground):
            yield KernelUnit(runit, self.space, runit.binding)
        # The kernel expressions of the quotient.
        for code in self.space.family.kernels:
            yield KernelUnit(code, self.space, code.binding)


class SewQuotient(Sew):

    adapt(QuotientSpace)

    def __call__(self):
        # Use an inflated space for joints.
        space = self.space.inflate()
        # The ground base units attaching the quotient to
        # the parent space.  FIXME: not needed when the parent
        # space is also sewn.
        for joint in tie(space.family.ground):
            op = KernelUnit(joint.rop, space, joint.rop.binding)
            yield joint.clone(lop=op, rop=op)
        # The kernel expressions.
        for code in space.family.kernels:
            unit = KernelUnit(code, space, code.binding)
            yield Joint(unit, unit)


class TieQuotient(Tie):

    adapt(QuotientSpace)

    def __call__(self):
        # Use an inflated space for joints.
        space = self.space.inflate()
        # Use the joints attaching the seed ground to its parent,
        # but wrap the ground units so they belong to the quotient space.
        for joint in tie(space.family.ground):
            rop = KernelUnit(joint.rop, space, joint.rop.binding)
            yield joint.clone(rop=rop)


class ArrangeCovering(Arrange):

    # The implementation is shared by all covering spaces.
    adapt_many(ComplementSpace,
               MonikerSpace,
               ForkedSpace,
               AttachSpace,
               ClippedSpace)

    def __call__(self):
        # Start with the parent ordering.
        for code, direction in arrange(self.space.base,
                                       with_strong=self.with_strong,
                                       with_weak=self.with_weak):
            yield (code, direction)

        # Add ordering of the seed space.
        if self.with_weak:
            # We use inflated spaces for ordering.
            space = self.space.inflate()
            # We use this space to filter out expressions singular against
            # the parent space; could be `None` (only for `ForkedSpace`
            # and `MonikerSpace`).  Note that we could have used
            # `self.space.base` for all but `ForkedSpace`.
            base = self.space.ground.base
            # Emit ordering of the seed, but ignore expressions
            # that are singular against the parent space --
            # they cannot affect the ordering.
            # Note that both weak and strong ordering of the seed
            # become weak ordering of the covering space.
            for code, direction in arrange(self.space.seed):
                if base is None or any(not base.spans(unit.space)
                                       for unit in code.units):
                    code = CoveringUnit(code, space, code.binding)
                    yield (code, direction)


class SpreadCovering(Spread):

    # The implementation is shared by all covering spaces.
    adapt_many(ComplementSpace,
               MonikerSpace,
               ForkedSpace,
               AttachSpace,
               ClippedSpace)

    def __call__(self):
        # Native units of the complement are inherited from the seed space.

        # Use an inflated seed to reduce the number of variations.
        seed = self.space.seed.inflate()
        for unit in spread(seed):
            yield unit.clone(space=self.space)


class SewCovering(Sew):

    # The implementation is shared by all covering spaces.
    adapt_many(ComplementSpace,
               MonikerSpace,
               AttachSpace,
               ForkedSpace,
               ClippedSpace)

    def __call__(self):
        # To sew two terms representing a covering space, we sew all axial spaces
        # from the seed to the ground.

        # Use an inflated space for joints.
        space = self.space.inflate()

        # The top axis.
        seed = self.space.seed.inflate()
        # The last axis to use (for `ForkedSpace`, same as `seed`).
        baseline = self.space.ground.inflate()
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
                    op = CoveringUnit(joint.lop, space, joint.lop.binding)
                    yield joint.clone(lop=op, rop=op)


class TieComplement(Tie):

    adapt(ComplementSpace)

    def __call__(self):
        # Use an inflated space for joints.
        space = self.space.inflate()
        # Units attaching the seed ground to its parent.
        for joint in tie(space.ground):
            # The ground base expression.
            op = joint.rop
            # The expression embedded in the quotient.
            lop = KernelUnit(op, space.base, op.binding)
            # The expression embedded in the complement.
            rop = CoveringUnit(op, space, op.binding)
            yield joint.clone(lop=lop, rop=rop)
        # The kernel expressions.
        for code in space.kernels:
            # The quotient kernel.
            lop = KernelUnit(code, space.base, code.binding)
            # The same kernel embedded in the complement space.
            rop = CoveringUnit(code, space, code.binding)
            yield Joint(lop=lop, rop=rop)


class TieMoniker(Tie):

    adapt(MonikerSpace)

    def __call__(self):
        # Use an inflated space for joints.
        space = self.space.inflate()
        # Normally, use the serial joints of the seed ground, but if
        # the ground (as well as the space itself) is singular against
        # the parent space, use parallel joints.
        if space.is_contracting:
            joints = sew(space.ground)
        else:
            joints = tie(space.ground)
        # Wrap the ground joints.
        for joint in joints:
            rop = CoveringUnit(joint.rop, space, joint.rop.binding)
            yield joint.clone(rop=rop)


class TieClipped(Tie):

    adapt(ClippedSpace)

    def __call__(self):
        space = self.space.inflate()
        joints = tie(space.ground)
        for joint in joints:
            rop = CoveringUnit(joint.rop, space, joint.rop.binding)
            yield joint.clone(rop=rop)


class TieForked(Tie):

    adapt(ForkedSpace)

    def __call__(self):
        # Use an inflated space for joints.
        space = self.space.inflate()
        # Attach the seed ground to its parent.
        for joint in tie(space.seed):
            lop = joint.rop
            rop = CoveringUnit(lop, space, lop.binding)
            yield joint.clone(lop=lop, rop=rop)
        # Attach the seed to itself by the kernel expressions.
        for code in self.space.kernels:
            lop = code
            rop = CoveringUnit(code, space, code.binding)
            yield Joint(lop, rop)


class TieAttach(Tie):

    adapt(AttachSpace)

    def __call__(self):
        # Use an inflated space for joints.
        space = self.space.inflate()
        for joint in tie(space.ground):
            rop = CoveringUnit(joint.rop, space, joint.rop.binding)
            yield joint.clone(rop=rop)
        # Attach the seed to the base space using the fiber conditions.
        for lop, rop in space.images:
            rop = CoveringUnit(rop, space, rop.binding)
            yield Joint(lop, rop)


class ArrangeOrdered(Arrange):

    adapt(OrderedSpace)

    def __call__(self):
        # Start with strong ordering of the parent space.
        if self.with_strong:
            for code, direction in arrange(self.space.base,
                                           with_strong=True, with_weak=False):
                yield (code, direction)
            # Emit the ordering specified by the node itself.
            for code, direction in self.space.order:
                yield (code, direction)
        # Conclude with weak ordering of the parent space.
        if self.with_weak:
            for code, direction in arrange(self.space.base,
                                           with_strong=False, with_weak=True):
                yield (code, direction)


def arrange(space, with_strong=True, with_weak=True):
    """
    Returns the ordering of the given space.

    `space` (:class:`htsql.core.tr.space.Space`)
        The space to order.

    `with_strong` (Boolean)
        If set, include explicit space ordering.

    `with_weak` (Boolean)
        If set, include implicit space ordering.
    """
    order = []
    duplicates = set()
    for code, direction in Arrange.__invoke__(space, with_strong, with_weak):
        if code in duplicates:
            continue
        order.append((code, direction))
        duplicates.add(code)
    return order


def spread(space):
    """
    Returns native units of the given space.
    """
    return list(Spread.__invoke__(space))


def sew(space):
    return list(Sew.__invoke__(space))


def tie(space):
    return list(Tie.__invoke__(space))


