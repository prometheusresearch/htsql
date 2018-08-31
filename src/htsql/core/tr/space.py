#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ..util import (maybe, listof, tupleof, Clonable, Hashable, Printable,
        cachedproperty)
from ..entity import TableEntity, ColumnEntity, Join
from ..domain import Domain, BooleanDomain, ListDomain, IdentityDomain
from ..error import point
from .flow import Flow
from .signature import Signature, Bag, Formula


class Expression(Hashable, Clonable, Printable):
    """
    Represents an expression node.

    This is an abstract class; most of its subclasses belong to one of the
    two categories: *space* and *code* nodes (see :class:`Space` and
    :class:`Code`).

    A space graph is an intermediate phase of the HTSQL translator.  It is
    translated from the flow graph by the *encoding* process.  The space
    graph is used to *compile* the term tree and then *assemble* the frame
    structure.

    A space graph reflects the space structure of the HTSQL query: each
    expression node represents either a data space or an expression over
    a data space.

    Expression nodes support equality by value: that is, two expression
    nodes are equal if they are of the same type and all their (essential)
    attributes are equal.  Some attributes (e.g. `flow`) are not
    considered essential and do not participate in comparison.  By-value
    semantics is respected when expression nodes are used as dictionary
    keys.

    The constructor arguments:

    `flow` (:class:`htsql.core.tr.flow.Flow`)
        The flow node that gave rise to the expression; should be used
        only for presentation or error reporting.

    Other attributes:

    `syntax` (:class:`htsql.core.tr.syntax.Syntax`)
        The syntax node that gave rise to the expression; for debugging
        purposes only.

    `mark` (:class:`htsql.core.mark.Mark`)
        The location of the node in the original query; for error reporting.

    `hash` (an integer)
        The node hash; if two nodes are considered equal, their hashes
        must be equal too.
    """

    def __init__(self, flow):
        #assert isinstance(flow, Flow)
        self.flow = flow
        self.binding = flow.binding
        self.syntax = flow.binding.syntax
        point(self, flow)

    @cachedproperty
    def priority(self):
        return 0

    def __str__(self):
        # Display the syntex node that gave rise to the expression.
        return str(self.syntax)


class SegmentExpr(Expression):

    def __init__(self, root, space, codes, dependents, flow):
        assert isinstance(root, Space)
        assert isinstance(space, Space)
        assert isinstance(codes, listof(Code))
        assert isinstance(dependents, listof(SegmentExpr))
        assert isinstance(flow, Flow)
        super(SegmentExpr, self).__init__(flow)
        self.root = root
        self.space = space
        self.codes = codes
        self.dependents = dependents

    def __basis__(self):
        return (self.root, self.space, tuple(self.codes),
                tuple(self.dependents))


class Family:
    """
    Represents the target class of a space.

    The space family specifies the type of values produced by
    a space.  There are three distinct space families:

    - *scalar*, which indicates that the space produces
      scalar values;
    - *table*, which indicates that the space produces
      records from a database table;
    - *quotient*, which indicates that the space produces
      records from a derived *quotient* class.

    Class attributes:

    `is_scalar` (Boolean)
        Set for a scalar family.

    `is_table` (Boolean)
        Set for a table family.

    `is_quotient` (Boolean)
        Set for a quotient family.
    """

    is_scalar = False
    is_table = False
    is_quotient = False


class ScalarFamily(Family):
    """
    Represents a scalar space family.

    A scalar space produces values of a primitive type.
    """

    is_scalar = True


class TableFamily(Family):
    """
    Represents a table space family.

    A table space produces records from a database table.

    `table` (:class:`htsql.core.entity.TableEntity`)
        The table.
    """

    is_table = True

    def __init__(self, table):
        assert isinstance(table, TableEntity)
        self.table = table


class QuotientFamily(Family):
    """
    Represents a quotient space family.

    A quotient space produces records from a derived quotient class.

    The quotient class contains records formed from the kernel expressions
    as they run over the `seed` space.

    `seed` (:class:`Space`)
        The dividend space.

    `ground` (:class:`Space`)
        The ground space of the dividend.

    `kernels` (list of :class:`Code`)
        The kernel expressions of the quotient.
    """

    is_quotient = True

    def __init__(self, seed, ground, kernels):
        assert isinstance(seed, Space)
        assert isinstance(ground, Space)
        assert ground.is_axis and seed.concludes(ground)
        assert isinstance(kernels, listof(Code))
        self.seed = seed
        self.ground = ground
        self.kernels = kernels


class Space(Expression):
    """
    Represents a space node.

    A data space is a sequence of homogeneous values.  A space is generated
    by a series of space operations applied sequentially to the root space.

    Each space operation takes an input space as an argument and produces
    an output space as a result.  The operation transforms each element
    from the input row into zero, one, or more elements of the output
    space; the generating element is called *the origin* of the generated
    elements.  Thus, with every element of a space, we could associate
    a sequence of origin elements, one per each elementary space operation
    that together produce the space.

    Each instance of :class:`Space` represents a single space operation
    applied to some input space.  The `base` attribute of the instance
    represents the input space while the type of the instance and the
    other attributes reflect the properies of the operation.  The root
    space is denoted by an instance of:class:`RootSpace`, different
    subclasses of :class:`Space` correspond to different types of
    space operations.

    The type of values produced by a space is indicated by the `family`
    attribute.  We distinguish three space families: *scalar*, *table*
    and *quotient*.  A scalar space produces values of an elementary data
    type; a table space produces records of some table; a quotient space
    produces elements of a derived quotient class.

    Among others, we consider the following space operations:

    *The root space* `I`
        The initial space that contains one empty record.

    *A direct product* `A * T`
        Given a scalar space `A` and a table `T`, the direct product
        `A * T` generates all records of `T` for each element of `A`.

    *A fiber product* `A . T`
        Given an input space `A` that produces records of some table `S`
        and a table `T` linked to `S`, for each element of `A`,
        the fiber product `A . T` generates all associated records
        from `T`.

    *Filtering* `A ? p`
        Given a space `A` and a predicate `p` defined on `A`,
        the filtered space `A ? p` consists of all elements of `A`
        satisfying condition `p`.

    *Ordering* `A [e,...]`
        Given a space `A` and a list of expressions `e,...`, the
        ordered space `A [e,...]` consists of elements of `A` reordered
        by the values of `e,...`.

    *Quotient* `A ^ k`
        Given a space `A` and a kernel expression `k` defined on `A`,
        a quotient `A ^ k` produces all unique values of the kernel
        as it runs over `A`.

    Space operations for which the output space does not consist of
    elements of the input space are called *axial*.  If we take an
    arbitrary space `A`, disassemble it into individual operations,
    and then reapply only axial operations, we get the new space `A'`,
    which we call *the inflation* of `A`.  Note that elements of `A`
    form a subset of elements of `A'`.

    Now we can establish how different spaces are related to each other.
    Formally, for each pair of spaces `A` and `B`, we define a relation
    `<->` ("converges to") on elements from `A` and `B`, that is,
    a subset of the Cartesian product `A x B`, by the following rules:

    (1) For any space `A`, `<->` is the identity relation on `A`,
        that is, each element converges only to itself.

        For a space `A` and its inflation `A'`, each element from `A`
        converges to an equal element from `A'`.

    (2) Suppose `A` and `B` are spaces such that `A` is produced
        from `B` as a result of some axial space operation.  Then
        each element from `A` converges to its origin element
        from `B`.

        By transitivity, we could extend `<->` on `A` and any of its
        *ancestor spaces*, that is, the parent space of `A`, the
        parent of the parent of `A` and so on.

        In particular, this defines `<->` on an arbitrary space `A`
        and the root space `I` since `I` is an ancestor of any space.
        By the above definition, any element of `A` converges to
        the (only) record of `I`.

    (3) Finally, we are ready to define `<->` on an arbitrary pair
        of spaces `A` and `B`.  First, suppose that `A` and `B`
        share the same inflated space: `A' = B'`.  Then we could
        define `<->` on `A` and `B` transitively via `A'`: `a` from `A`
        converges to `b` from `B` if there exists `a'` from `A'` such
        that `a <-> a'` and `a' <-> b`.

        In the general case, find the closest ancestors `C` of `A`
        and `D` of `B` such that `C` and `D` have the same
        inflated space: `C' = D'`.  Rules `(1)` and `(2)` establish
        `<->` for the pairs `A` and `C`, `C` and `C' = D'`,
        `C' = D'` and `D`, and `D` and `B`.  We define `<->`
        on `A` and `B` transitively: `a` from `A` converges to
        `b` from `B` if there exist elements `c` from `C`,
        `c'` from `C' = D'`, `d` from `D` such that
        `a <-> c <-> c' <-> d <-> b`.

        Note that it is important that we take among the common inflated
        ancestors the closest one.  Any two spaces have a common inflated
        ancestor: the root space.  If the root space is, indeed, the closest
        common inflated ancestor of `A` and `B`, then each element of `A`
        converges to every element of `B`.

    Now we are ready to introduce several important relations between
    spaces:

    `A` *spans* `B`
        A space `A` spans a space `B` if for every element `a` from `A`:

            `card { b` from `B | a <-> b } <= 1`.

        Informally, it means that the statement::

            SELECT * FROM A

        and the statement::

            SELECT * FROM A LEFT OUTER JOIN B ON (A <-> B)

        produce the same number of rows.

    `A` *dominates* `B`
        A space `A` dominates a space `B` if `A` spans `B` and
        for every element `b` from `B`:

            `card { a` from `A | a <-> b } >= 1`.

        Informally, it implies that the statement::

            SELECT * FROM B INNER JOIN A ON (A <-> B)

        and the statement::

            SELECT * FROM B LEFT OUTER JOIN A ON (A <-> B)

        produce the same number of rows.

    `A` *conforms* `B`
        A space `A` conforms a space `B` if `A` dominates `B`
        and `B` dominates `A`.  Alternatively, we could say
        `A` conforms `B` if the `<->` relation establishes
        a bijection between `A` and `B`.

        Informally, it means that the statement::

            SELECT * FROM A

        and the statement::

            SELECT * FROM B

        produce the same number of rows.

        Note that `A` conforming `B` is not the same as `A` being equal
        to `B`; even if `A` conforms `B`,  elements of `A` and `B` may
        be of different types, therefore as sets, they are different.

    Now take an arbitrary space `A` and its parent space `B`.  We say:

    `A` *contracts* `B`
        A space `A` contracts its parent `B` if for any element from `B`
        there is no more than one converging element from `A`.

        Typically, it is non-axis spaces that contract their bases,
        although in some cases, an axis space could do it too.

    `A` *expands* `B`
        A space `A` expands its parent `B` if for any element from `B`
        there is at least one converging element from `A`.

        Note that it is possible that a space `A` both contracts and
        expands its base `B`, and also that `A` neither contracts
        nor expands `B`.  The former means that `A` conforms `B`.
        The latter holds, in particular, for the direct table space
        `A * T`.  `A * T` violates the contraction condition when
        `T` contains more than one record and violates the expansion
        condition when `T` has no records.

    A few words about how elements of a space are ordered.  The default
    (also called *weak*) ordering rules are:

    - a table space `T = I * T` is sorted by the lexicographic order
      of the table primary key;

    - a non-axial space keeps the order of its base;

    - an axial table space `A * T` or `A . T` respects the order its
      base `A`; records with the same origin are sorted by the table order.

    An alternative sort order could be specified explicitly (also called
    *strong* ordering).  Whenever strong ordering is  specified, it
    overrides the weak ordering.  Thus, elements of an ordered space `A [e]`
    are sorted first by expression `e`, and then elements which are not
    differentiated by `e` are sorted using the weak ordering of `A`.
    However, if `A` already has a strong ordering, it must be respected.
    Therefore, the general rule for sorting `A [e]` is:

    - first, sort the space by the strong ordering of `A`;

    - then, by `e`;

    - finally, by the weak ordering of `A`.

    Class attributes:

    `is_axis` (Boolean)
        Indicates whether the space is axial, that is, the elements
        of the space do not necessarily coincide with their origins.

    `is_root` (Boolean)
        Indicates that the space is the root space.

    The constructor arguments:

    `base` (:class:`Space` or ``None``)
        The parent input space; ``None`` for the root space.

    `family` (:class:`Family`)
        Specifies the type of the elements produced by the space.

    `is_contracting` (Boolean)
        Indicates if the space contracts its base space.

    `is_expanding` (Boolean)
        Indicates if the space expands its base space.

    Other attributes:

    `is_inflated` (Boolean)
        Indicates if the space is an inflation, that is, this space
        operation and all its ancestors are axial.
    """

    is_axis = False
    is_root = False
    is_commutative = True

    def __init__(self, base, family, is_contracting, is_expanding, flow):
        #assert isinstance(base, maybe(Space))
        #assert isinstance(family, Family)
        #assert isinstance(is_contracting, bool)
        #assert isinstance(is_expanding, bool)
        super(Space, self).__init__(flow)
        self.base = base
        self.family = family
        self.is_contracting = is_contracting
        self.is_expanding = is_expanding
        # Indicates that the space itself and all its ancestors are axes.
        self.is_inflated = (self.is_root or
                            (base.is_inflated and self.is_axis))

    def root(self):
        root = self
        while root.base is not None:
            root = root.base
        return root

    def unfold(self):
        """
        Produces a list of ancestor spaces.

        The method returns a list composed of the space itself,
        its base, the base of its base and so on.
        """
        ancestors = []
        ancestor = self
        while ancestor is not None:
            ancestors.append(ancestor)
            # Note: `ancestor.base` is None for the root space.
            ancestor = ancestor.base
        return ancestors

    def resembles(self, other):
        """
        Verifies if the spaces represent the same operation.

        Typically, it means that `self` and `other` have the same type
        and equal attributes, but may have different bases.
        """
        # We rely upon an assumption that the equality vector of a space node
        # is a tuple of all its essential attributes and the first element
        # of the tuple is the space base.  So we skip the base space and
        # compare the remaining attributes.
        if not isinstance(other, self.__class__):
            return False
        try:
            _basis = self._basis
        except AttributeError:
            self._rehash()
            _basis = self._basis
        try:
            _other_basis = other._basis
        except AttributeError:
            other._rehash()
            _other_basis = other._basis
        return (_basis[1:] == _other_basis[1:])

    def inflate(self):
        """
        Produces the inflation of the space.

        If we represent a space as a series of operations sequentially
        applied to the scalar space, the inflation of the space is obtained
        by ignoring any non-axial operations and applying axial operations
        only.
        """
        # Shortcut: check if the space is already an inflation.
        if self.is_inflated:
            return self
        # This is going to become a new inflated space.
        space = None
        # Iterate over all ancestors starting from the scalar space.
        for ancestor in reversed(self.unfold()):
            # Skip non-axial operations, reapply axial operations to
            # a new base.
            if ancestor.is_axis:
                space = ancestor.clone(base=space)
        # This is the inflated space now.
        return space

    def prune(self, other):
        """
        Prunes shared non-axial operations.

        Given spaces `A` and `B`, this function produces a new space
        `A'` such that `A` is a subset of `A'` and the convergence
        of `A` and `B` coincides with the convergence of `A'` and `B`.
        This is done by pruning any non-axial operations of `A` that
        also occur in `B`.
        """
        # Sanity check on the argument.
        assert isinstance(other, Space)
        # Shortcut: we cannot further prune an inflated space.
        if self.is_inflated:
            return self
        # Unfold the spaces into individual operations.
        my_ancestors = self.unfold()
        their_ancestors = other.unfold()
        # This is going to become the pruned space.
        space = None
        # Iterate until the ancestors are exhausted or diverged.
        while my_ancestors and their_ancestors:
            # Get the next operation.
            my_ancestor = my_ancestors[-1]
            their_ancestor = their_ancestors[-1]
            # Compare the ancestors.
            if my_ancestor.resembles(their_ancestor):
                # So both ancestors represent the same operation.
                # If it is an axis operation, apply it; otherwise,
                # discard it.
                # FIXME: may break if the space contains a non-matching
                # `limit/offset` operation?
                if not (my_ancestor.is_commutative or
                        my_ancestor == their_ancestor):
                    return self
                if my_ancestor.is_axis:
                    space = my_ancestor.clone(base=space)
                my_ancestors.pop()
                their_ancestors.pop()
            elif not their_ancestor.is_axis:
                # The ancestors represent different operations and `B`'s
                # ancestor is not an axis.  Discard it, we will try the
                # next ancestor.
                # FIXME: we may miss an opportunity to compare `B`'s ancestor
                # with other `A`'s ancestors.  It is not a big deal though,
                # we do not need to generate an optimal result here.
                their_ancestors.pop()
            elif not my_ancestor.is_axis:
                # The ancestors represent different operations, `B`'s ancestor
                # is an axis, and `A`'s ancestor is not.  Here we apply the
                # `A`'s ancestor.
                if not my_ancestor.is_commutative:
                    return self
                space = my_ancestor.clone(base=space)
                my_ancestors.pop()
            else:
                # The ancestors are both axial and differ from each other.
                # At this point, the ancestors diverge and are not
                # comparable anymore.  Break from the loop.
                break
        # Reapply the unprocessed ancestors.
        while my_ancestors:
            my_ancestor = my_ancestors.pop()
            if not my_ancestor.is_commutative:
                return self
            space = my_ancestor.clone(base=space)
        # We have a pruned space here.
        return space

    def spans(self, other):
        """
        Verifies if the space spans another space.
        """
        # Sanity check on the argument.
        assert isinstance(other, Space)
        # Shortcut: any space spans itself.
        if self == other:
            return True
        # Extract axial ancestors from both spaces.
        my_axes = [ancestor for ancestor in self.unfold()
                            if ancestor.is_axis]
        their_axes = [ancestor for ancestor in other.unfold()
                               if ancestor.is_axis]
        # Iterate until the axes are exhausted or diverged.
        while my_axes and their_axes:
            # Check if the next pair of axes represent the same operation.
            if my_axes[-1].resembles(their_axes[-1]):
                # If so, go compare the next pair of axes.
                my_axes.pop()
                their_axes.pop()
            else:
                # Otherwise, the axes diverge.
                break
        # At this point, all processed pairs of axes converge by identity.
        # If the other space has no more axes left, it is spanned.  Otherwise,
        # it is spanned only if its remaining unprocessed axes represent
        # contracting operations.
        for their_axis in their_axes:
            if not their_axis.is_contracting:
                return False
        return True

    def conforms(self, other):
        """
        Verifies if the space conforms another space.
        """
        # Sanity check on the argument.
        assert isinstance(other, Space)
        # Shortcut: any space conforms itself.
        if self == other:
            return True
        # Unfold the spaces into individual operations.
        my_ancestors = self.unfold()
        their_ancestors = other.unfold()
        # Iterate until the ancestors are exhausted or diverged.
        while my_ancestors and their_ancestors:
            # Get the next pair of ancestors.
            my_ancestor = my_ancestors[-1]
            their_ancestor = their_ancestors[-1]
            # Compare the ancestors.
            if my_ancestor.resembles(their_ancestor):
                # If the ancestors represent the same operation, we could
                # proceed to the next pair of ancestors.
                my_ancestors.pop()
                their_ancestors.pop()
            elif (my_ancestor.is_contracting and
                  my_ancestor.is_expanding and
                  not my_ancestor.is_axis):
                # Ok, the ancestors represent different operations, but
                # one of them is not an axis and does not change the
                # cardinality of its base.  We could skip this ancestor
                # and proceed further.
                my_ancestors.pop()
            elif (their_ancestor.is_contracting and
                  their_ancestor.is_expanding and
                  not their_ancestor.is_axis):
                # Same with the other ancestor.
                their_ancestors.pop()
            else:
                # The ancestors start to diverge; break from the loop.
                break
        # If all ancestors are processed, the spaces conform each other.
        # Otherwise, they conform each other only if the remaining unprocessed
        # ancestors do not change the cardinality of their bases.
        for ancestor in my_ancestors + their_ancestors:
            if not (ancestor.is_contracting and ancestor.is_expanding):
                return False
        return True

    def dominates(self, other):
        """
        Verifies if the space dominates another space.
        """
        # Sanity check on the argument.
        assert isinstance(other, Space)
        # Shortcut: any space dominates itself.
        if self == other:
            return True
        # Unfold the spaces into individual operations.
        my_ancestors = self.unfold()
        their_ancestors = other.unfold()
        # Iterate until the ancestors are exhausted or diverged.
        while my_ancestors and their_ancestors:
            # Get the next pair of ancestors.
            my_ancestor = my_ancestors[-1]
            their_ancestor = their_ancestors[-1]
            # Compare the ancestors.
            if my_ancestor.resembles(their_ancestor):
                # If the ancestors represent the same operation, we could
                # proceed to the next pair of ancestors.
                my_ancestors.pop()
                their_ancestors.pop()
            elif their_ancestor.is_contracting and not their_ancestor.is_axis:
                # We got ancestors representing different operations; however
                # the dominated ancestor represents a non-axis operation that
                # does not increase the cardinality of its base.  Therefore
                # we could ignore this ancestor and proceed further.
                their_ancestors.pop()
            else:
                # The ancestors start to diverge; break from the loop.
                break
        # If all ancestors are processed, the space dominates the other.
        # Otherwise, it is only possible if the remaining ancestors of
        # the space do not decrease the base cardinality while the
        # remaining ancestors of the other space do not increase the
        # base cardinality.
        for my_ancestor in my_ancestors:
            if not my_ancestor.is_expanding:
                return False
        for their_ancestor in their_ancestors:
            if not their_ancestor.is_contracting:
                return False
        return True

    def concludes(self, other):
        """
        Verifies if the other space is a ancestor of the space.
        """
        # Sanity check on the argument.
        assert isinstance(other, Space)
        # Iterate over all ancestors of the space comparing them with
        # the given other space.
        space = self
        while space is not None:
            if space == other:
                return True
            space = space.base
        # None of the ancestors matched, the spaces must be unrelated.
        return False


class RootSpace(Space):
    """
    Represents a root scalar space.

    A root space `I` contains one record ``()``.  Any other space is generated
    by applying a sequence of elementary space operations to `I`.

    `base` (always ``None``)
        The root space (and only the root space) has no parent space.
    """

    # Scalar space is an axial space.
    is_axis = True
    is_root = True

    def __init__(self, base, flow):
        # We keep `base` among constructor arguments despite it always being
        # equal to `None` to make
        #   space = space.clone(base=new_base)
        # work for all types of spaces.
        assert base is None
        # Note that we must satisfy the assumption that the first element
        # of the equality vector is the space base (used by `Space.resembles`).
        super(RootSpace, self).__init__(
                    base=None,
                    family=ScalarFamily(),
                    is_contracting=False,
                    is_expanding=False,
                    flow=flow)

    def __basis__(self):
        return (self.base,)

    def __str__(self):
        # Display a table expression in an algebraic form.
        return "I"


class ScalarSpace(Space):
    """
    Represents a link to the scalar class.

    Traversing a link to the scalar class produces an empty record ``()``
    for each element of the input space.
    """

    is_axis = True

    def __init__(self, base, flow):
        super(ScalarSpace, self).__init__(
                    base=base,
                    family=ScalarFamily(),
                    is_contracting=True,
                    is_expanding=True,
                    flow=flow)

    def __basis__(self):
        return (self.base,)

    def __str__(self):
        # Display:
        #   (<base> * I)
        return "(%s * I)" % self.base


class TableSpace(Space):
    """
    Represents a product of an input space to a table.

    A product operation generates a subset of a Cartesian product
    between the base space and records of a table.  This is an abstract
    class, see concrete subclasses :class:`DirectTableSpace` and
    :class:`FiberTableSpace`.

    `table` (:class:`htsql.core.entity.TableEntity`)
        The table.
    """

    # All subclasses of `TableSpace` are axial spaces.
    is_axis = True


class DirectTableSpace(TableSpace):
    """
    Represents a direct product between a scalar space and a table.

    A direct product `A * T` produces all records of the table `T`
    for each element of the input space `A`.

    `base` (:class:`Space`)
        The base space.

    `table` (:class:`htsql.core.entity.TableEntity`)
        The table.
    """

    def __init__(self, base, table, flow):
        assert isinstance(base, Space) and base.family.is_scalar
        super(DirectTableSpace, self).__init__(
                    base=base,
                    family=TableFamily(table),
                    is_contracting=False,
                    is_expanding=False,
                    flow=flow)
        self.table = table

    def __basis__(self):
        return (self.base, self.table)

    def __str__(self):
        # Display:
        #   (<base> * <schema>.<table>)
        return "(%s * %s)" % (self.base, self.family.table)


class FiberTableSpace(TableSpace):
    """
    Represents a fiber product between a table space and a linked table.

    Let `A` be a space producing records of table `S`, `j` be a join
    condition between tables `S` and `T`.  A fiber product `A .j T`
    (or `A . T` when the join condition is implied) of the space `A`
    and the table `T` is a sequence of records of `T` that for each
    record of `A` generates all records of `T` satisfying the join
    condition `j`.

    `base` (:class:`Space`)
        The base space.

    `join` (:class:`htsql.core.entity.Join`)
        The join condition.
    """

    def __init__(self, base, join, flow):
        assert isinstance(join, Join)
        # Check that the join origin is the table of the base space.
        assert isinstance(base, Space) and base.family.is_table
        assert base.family.table is join.origin, (base.family.table, join.origin)
        super(FiberTableSpace, self).__init__(
                    base=base,
                    family=TableFamily(join.target),
                    is_contracting=join.is_contracting,
                    is_expanding=join.is_expanding,
                    flow=flow)
        self.join = join

    def __basis__(self):
        return (self.base, self.join)

    def __str__(self):
        # Display:
        #   (<base> . <schema>.<table>)
        return "(%s . %s)" % (self.base, self.family.table)


class QuotientSpace(Space):
    """
    Represents a quotient operation.

    A quotient operation takes three arguments: an input space `A`,
    a seed space `S`, which should be a descendant of the input space,
    and a kernel expression `k` on the seed space.  For each element
    of the input space, the output space `A . (S ^ k)` generates unique
    values of `k` as it runs over convergent elements of `S`.

    `base` (:class:`Space`)
        The base space.

    `seed` (:class:`Space`)
        The seed space of the quotient; must be a descendant
        of the base space.

    `kernels` (a list of :class:`Code`)
        Kernel expressions of the quotient.

    `companions` (a list of :class:`Code`)
        An auxiliary hint to the compiler indicating that the term
        representing the space needs to export extra aggregate units.
        The value of this attribute has no effect on the semantics of
        the space graph and node comparison.

    Other attributes:

    `ground` (:class:`Space`)
        The closest axial ancestor of `seed` that is spanned
        by the `base` space.
    """

    is_axis = True

    def __init__(self, base, seed, kernels, flow,
                 companions=[]):
        assert isinstance(base, Space)
        assert isinstance(seed, Space)
        # Check that `seed` is a plural descendant of `base`.
        assert seed.spans(base)
        assert not base.spans(seed)
        assert isinstance(kernels, listof(Code))
        assert isinstance(companions, listof(Code))
        # Find an ancestor of `seed` that is spanned by `base`.
        ground = seed
        while not base.spans(ground.base):
            ground = ground.base
        # The quotient space conforms its base space only when
        # the kernel expression is constant.
        is_contracting = (not kernels)
        # FIXME: this is wrong, but the assembler relies on it
        # to collapse `GROUP BY` to the segment frame.
        is_expanding = (base.is_root and not kernels)
        super(QuotientSpace, self).__init__(
                    base=base,
                    family=QuotientFamily(seed, ground, kernels),
                    is_contracting=is_contracting,
                    is_expanding=is_expanding,
                    flow=flow)
        self.seed = seed
        self.ground = ground
        self.kernels = kernels
        self.companions = companions

    def __basis__(self):
        return (self.base, self.seed, tuple(self.kernels))

    def __str__(self):
        # Display:
        #   (<base> . (<seed> ^ {<kernels>}))
        return "(%s . (%s ^ {%s}))" % (self.base, self.seed,
                        ", ".join(str(kernel) for kernel in self.kernels))


class ComplementSpace(Space):
    """
    Represents a complement to a quotient.

    A complement takes a quotient as an input space and generates
    elements of the quotient seed.

    `base` (:class:`Space`)
        The base space.

    `companions` (list of :class:`Code`)
        An auxiliary hint to the compiler indicating that the term
        representing the space needs to export extra covering units.
        The value of this attribute has no effect on the semantics of
        the space graph and node comparison.

    Other attributes:

    `seed` (:class:`Space`)
        The seed space of the quotient.

    `ground` (:class:`Space`)
        The grond space of the quotient.

    `kernels` (list of :class:`Code`)
        Kernel expressions of the quotient.
    """

    is_axis = True

    def __init__(self, base, flow, companions=[]):
        assert isinstance(base, Space)
        assert base.family.is_quotient
        assert isinstance(companions, listof(Code))
        super(ComplementSpace, self).__init__(
                    base=base,
                    family=base.family.seed.family,
                    is_contracting=False,
                    is_expanding=True,
                    flow=flow)
        self.seed = base.family.seed
        self.ground = base.family.ground
        self.kernels = base.family.kernels
        self.companions = companions

    def __basis__(self):
        return (self.base,)

    def __str__(self):
        # Display:
        #   (<base> . ^)
        return "(%s . ^)" % self.base


class MonikerSpace(Space):
    """
    Represents an moniker operation.

    A moniker masks an arbitrary sequence of operations
    as a single axial space operation.

    `base` (:class:`Space`)
        The base space.

    `seed` (:class:`Space`)
        The seed space.

    `companions` (list of :class:`Code`)
        An auxiliary hint to the compiler indicating that the term
        representing the space must export extra covering units.
        The value of this attribute has no effect on the semantics of
        the space graph and node comparison.

    Other attributes:

    `ground` (:class:`Space`)
        The closest axial ancestor of `seed` spanned by `base`.
    """

    is_axis = True

    def __init__(self, base, seed, flow, companions=[]):
        assert isinstance(base, Space)
        assert isinstance(seed, Space)
        assert seed.spans(base)
        # We don't need `seed` to be plural or even axial against `base`.
        #assert not base.spans(seed)
        assert isinstance(companions, listof(Code))
        # Determine an axial ancestor of `seed` spanned by `base`
        # (could be `seed` itself).
        ground = seed
        while not ground.is_axis:
            ground = ground.base
        if not base.spans(ground):
            while not base.spans(ground.base):
                ground = ground.base
        super(MonikerSpace, self).__init__(
                    base=base,
                    family=seed.family,
                    is_contracting=base.spans(seed),
                    is_expanding=seed.dominates(base),
                    flow=flow)
        self.seed = seed
        self.ground = ground
        self.companions = companions

    def __basis__(self):
        return (self.base, self.seed)

    def __str__(self):
        # Display:
        #   (<base> . (<seed>))
        return "(%s . (%s))" % (self.base, self.seed)


class ForkedSpace(Space):
    """
    Represents a fork expression.

    A fork expression associated each element of the input space
    with every element of the input space sharing the same origin
    and values of the kernel expression.

    `base` (:class:`Space`)
        The base space.

    `seed` (:class:`Space`)
        The space to fork (typically coincides with the base space).

    `kernels` (list of :class:`Code`)
        The kernel expressions.

    `companions` (list of :class:`Code`)
        An auxiliary hint to the compiler indicating that the term
        representing the space must export extra covering units.
        The value of this attribute has no effect on the semantics of
        the space graph and node comparison.

    Other attributes:

    `ground` (:class:`Space`)
        The closest axial ancestor of the seed space.
    """

    is_axis = True

    def __init__(self, base, seed, kernels, flow, companions=[]):
        assert isinstance(base, Space)
        assert isinstance(seed, Space)
        assert isinstance(kernels, listof(Code))
        assert base.spans(seed) and seed.spans(base)
        # FIXME: this condition could be violated after the rewrite step
        # (also, equal-by-value is not implemented for `Family`):
        #assert base.family == seed.family
        # FIXME: we don't check for this constraint in the encoder anymore.
        #assert all(base.spans(unit.space) for code in kernels
        #                                  for unit in code.units)
        assert isinstance(companions, listof(Code))
        ground = seed
        while not ground.is_axis:
            ground = ground.base
        is_contracting = ground.is_contracting
        is_expanding = (not kernels and seed.dominates(base))
        super(ForkedSpace, self).__init__(
                    base=base,
                    family=base.family,
                    is_contracting=is_contracting,
                    is_expanding=is_expanding,
                    flow=flow)
        self.seed = seed
        self.ground = ground
        self.kernels = kernels
        self.companions = companions

    def __basis__(self):
        return (self.base, self.seed, tuple(self.kernels))

    def __str__(self):
        # Display:
        #   (<base> . fork({<kernels>}))
        return "(%s . fork({%s}))" \
                % (self.base, ", ".join(str(code) for code in self.kernels))


class AttachSpace(Space):
    """
    Represents a linking operation.

    A linking operation generates, for every element of the input space,
    convergent elements from the seed space with the same image value.

    `base` (:class:`Space`)
        The base space.

    `seed` (:class:`Space`)
        The seed space.

    `images` (list of pairs of :class:`Code`)
        Pairs of expressions forming a fiber join condition.

    `companions` (list of :class:`Code`)
        An auxiliary hint to the compiler indicating that the term
        representing the space must export extra covering units.
        The value of this attribute has no effect on the semantics of
        the space graph and node comparison.

    Other attributes:

    `ground` (:class:`Space`)
        The closest axial ancestor of `seed` spanned by `base`.
    """

    is_axis = True

    def __init__(self, base, seed, images, filter, flow, companions=[]):
        assert isinstance(base, Space)
        assert isinstance(seed, Space)
        assert seed.spans(base)
        assert not base.spans(seed)
        assert isinstance(images, listof(tupleof(Code, Code)))
        assert isinstance(filter, maybe(Code))
        if filter is not None:
            assert isinstance(filter.domain, BooleanDomain)
        assert isinstance(companions, listof(Code))
        ground = seed
        while not base.spans(ground.base):
            ground = ground.base
        super(AttachSpace, self).__init__(
                    base=base,
                    family=seed.family,
                    is_contracting=False,
                    is_expanding=False,
                    flow=flow)
        self.seed = seed
        self.ground = ground
        self.images = images
        self.filter = filter
        self.companions = companions

    def __basis__(self):
        return (self.base, self.seed, tuple(self.images), self.filter)

    def __str__(self):
        # Display:
        #   (<base> . ({<limages>} -> <seed>{<rimages>}))
        return "(%s . ({%s} -> %s{%s}))" \
                % (self.base, ", ".join(str(lop) for lop, rop in self.images),
                   self.seed, ", ".join(str(rop) for lop, rop in self.images))


class ClippedSpace(Space):

    is_axis = True

    def __init__(self, base, seed, limit, offset, flow, companions=[]):
        assert isinstance(base, Space)
        assert isinstance(seed, Space)
        assert seed.spans(base)
        assert not base.spans(seed)
        assert isinstance(limit, maybe(int))
        assert isinstance(offset, maybe(int))
        assert isinstance(companions, listof(Code))
        # Determine an axial ancestor of `seed` spanned by `base`.
        ground = seed
        while not ground.is_axis:
            ground = ground.base
        assert not base.spans(ground)
        while not base.spans(ground.base):
            ground = ground.base
        is_contracting = (limit is None)
        is_expanding = (seed.dominates(base) and offset is None
                        and (limit is None or limit > 0))
        super(ClippedSpace, self).__init__(
                    base=base,
                    family=seed.family,
                    is_contracting=is_contracting,
                    is_expanding=is_expanding,
                    flow=flow)
        self.seed = seed
        self.ground = ground
        self.limit = limit
        self.offset = offset
        self.companions = companions

    def __basis__(self):
        return (self.base, self.seed, self.limit, self.offset)

    def __str__(self):
        # Display:
        #   (<base> . (<seed>) [<offset>:<offset>+<limit>])
        return "(%s . (%s) [%s:%s+%s])" \
                % (self.base, self.seed,
                   self.offset if self.offset is not None else 0,
                   self.offset if self.offset is not None else 0,
                   self.limit if self.limit is not None else 1)


class LocatorSpace(AttachSpace):

    is_axis = True

    def __init__(self, base, seed, images, filter, flow, companions=[]):
        assert isinstance(base, Space)
        assert isinstance(seed, Space)
        assert isinstance(images, listof(tupleof(Code, Code)))
        assert isinstance(filter, maybe(Code))
        if filter is not None:
            assert isinstance(filter.domain, BooleanDomain)
        assert seed.spans(base)
        # We don't need `seed` to be plural or even axial against `base`.
        #assert not base.spans(seed)
        assert isinstance(companions, listof(Code))
        # Determine an axial ancestor of `seed` spanned by `base`
        # (could be `seed` itself).
        ground = seed
        while not ground.is_axis:
            ground = ground.base
        if not base.spans(ground):
            while not base.spans(ground.base):
                ground = ground.base
        axis = seed
        while not axis.is_axis:
            axis = axis.base
        is_contracting = (axis.base is None or base.spans(axis.base))
        # Note: skip Attach constructor.
        super(AttachSpace, self).__init__(
                    base=base,
                    family=seed.family,
                    is_contracting=is_contracting,
                    is_expanding=False,
                    flow=flow)
        self.seed = seed
        self.images = images
        self.filter = filter
        self.ground = ground
        self.companions = companions

    def __basis__(self):
        return (self.base, self.seed, tuple(self.images), self.filter)


class FilteredSpace(Space):
    """
    Represents a filtering operation.

    A filtered space `A ? f`, where `A` is the input space and `f` is
    a predicate expression on `A`, consists of rows of `A` satisfying
    the condition `f`.

    `base` (:class:`Space`)
        The base space.

    `filter` (:class:`Code`)
        The predicate expression.
    """

    def __init__(self, base, filter, flow):
        assert isinstance(filter, Code)
        assert isinstance(filter.domain, BooleanDomain)
        super(FilteredSpace, self).__init__(
                    base=base,
                    family=base.family,
                    is_contracting=True,
                    is_expanding=False,
                    flow=flow)
        self.filter = filter

    def __basis__(self):
        return (self.base, self.filter)

    def __str__(self):
        # Display:
        #   (<base> ? <filter>)
        return "(%s ? %s)" % (self.base, self.filter)


class OrderedSpace(Space):
    """
    Represents an ordered space.

    An ordered space `A [e,...;p:q]` is a space with explicitly specified
    strong ordering.  It also may extract a slice of the input space.

    `base` (:class:`Space`)
        The base space.

    `order` (a list of pairs `(code, direction)`)
        Expressions to sort the space by.

        Here `code` is a :class:`Code` instance, `direction` is either
        ``+1`` (indicates ascending order) or ``-1`` (indicates descending
        order).

    `limit` (a non-negative integer or ``None``)
        If set, the space extracts the first `limit` rows from the base
        space (with respect to the space ordering).  The remaining rows
        are discarded.

    `offset` (a non-negative integer or ``None``)
        If set, indicates that when extracting rows from the base space,
        the first `offset` rows should be skipped.
    """

    # FIXME: Non-commutativity of the ordered space may affect `prune`
    # and other functions.  Add class attribute `is_commutative`?
    # Or override `resembles` to return `True` only for equal nodes?

    def __init__(self, base, order, limit, offset, flow):
        assert isinstance(order, listof(tupleof(Code, int)))
        assert isinstance(limit, maybe(int))
        assert isinstance(offset, maybe(int))
        assert limit is None or limit >= 0
        assert offset is None or offset >= 0
        super(OrderedSpace, self).__init__(
                    base=base,
                    family=base.family,
                    is_contracting=True,
                    is_expanding=(limit is None and offset is None),
                    flow=flow)
        self.order = order
        self.limit = limit
        self.offset = offset
        self.is_commutative = (limit is None and offset is None)

    def __basis__(self):
        return (self.base, tuple(self.order))

    def __str__(self):
        # Display:
        #   <base> [<code>,...;<offset>:<limit>+<offset>]
        indicators = []
        if self.order:
            indicator = ",".join(str(code) for code, dir in self.order)
            indicators.append(indicator)
        if self.limit is not None and self.offset is not None:
            indicator = "%s:%s+%s" % (self.offset, self.offset, self.limit)
            indicators.append(indicator)
        elif self.limit is not None:
            indicator = ":%s" % self.limit
            indicators.append(indicator)
        elif self.offset is not None:
            indicator = "%s:" % self.offset
            indicators.append(indicator)
        indicators = ";".join(indicators)
        return "%s [%s]" % (self.base, indicators)


class Code(Expression):
    """
    Represents a code expression.

    A code expression is a function on spaces.  Specifically, it is a
    functional (possibly of several variables) that maps a space
    (or a Cartesian product of several spaces) to some scalar domain.
    :class:`Code` is an abstract base class for all code expressions;
    see its subclasses for concrete types of expressions.

    Among all code expressions, we distinguish *unit expressions*:
    elementary functions on spaces.  There are several kinds of units:
    among them are table columns and aggregate functions (see :class:`Unit`
    for more detail).  A non-unit code could be expressed as
    a composition of a scalar function and one or several units:

        `f = f(a,b,...) = F(u(a),v(b),...)`,

    where

    - `f` is a code expression;
    - `F` is a scalar function;
    - `a`, `b`, ... are elements of spaces `A`, `B`, ...;
    - `u`, `v`, ... are unit expressions on `A`, `B`, ....

    Note: special forms like `COUNT` or `EXISTS` are also expressed
    as code nodes.  Since they are not regular functions, special care
    must be taken to properly wrap them with appropriate
    :class:`ScalarUnit` and/or :class:`AggregateUnit` instances.

    `domain` (:class:`htsql.core.domain.Domain`)
        The co-domain of the code expression.

    `units` (a list of :class:`Unit`)
        The unit expressions of which the code is composed.
    """

    def __init__(self, domain, flow):
        #assert isinstance(domain, Domain)
        super(Code, self).__init__(flow)
        self.domain = domain

    @cachedproperty
    def priority(self):
        priority = 0
        for unit in self.units:
            priority += unit.priority
        return priority

    @cachedproperty
    def units(self):
        return self.get_units()

    def get_units(self):
        return []


class LiteralCode(Code):
    """
    Represents a literal value.

    `value` (valid type depends on the domain)
        The value.

    `domain` (:class:`htsql.core.domain.Domain`)
        The value type.
    """

    def __init__(self, value, domain, flow):
        super(LiteralCode, self).__init__(
                    domain=domain,
                    flow=flow)
        self.value = value

    def __basis__(self):
        if not isinstance(self.value, (list, dict)):
            return (self.value, self.domain)
        else:
            return (repr(self.value), self.domain)

    def __str__(self):
        # The actual value is often more helpful than the expression
        # that generated it.
        return repr(self.value)


class CastCode(Code):
    """
    Represents a type conversion operator.

    `base` (:class:`Code`)
        The expression to convert.

    `domain` (:class:`htsql.core.domain.Domain`)
        The target domain.
    """

    def __init__(self, base, domain, flow):
        super(CastCode, self).__init__(
                    domain=domain,
                    flow=flow)
        self.base = base

    def __basis__(self):
        return (self.base, self.domain)

    def get_units(self):
        return self.base.units


class FormulaCode(Formula, Code):
    """
    Represents a formula code.

    A formula code represents a function or an operator call as a code node.

    `signature` (:class:`htsql.core.tr.signature.Signature`)
        The signature of the formula.

    `domain` (:class:`Domain`)
        The co-domain of the formula.

    `arguments` (a dictionary)
        The arguments of the formula.

        Note that all the arguments become attributes of the node object.
    """

    def __init__(self, signature, domain, flow, **arguments):
        assert isinstance(signature, Signature)
        # Check that the arguments match the formula signature.
        arguments = Bag(**arguments)
        assert arguments.admits(Code, signature)
        # The first two arguments are processed by the `Formula`
        # constructor, the rest of them go to the `Flow` constructor.
        super(FormulaCode, self).__init__(
                    signature, arguments,
                    domain=domain,
                    flow=flow)

    def __basis__(self):
        return (self.signature, self.domain, self.arguments.freeze())

    def get_units(self):
        units = []
        for cell in self.arguments.cells():
            units.extend(cell.units)
        return units


class CorrelationCode(Code):

    def __init__(self, code):
        super(CorrelationCode, self).__init__(code.domain, code.flow)
        self.code = code

    def __basis__(self):
        return (self.code,)


class Unit(Code):
    """
    Represents a unit expression.

    A unit is an elementary function on a space.  There are several kinds
    of units; see subclasses :class:`ColumnUnit`, :class:`ScalarUnit`,
    :class:`AggregateUnit`, and :class:`CorrelatedUnit` for more detail.

    Units are divided into two categories: *primitive* and *compound*.

    A primitive unit is an intrinsic function of its space; no additional
    calculations are required to generate a primitive unit.  Currently,
    the only example of a primitive unit is :class:`ColumnUnit`.

    A compound unit requires calculating some non-intrinsic function
    on the target space.  Among compound units there are :class:`ScalarUnit`
    and :class:`AggregateUnit`, which correspond respectively to
    scalar and aggregate functions on a space.

    Note that it is easy to *lift* a unit code from one space to another.
    Specifically, suppose a unit `u` is defined on a space `A` and `B`
    is another space such that `B` spans `A`.  Then for each row `b`
    from `B` there is no more than one row `a` from `A` such that `a <-> b`.
    Therefore we could define `u` on `B` as follows:

    - `u(b) = u(a)` if there exists `a` from `A` such that `a <-> b`;
    - `u(b) =` ``NULL`` if there is no rows in `A` convergent to `b`.

    When a space `B` spans the space `A` of a unit `u`, we say that
    `u` is *singular* on `B`.  By the previous argument, `u` could be
    lifted to `B`.  Thus any unit is well-defined not only on the
    space where it is originally defined, but also on any space where
    it is singular.

    Attributes:

    `space` (:class:`Space`)
        The space on which the unit is defined.

    `domain` (:class:`htsql.core.domain.Domain`)
        The unit co-domain.

    Class attributes:

    `is_primitive` (Boolean)
        If set, indicates that the unit is primitive.

    `is_compound` (Boolean)
        If set, indicates that the unit is compound.
    """

    is_primitive = False
    is_compound = False

    def __init__(self, space, domain, flow):
        #assert isinstance(space, Space)
        super(Unit, self).__init__(
                    domain=domain,
                    flow=flow)
        self.space = space

    @cachedproperty
    def priority(self):
        return 0

    @property
    def units(self):
        # Use `property` instead of `cachedproperty` to avoid
        # creating a reference cycle.
        return [self]

    def singular(self, space):
        """
        Verifies if the unit is singular (well-defined) on the given space.
        """
        return space.spans(self.space)


class PrimitiveUnit(Unit):
    """
    Represents a primitive unit.

    A primitive unit is an intrinsic function on a space.

    This is an abstract class; for the (only) concrete subclass, see
    :class:`ColumnUnit`.
    """

    is_primitive = True


class CompoundUnit(Unit):
    """
    Represents a compound unit.

    A compound unit is some non-intrinsic function on a space.

    This is an abstract class; for concrete subclasses, see
    :class:`ScalarUnit`, :class:`AggregateUnit`, etc.

    `code` (:class:`Code`)
        The expression to evaluate on the unit space.
    """

    is_compound = True

    def __init__(self, code, space, domain, flow):
        assert isinstance(code, Code)
        super(CompoundUnit, self).__init__(
                    space=space,
                    domain=domain,
                    flow=flow)
        self.code = code


class ColumnUnit(PrimitiveUnit):
    """
    Represents a column unit.

    A column unit is a function on a space that returns a column of the
    prominent table of the space.

    `column` (:class:`htsql.core.entity.ColumnEntity`)
        The column produced by the unit.

    `space` (:class:`Space`)
        The unit space.  The space must be of a table family and the space
        table must coincide with the column table.
    """

    def __init__(self, column, space, flow):
        #assert isinstance(column, ColumnEntity)
        #assert (space.family.is_table and
        #        space.family.table == column.table)
        super(ColumnUnit, self).__init__(
                    space=space,
                    domain=column.domain,
                    flow=flow)
        self.column = column

    def __basis__(self):
        return (self.column, self.space)


class ScalarUnit(CompoundUnit):
    """
    Represents a scalar unit.

    A scalar unit is an expression evaluated in the specified space.

    Recall that any expression has the following form:

        `F(u(a),v(b),...)`,

    where

    - `F` is a scalar function;
    - `a`, `b`, ... are elements of spaces `A`, `B`, ...;
    - `u`, `v`, ... are unit expressions on `A`, `B`, ....

    We require that the units of the expression are singular on the given
    space.  If so, the expression units `u`, `v`, ... could be lifted to
    the given slace (see :class:`Unit`).  The scalar unit is defined as

        `F(u(x),v(x),...)`,

    where `x` is an element of the space where the scalar unit is defined.

    `code` (:class:`Code`)
        The expression to evaluate.

    `space` (:class:`Space`)
        The space on which the unit is defined.

    `companions` (list of :class:`Code`)
        An auxiliary hint to the compiler indicating that the term
        exporting the unit must also export extra scalar units.
        The value of this attribute has no effect on the semantics of
        the space graph and node comparison.
    """

    def __init__(self, code, space, flow, companions=[]):
        assert isinstance(companions, listof(Code))
        super(ScalarUnit, self).__init__(
                    code=code,
                    space=space,
                    domain=code.domain,
                    flow=flow)
        self.companions = companions

    @cachedproperty
    def priority(self):
        return 1 + len(self.companions)

    def __basis__(self):
        return (self.code, self.space)


class AggregateUnitBase(CompoundUnit):
    """
    Represents an aggregate unit.

    Aggregate units express functions on sets.  Specifically, let `A` and `B`
    be spaces such that `B` spans `A`, but  `A` does not span `B`, and
    let `g` be a function that takes subsets of `B` as an argument.  Then
    we could define an aggregate unit `u` on `A` as follows:

        `u(a) = g({b | a <-> b})`

    Here, for each row `a` from `A`, we take the subset of convergent
    rows from `B` and apply `g` to it; the result is the value of `u(a)`.

    The space `A` is the unit space, the space `B` is called *the plural
    space* of an aggregate unit, and `g` is called *the composite expression*
    of an aggregate unit.

    `code` (:class:`Code`)
        The composite expression of the aggregate unit.

    `plural_space` (:class:`Space`)
        The plural space of the aggregate unit, that is, the space
        which subsets form the argument of the composite expression.

    `space` (:class:`Space`)
        The space on which the unit is defined.
    """

    def __init__(self, code, plural_space, space, flow):
        assert isinstance(code, Code)
        assert isinstance(plural_space, Space)
        # FIXME: consider lifting the requirement that the plural
        # space spans the unit space.  Is it really necessary?
        assert plural_space.spans(space)
        assert not space.spans(plural_space)
        super(AggregateUnitBase, self).__init__(
                    code=code,
                    space=space,
                    domain=code.domain,
                    flow=flow)
        self.plural_space = plural_space

    def __basis__(self):
        return (self.code, self.plural_space, self.space)


class AggregateUnit(AggregateUnitBase):
    """
    Represents a regular aggregate unit.

    A regular aggregate unit is expressed in SQL using an aggregate
    expression with ``GROUP BY`` clause.

    `companions` (list of :class:`Code`)
        An auxiliary hint to the compiler indicating that the term
        exporting the unit must also export extra aggregate units.
        The value of this attribute has no effect on the semantics of
        the space graph and node comparison.
    """

    def __init__(self, code, plural_space, space, flow, companions=[]):
        assert isinstance(companions, listof(Code))
        super(AggregateUnit, self).__init__(code, plural_space, space, flow)
        self.companions = companions


class CorrelatedUnit(AggregateUnitBase):
    """
    Represents a correlated aggregate unit.

    A correlated aggregate unit is expressed in SQL using a correlated
    subquery.
    """


class KernelUnit(CompoundUnit):
    """
    Represents a value generated by a quotient space.

    A value generated by a quotient is either a part of a kernel
    expression or a unit from a ground space.

    `code` (:class:`Code`)
        An expression (calculated against the seed space of the quotient).

    `space` (:class:`Space`)
        The space of the quotient family on which the unit is defined.
    """

    def __init__(self, code, space, flow):
        assert space.family.is_quotient
        super(KernelUnit, self).__init__(
                    code=code,
                    space=space,
                    domain=code.domain,
                    flow=flow)

    def __basis__(self):
        return (self.code, self.space)


class CoveringUnit(CompoundUnit):
    """
    Represents a value generated by a covering space.

    A covering space represents another space expression as
    a single axial space operation.

    `code` (:class:`Code`)
        An expression (calculated against the seed space of
        the covering space).

    `space` (:class:`Space`)
        The space on which the unit is defined.
    """

    def __init__(self, code, space, flow):
        assert isinstance(space, (ComplementSpace,
                                 MonikerSpace,
                                 ForkedSpace,
                                 AttachSpace,
                                 ClippedSpace,
                                 LocatorSpace))
        super(CoveringUnit, self).__init__(
                    code=code,
                    space=space,
                    domain=code.domain,
                    flow=flow)

    def __basis__(self):
        return (self.code, self.space)


