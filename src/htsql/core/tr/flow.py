#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql.core.tr.flow`
=========================

This module declares flow and code nodes.
"""


from ..util import (maybe, listof, tupleof, Clonable, Comparable, Printable,
                    cachedproperty)
from ..entity import TableEntity, ColumnEntity, Join
from ..domain import Domain, BooleanDomain, ListDomain, IdentityDomain
from .binding import Binding, QueryBinding, SegmentBinding
from .signature import Signature, Bag, Formula


class Expression(Comparable, Clonable, Printable):
    """
    Represents an expression node.

    This is an abstract class; most of its subclasses belong to one of the
    two categories: *flow* and *code* nodes (see :class:`Flow` and
    :class:`Code`).

    A flow graph is an intermediate phase of the HTSQL translator.  It is
    translated from the binding graph by the *encoding* process.  The flow
    graph is used to *compile* the term tree and then *assemble* the frame
    structure.

    A flow graph reflects the flow structure of the HTSQL query: each
    expression node represents either a data flow or an expression over
    a data flow.

    Expression nodes support equality by value: that is, two expression
    nodes are equal if they are of the same type and all their (essential)
    attributes are equal.  Some attributes (e.g. `binding`) are not
    considered essential and do not participate in comparison.  By-value
    semantics is respected when expression nodes are used as dictionary
    keys.

    The constructor arguments:

    `binding` (:class:`htsql.core.tr.binding.Binding`)
        The binding node that gave rise to the expression; should be used
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

    def __init__(self, binding):
        assert isinstance(binding, Binding)
        self.binding = binding
        self.syntax = binding.syntax
        self.mark = binding.syntax.mark

    def __str__(self):
        # Display the syntex node that gave rise to the expression.
        return str(self.syntax)


class QueryExpr(Expression):
    """
    Represents the whole HTSQL query.

    `segment` (:class:`SegmentCode` or ``None``)
        The query segment.
    """

    def __init__(self, segment, binding):
        assert isinstance(segment, maybe(SegmentCode))
        assert isinstance(binding, QueryBinding)
        super(QueryExpr, self).__init__(binding)
        self.segment = segment


class Family(object):
    """
    Represents the target class of a flow.

    The flow family specifies the type of values produced by
    a flow.  There are three distinct flow families:

    - *scalar*, which indicates that the flow produces
      scalar values;
    - *table*, which indicates that the flow produces
      records from a database table;
    - *quotient*, which indicates that the flow produces
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
    Represents a scalar flow family.

    A scalar flow produces values of a primitive type.
    """

    is_scalar = True


class TableFamily(Family):
    """
    Represents a table flow family.

    A table flow produces records from a database table.

    `table` (:class:`htsql.core.entity.TableEntity`)
        The table.
    """

    is_table = True

    def __init__(self, table):
        assert isinstance(table, TableEntity)
        self.table = table


class QuotientFamily(Family):
    """
    Represents a quotient flow family.

    A quotient flow produces records from a derived quotient class.

    The quotient class contains records formed from the kernel expressions
    as they run over the `seed` flow.

    `seed` (:class:`Flow`)
        The dividend flow.

    `ground` (:class:`Flow`)
        The ground flow of the dividend.

    `kernels` (list of :class:`Code`)
        The kernel expressions of the quotient.
    """

    is_quotient = True

    def __init__(self, seed, ground, kernels):
        assert isinstance(seed, Flow)
        assert isinstance(ground, Flow)
        assert ground.is_axis and seed.concludes(ground)
        assert isinstance(kernels, listof(Code))
        self.seed = seed
        self.ground = ground
        self.kernels = kernels


class Flow(Expression):
    """
    Represents a flow node.

    A data flow is a sequence of homogeneous values.  A flow is generated
    by a series of flow operations applied sequentially to the root flow.

    Each flow operation takes an input flow as an argument and produces
    an output flow as a result.  The operation transforms each element
    from the input row into zero, one, or more elements of the output
    flow; the generating element is called *the origin* of the generated
    elements.  Thus, with every element of a flow, we could associate
    a sequence of origin elements, one per each elementary flow operation
    that together produce the flow.

    Each instance of :class:`Flow` represents a single flow operation
    applied to some input flow.  The `base` attribute of the instance
    represents the input flow while the type of the instance and the
    other attributes reflect the properies of the operation.  The root
    flow is denoted by an instance of:class:`RootFlow`, different
    subclasses of :class:`Flow` correspond to different types of
    flow operations.

    The type of values produced by a flow is indicated by the `family`
    attribute.  We distinguish three flow families: *scalar*, *table*
    and *quotient*.  A scalar flow produces values of an elementary data
    type; a table flow produces records of some table; a quotient flow
    produces elements of a derived quotient class.

    Among others, we consider the following flow operations:

    *The root flow* `I`
        The initial flow that contains one empty record.

    *A direct product* `A * T`
        Given a scalar flow `A` and a table `T`, the direct product
        `A * T` generates all records of `T` for each element of `A`.

    *A fiber product* `A . T`
        Given an input flow `A` that produces records of some table `S`
        and a table `T` linked to `S`, for each element of `A`,
        the fiber product `A . T` generates all associated records
        from `T`.

    *Filtering* `A ? p`
        Given a flow `A` and a predicate `p` defined on `A`,
        the filtered flow `A ? p` consists of all elements of `A`
        satisfying condition `p`.

    *Ordering* `A [e,...]`
        Given a flow `A` and a list of expressions `e,...`, the
        ordered flow `A [e,...]` consists of elements of `A` reordered
        by the values of `e,...`.

    *Quotient* `A ^ k`
        Given a flow `A` and a kernel expression `k` defined on `A`,
        a quotient `A ^ k` produces all unique values of the kernel
        as it runs over `A`.

    Flow operations for which the output flow does not consist of
    elements of the input flow are called *axial*.  If we take an
    arbitrary flow `A`, disassemble it into individual operations,
    and then reapply only axial operations, we get the new flow `A'`,
    which we call *the inflation* of `A`.  Note that elements of `A`
    form a subset of elements of `A'`.

    Now we can establish how different flows are related to each other.
    Formally, for each pair of flows `A` and `B`, we define a relation
    `<->` ("converges to") on elements from `A` and `B`, that is,
    a subset of the Cartesian product `A x B`, by the following rules:

    (1) For any flow `A`, `<->` is the identity relation on `A`,
        that is, each element converges only to itself.

        For a flow `A` and its inflation `A'`, each element from `A`
        converges to an equal element from `A'`.

    (2) Suppose `A` and `B` are flows such that `A` is produced
        from `B` as a result of some axial flow operation.  Then
        each element from `A` converges to its origin element
        from `B`.

        By transitivity, we could extend `<->` on `A` and any of its
        *ancestor flows*, that is, the parent flow of `A`, the
        parent of the parent of `A` and so on.

        In particular, this defines `<->` on an arbitrary flow `A`
        and the root flow `I` since `I` is an ancestor of any flow.
        By the above definition, any element of `A` converges to
        the (only) record of `I`.

    (3) Finally, we are ready to define `<->` on an arbitrary pair
        of flows `A` and `B`.  First, suppose that `A` and `B`
        share the same inflated flow: `A' = B'`.  Then we could
        define `<->` on `A` and `B` transitively via `A'`: `a` from `A`
        converges to `b` from `B` if there exists `a'` from `A'` such
        that `a <-> a'` and `a' <-> b`.

        In the general case, find the closest ancestors `C` of `A`
        and `D` of `B` such that `C` and `D` have the same
        inflated flow: `C' = D'`.  Rules `(1)` and `(2)` establish
        `<->` for the pairs `A` and `C`, `C` and `C' = D'`,
        `C' = D'` and `D`, and `D` and `B`.  We define `<->`
        on `A` and `B` transitively: `a` from `A` converges to
        `b` from `B` if there exist elements `c` from `C`,
        `c'` from `C' = D'`, `d` from `D` such that
        `a <-> c <-> c' <-> d <-> b`.

        Note that it is important that we take among the common inflated
        ancestors the closest one.  Any two flows have a common inflated
        ancestor: the root flow.  If the root flow is, indeed, the closest
        common inflated ancestor of `A` and `B`, then each element of `A`
        converges to every element of `B`.

    Now we are ready to introduce several important relations between
    flows:

    `A` *spans* `B`
        A flow `A` spans a flow `B` if for every element `a` from `A`:

            `card { b` from `B | a <-> b } <= 1`.

        Informally, it means that the statement::

            SELECT * FROM A

        and the statement::

            SELECT * FROM A LEFT OUTER JOIN B ON (A <-> B)

        produce the same number of rows.

    `A` *dominates* `B`
        A flow `A` dominates a flow `B` if `A` spans `B` and
        for every element `b` from `B`:

            `card { a` from `A | a <-> b } >= 1`.

        Informally, it implies that the statement::

            SELECT * FROM B INNER JOIN A ON (A <-> B)

        and the statement::

            SELECT * FROM B LEFT OUTER JOIN A ON (A <-> B)

        produce the same number of rows.

    `A` *conforms* `B`
        A flow `A` conforms a flow `B` if `A` dominates `B`
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

    Now take an arbitrary flow `A` and its parent flow `B`.  We say:

    `A` *contracts* `B`
        A flow `A` contracts its parent `B` if for any element from `B`
        there is no more than one converging element from `A`.

        Typically, it is non-axis flows that contract their bases,
        although in some cases, an axis flow could do it too.

    `A` *expands* `B`
        A flow `A` expands its parent `B` if for any element from `B`
        there is at least one converging element from `A`.

        Note that it is possible that a flow `A` both contracts and
        expands its base `B`, and also that `A` neither contracts
        nor expands `B`.  The former means that `A` conforms `B`.
        The latter holds, in particular, for the direct table flow
        `A * T`.  `A * T` violates the contraction condition when
        `T` contains more than one record and violates the expansion
        condition when `T` has no records.

    A few words about how elements of a flow are ordered.  The default
    (also called *weak*) ordering rules are:

    - a table flow `T = I * T` is sorted by the lexicographic order
      of the table primary key;

    - a non-axial flow keeps the order of its base;

    - an axial table flow `A * T` or `A . T` respects the order its
      base `A`; records with the same origin are sorted by the table order.

    An alternative sort order could be specified explicitly (also called
    *strong* ordering).  Whenever strong ordering is  specified, it
    overrides the weak ordering.  Thus, elements of an ordered flow `A [e]`
    are sorted first by expression `e`, and then elements which are not
    differentiated by `e` are sorted using the weak ordering of `A`.
    However, if `A` already has a strong ordering, it must be respected.
    Therefore, the general rule for sorting `A [e]` is:

    - first, sort the flow by the strong ordering of `A`;

    - then, by `e`;

    - finally, by the weak ordering of `A`.

    Class attributes:

    `is_axis` (Boolean)
        Indicates whether the flow is axial, that is, the elements
        of the flow do not necessarily coincide with their origins.

    `is_root` (Boolean)
        Indicates that the flow is the root flow.

    The constructor arguments:

    `base` (:class:`Flow` or ``None``)
        The parent input flow; ``None`` for the root flow.

    `family` (:class:`Family`)
        Specifies the type of the elements produced by the flow.

    `is_contracting` (Boolean)
        Indicates if the flow contracts its base flow.

    `is_expanding` (Boolean)
        Indicates if the flow expands its base flow.

    Other attributes:

    `is_inflated` (Boolean)
        Indicates if the flow is an inflation, that is, this flow
        operation and all its ancestors are axial.
    """

    is_axis = False
    is_root = False
    is_commutative = True

    def __init__(self, base, family, is_contracting, is_expanding, binding):
        assert isinstance(base, maybe(Flow))
        assert isinstance(family, Family)
        assert isinstance(is_contracting, bool)
        assert isinstance(is_expanding, bool)
        super(Flow, self).__init__(binding)
        self.base = base
        self.family = family
        self.is_contracting = is_contracting
        self.is_expanding = is_expanding
        # Indicates that the flow itself and all its ancestors are axes.
        self.is_inflated = (self.is_root or
                            (base.is_inflated and self.is_axis))

    def unfold(self):
        """
        Produces a list of ancestor flows.

        The method returns a list composed of the flow itself,
        its base, the base of its base and so on.
        """
        ancestors = []
        ancestor = self
        while ancestor is not None:
            ancestors.append(ancestor)
            # Note: `ancestor.base` is None for the root flow.
            ancestor = ancestor.base
        return ancestors

    def resembles(self, other):
        """
        Verifies if the flows represent the same operation.

        Typically, it means that `self` and `other` have the same type
        and equal attributes, but may have different bases.
        """
        # We rely upon an assumption that the equality vector of a flow node
        # is a tuple of all its essential attributes and the first element
        # of the tuple is the flow base.  So we skip the base flow and
        # compare the remaining attributes.
        return (isinstance(other, self.__class__) and
                self._basis[1:] == other._basis[1:])

    def inflate(self):
        """
        Produces the inflation of the flow.

        If we represent a flow as a series of operations sequentially
        applied to the scalar flow, the inflation of the flow is obtained
        by ignoring any non-axial operations and applying axial operations
        only.
        """
        # Shortcut: check if the flow is already an inflation.
        if self.is_inflated:
            return self
        # This is going to become a new inflated flow.
        flow = None
        # Iterate over all ancestors starting from the scalar flow.
        for ancestor in reversed(self.unfold()):
            # Skip non-axial operations, reapply axial operations to
            # a new base.
            if ancestor.is_axis:
                flow = ancestor.clone(base=flow)
        # This is the inflated flow now.
        return flow

    def prune(self, other):
        """
        Prunes shared non-axial operations.

        Given flows `A` and `B`, this function produces a new flow
        `A'` such that `A` is a subset of `A'` and the convergence
        of `A` and `B` coincides with the convergence of `A'` and `B`.
        This is done by pruning any non-axial operations of `A` that
        also occur in `B`.
        """
        # Sanity check on the argument.
        assert isinstance(other, Flow)
        # Shortcut: we cannot further prune an inflated flow.
        if self.is_inflated:
            return self
        # Unfold the flows into individual operations.
        my_ancestors = self.unfold()
        their_ancestors = other.unfold()
        # This is going to become the pruned flow.
        flow = None
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
                # FIXME: may break if the flow contains a non-matching
                # `limit/offset` operation?
                if not (my_ancestor.is_commutative or
                        my_ancestor == their_ancestor):
                    return self
                if my_ancestor.is_axis:
                    flow = my_ancestor.clone(base=flow)
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
                flow = my_ancestor.clone(base=flow)
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
            flow = my_ancestor.clone(base=flow)
        # We have a pruned flow here.
        return flow

    def spans(self, other):
        """
        Verifies if the flow spans another flow.
        """
        # Sanity check on the argument.
        assert isinstance(other, Flow)
        # Shortcut: any flow spans itself.
        if self == other:
            return True
        # Extract axial ancestors from both flows.
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
        # If the other flow has no more axes left, it is spanned.  Otherwise,
        # it is spanned only if its remaining unprocessed axes represent
        # contracting operations.
        for their_axis in their_axes:
            if not their_axis.is_contracting:
                return False
        return True

    def conforms(self, other):
        """
        Verifies if the flow conforms another flow.
        """
        # Sanity check on the argument.
        assert isinstance(other, Flow)
        # Shortcut: any flow conforms itself.
        if self == other:
            return True
        # Unfold the flows into individual operations.
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
        # If all ancestors are processed, the flows conform each other.
        # Otherwise, they conform each other only if the remaining unprocessed
        # ancestors do not change the cardinality of their bases.
        for ancestor in my_ancestors + their_ancestors:
            if not (ancestor.is_contracting and ancestor.is_expanding):
                return False
        return True

    def dominates(self, other):
        """
        Verifies if the flow dominates another flow.
        """
        # Sanity check on the argument.
        assert isinstance(other, Flow)
        # Shortcut: any flow dominates itself.
        if self == other:
            return True
        # Unfold the flows into individual operations.
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
        # If all ancestors are processed, the flow dominates the other.
        # Otherwise, it is only possible if the remaining ancestors of
        # the flow do not decrease the base cardinality while the
        # remaining ancestors of the other flow do not increase the
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
        Verifies if the other flow is a ancestor of the flow.
        """
        # Sanity check on the argument.
        assert isinstance(other, Flow)
        # Iterate over all ancestors of the flow comparing them with
        # the given other flow.
        flow = self
        while flow is not None:
            if flow == other:
                return True
            flow = flow.base
        # None of the ancestors matched, the flows must be unrelated.
        return False


class RootFlow(Flow):
    """
    Represents a root scalar flow.

    A root flow `I` contains one record ``()``.  Any other flow is generated
    by applying a sequence of elementary flow operations to `I`.

    `base` (always ``None``)
        The root flow (and only the root flow) has no parent flow.
    """

    # Scalar flow is an axial flow.
    is_axis = True
    is_root = True

    def __init__(self, base, binding):
        # We keep `base` among constructor arguments despite it always being
        # equal to `None` to make
        #   flow = flow.clone(base=new_base)
        # work for all types of flows.
        assert base is None
        # Note that we must satisfy the assumption that the first element
        # of the equality vector is the flow base (used by `Flow.resembles`).
        super(RootFlow, self).__init__(
                    base=None,
                    family=ScalarFamily(),
                    is_contracting=False,
                    is_expanding=False,
                    binding=binding)

    def __basis__(self):
        return (self.base,)

    def __str__(self):
        # Display a table expression in an algebraic form.
        return "I"


class ScalarFlow(Flow):
    """
    Represents a link to the scalar class.

    Traversing a link to the scalar class produces an empty record ``()``
    for each element of the input flow.
    """

    is_axis = True

    def __init__(self, base, binding):
        super(ScalarFlow, self).__init__(
                    base=base,
                    family=ScalarFamily(),
                    is_contracting=True,
                    is_expanding=True,
                    binding=binding)

    def __basis__(self):
        return (self.base,)

    def __str__(self):
        # Display:
        #   (<base> * I)
        return "(%s * I)" % self.base


class TableFlow(Flow):
    """
    Represents a product of an input flow to a table.

    A product operation generates a subset of a Cartesian product
    between the base flow and records of a table.  This is an abstract
    class, see concrete subclasses :class:`DirectTableFlow` and
    :class:`FiberTableFlow`.

    `table` (:class:`htsql.core.entity.TableEntity`)
        The table.
    """

    # All subclasses of `TableFlow` are axial flows.
    is_axis = True


class DirectTableFlow(TableFlow):
    """
    Represents a direct product between a scalar flow and a table.

    A direct product `A * T` produces all records of the table `T`
    for each element of the input flow `A`.

    `base` (:class:`Flow`)
        The base flow.

    `table` (:class:`htsql.core.entity.TableEntity`)
        The table.
    """

    def __init__(self, base, table, binding):
        assert isinstance(base, Flow) and base.family.is_scalar
        super(DirectTableFlow, self).__init__(
                    base=base,
                    family=TableFamily(table),
                    is_contracting=False,
                    is_expanding=False,
                    binding=binding)
        self.table = table

    def __basis__(self):
        return (self.base, self.table)

    def __str__(self):
        # Display:
        #   (<base> * <schema>.<table>)
        return "(%s * %s)" % (self.base, self.family.table)


class FiberTableFlow(TableFlow):
    """
    Represents a fiber product between a table flow and a linked table.

    Let `A` be a flow producing records of table `S`, `j` be a join
    condition between tables `S` and `T`.  A fiber product `A .j T`
    (or `A . T` when the join condition is implied) of the flow `A`
    and the table `T` is a sequence of records of `T` that for each
    record of `A` generates all records of `T` satisfying the join
    condition `j`.

    `base` (:class:`Flow`)
        The base flow.

    `join` (:class:`htsql.core.entity.Join`)
        The join condition.
    """

    def __init__(self, base, join, binding):
        assert isinstance(join, Join)
        # Check that the join origin is the table of the base flow.
        assert isinstance(base, Flow) and base.family.is_table
        assert base.family.table is join.origin
        super(FiberTableFlow, self).__init__(
                    base=base,
                    family=TableFamily(join.target),
                    is_contracting=join.is_contracting,
                    is_expanding=join.is_expanding,
                    binding=binding)
        self.join = join

    def __basis__(self):
        return (self.base, self.join)

    def __str__(self):
        # Display:
        #   (<base> . <schema>.<table>)
        return "(%s . %s)" % (self.base, self.family.table)


class QuotientFlow(Flow):
    """
    Represents a quotient operation.

    A quotient operation takes three arguments: an input flow `A`,
    a seed flow `S`, which should be a descendant of the input flow,
    and a kernel expression `k` on the seed flow.  For each element
    of the input flow, the output flow `A . (S ^ k)` generates unique
    values of `k` as it runs over convergent elements of `S`.

    `base` (:class:`Flow`)
        The base flow.

    `seed` (:class:`Flow`)
        The seed flow of the quotient; must be a descendant
        of the base flow.

    `kernels` (a list of :class:`Code`)
        Kernel expressions of the quotient.

    `companions` (a list of :class:`Code`)
        An auxiliary hint to the compiler indicating that the term
        representing the flow needs to export extra aggregate units.
        The value of this attribute has no effect on the semantics of
        the flow graph and node comparison.

    Other attributes:

    `ground` (:class:`Flow`)
        The closest axial ancestor of `seed` that is spanned
        by the `base` flow.
    """

    is_axis = True

    def __init__(self, base, seed, kernels, binding,
                 companions=[]):
        assert isinstance(base, Flow)
        assert isinstance(seed, Flow)
        # Check that `seed` is a plural descendant of `base`.
        assert seed.spans(base)
        assert not base.spans(seed)
        assert isinstance(kernels, listof(Code))
        assert isinstance(companions, listof(Code))
        # Find an ancestor of `seed` that is spanned by `base`.
        ground = seed
        while not base.spans(ground.base):
            ground = ground.base
        # The quotient flow conforms its base flow only when
        # the kernel expression is constant.
        is_contracting = (not kernels)
        # FIXME: this is wrong, but the assembler relies on it
        # to collapse `GROUP BY` to the segment frame.
        is_expanding = (base.is_root and not kernels)
        super(QuotientFlow, self).__init__(
                    base=base,
                    family=QuotientFamily(seed, ground, kernels),
                    is_contracting=is_contracting,
                    is_expanding=is_expanding,
                    binding=binding)
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


class ComplementFlow(Flow):
    """
    Represents a complement to a quotient.

    A complement takes a quotient as an input flow and generates
    elements of the quotient seed.

    `base` (:class:`Flow`)
        The base flow.

    `companions` (list of :class:`Code`)
        An auxiliary hint to the compiler indicating that the term
        representing the flow needs to export extra covering units.
        The value of this attribute has no effect on the semantics of
        the flow graph and node comparison.

    Other attributes:

    `seed` (:class:`Flow`)
        The seed flow of the quotient.

    `ground` (:class:`Flow`)
        The grond flow of the quotient.

    `kernels` (list of :class:`Code`)
        Kernel expressions of the quotient.
    """

    is_axis = True

    def __init__(self, base, binding, companions=[]):
        assert isinstance(base, Flow)
        assert base.family.is_quotient
        assert isinstance(companions, listof(Code))
        super(ComplementFlow, self).__init__(
                    base=base,
                    family=base.family.seed.family,
                    is_contracting=False,
                    is_expanding=True,
                    binding=binding)
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


class MonikerFlow(Flow):
    """
    Represents an moniker operation.

    A moniker masks an arbitrary sequence of operations
    as a single axial flow operation.

    `base` (:class:`Flow`)
        The base flow.

    `seed` (:class:`Flow`)
        The seed flow.

    `companions` (list of :class:`Code`)
        An auxiliary hint to the compiler indicating that the term
        representing the flow must export extra covering units.
        The value of this attribute has no effect on the semantics of
        the flow graph and node comparison.

    Other attributes:

    `ground` (:class:`Flow`)
        The closest axial ancestor of `seed` spanned by `base`.
    """

    is_axis = True

    def __init__(self, base, seed, binding, companions=[]):
        assert isinstance(base, Flow)
        assert isinstance(seed, Flow)
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
        super(MonikerFlow, self).__init__(
                    base=base,
                    family=seed.family,
                    is_contracting=base.spans(seed),
                    is_expanding=seed.dominates(base),
                    binding=binding)
        self.seed = seed
        self.ground = ground
        self.companions = companions

    def __basis__(self):
        return (self.base, self.seed)

    def __str__(self):
        # Display:
        #   (<base> . (<seed>))
        return "(%s . (%s))" % (self.base, self.seed)


class ForkedFlow(Flow):
    """
    Represents a fork expression.

    A fork expression associated each element of the input flow
    with every element of the input flow sharing the same origin
    and values of the kernel expression.

    `base` (:class:`Flow`)
        The base flow.

    `seed` (:class:`Flow`)
        The flow to fork (typically coincides with the base flow).

    `kernels` (list of :class:`Code`)
        The kernel expressions.

    `companions` (list of :class:`Code`)
        An auxiliary hint to the compiler indicating that the term
        representing the flow must export extra covering units.
        The value of this attribute has no effect on the semantics of
        the flow graph and node comparison.

    Other attributes:

    `ground` (:class:`Flow`)
        The closest axial ancestor of the seed flow.
    """

    is_axis = True

    def __init__(self, base, seed, kernels, binding, companions=[]):
        assert isinstance(base, Flow)
        assert isinstance(seed, Flow)
        assert isinstance(kernels, listof(Code))
        assert base.spans(seed) and seed.spans(base)
        # FIXME: this condition could be violated after the rewrite step
        # (also, equal-by-value is not implemented for `Family`):
        #assert base.family == seed.family
        # FIXME: we don't check for this constraint in the encoder anymore.
        #assert all(base.spans(unit.flow) for code in kernels
        #                                  for unit in code.units)
        assert isinstance(companions, listof(Code))
        ground = seed
        while not ground.is_axis:
            ground = ground.base
        is_contracting = ground.is_contracting
        is_expanding = (not kernels and seed.dominates(base))
        super(ForkedFlow, self).__init__(
                    base=base,
                    family=base.family,
                    is_contracting=is_contracting,
                    is_expanding=is_expanding,
                    binding=binding)
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


class LinkedFlow(Flow):
    """
    Represents a linking operation.

    A linking operation generates, for every element of the input flow,
    convergent elements from the seed flow with the same image value.

    `base` (:class:`Flow`)
        The base flow.

    `seed` (:class:`Flow`)
        The seed flow.

    `images` (list of pairs of :class:`Code`)
        Pairs of expressions forming a fiber join condition.

    `companions` (list of :class:`Code`)
        An auxiliary hint to the compiler indicating that the term
        representing the flow must export extra covering units.
        The value of this attribute has no effect on the semantics of
        the flow graph and node comparison.

    Other attributes:

    `ground` (:class:`Flow`)
        The closest axial ancestor of `seed` spanned by `base`.
    """

    is_axis = True

    def __init__(self, base, seed, images, binding, companions=[]):
        assert isinstance(base, Flow)
        assert isinstance(seed, Flow)
        assert seed.spans(base)
        assert not base.spans(seed)
        assert isinstance(images, listof(tupleof(Code, Code)))
        # FIXME: the constraint may be violated after rewriting.
        #assert all(base.spans(unit.flow) for lop, rop in images
        #                                 for unit in lop.units)
        #assert all(seed.spans(unit.flow) for lop, rop in images
        #                                 for unit in rop.units)
        assert isinstance(companions, listof(Code))
        ground = seed
        while not base.spans(ground.base):
            ground = ground.base
        super(LinkedFlow, self).__init__(
                    base=base,
                    family=seed.family,
                    is_contracting=False,
                    is_expanding=False,
                    binding=binding)
        self.seed = seed
        self.ground = ground
        self.images = images
        self.companions = companions

    def __basis__(self):
        return (self.base, self.seed, tuple(self.images))

    def __str__(self):
        # Display:
        #   (<base> . ({<limages>} -> <seed>{<rimages>}))
        return "(%s . ({%s} -> %s{%s}))" \
                % (self.base, ", ".join(str(lop) for lop, rop in self.images),
                   self.seed, ", ".join(str(rop) for lop, rop in self.images))


class ClippedFlow(Flow):

    is_axis = True

    def __init__(self, base, seed, limit, offset, binding, companions=[]):
        assert isinstance(base, Flow)
        assert isinstance(seed, Flow)
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
        super(ClippedFlow, self).__init__(
                    base=base,
                    family=seed.family,
                    is_contracting=is_contracting,
                    is_expanding=is_expanding,
                    binding=binding)
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


class LocatorFlow(Flow):

    is_axis = True

    def __init__(self, base, seed, filter, binding, companions=[]):
        assert isinstance(base, Flow)
        assert isinstance(seed, Flow)
        assert (isinstance(filter, Code) and
                isinstance(filter.domain, BooleanDomain))
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
        super(LocatorFlow, self).__init__(
                    base=base,
                    family=seed.family,
                    is_contracting=is_contracting,
                    is_expanding=False,
                    binding=binding)
        self.seed = seed
        self.filter = filter
        self.ground = ground
        self.companions = companions

    def __basis__(self):
        return (self.base, self.seed, self.filter)


class FilteredFlow(Flow):
    """
    Represents a filtering operation.

    A filtered flow `A ? f`, where `A` is the input flow and `f` is
    a predicate expression on `A`, consists of rows of `A` satisfying
    the condition `f`.

    `base` (:class:`Flow`)
        The base flow.

    `filter` (:class:`Code`)
        The predicate expression.
    """

    def __init__(self, base, filter, binding):
        assert isinstance(filter, Code)
        assert isinstance(filter.domain, BooleanDomain)
        super(FilteredFlow, self).__init__(
                    base=base,
                    family=base.family,
                    is_contracting=True,
                    is_expanding=False,
                    binding=binding)
        self.filter = filter

    def __basis__(self):
        return (self.base, self.filter)

    def __str__(self):
        # Display:
        #   (<base> ? <filter>)
        return "(%s ? %s)" % (self.base, self.filter)


class OrderedFlow(Flow):
    """
    Represents an ordered flow.

    An ordered flow `A [e,...;p:q]` is a flow with explicitly specified
    strong ordering.  It also may extract a slice of the input flow.

    `base` (:class:`Flow`)
        The base flow.

    `order` (a list of pairs `(code, direction)`)
        Expressions to sort the flow by.

        Here `code` is a :class:`Code` instance, `direction` is either
        ``+1`` (indicates ascending order) or ``-1`` (indicates descending
        order).

    `limit` (a non-negative integer or ``None``)
        If set, the flow extracts the first `limit` rows from the base
        flow (with respect to the flow ordering).  The remaining rows
        are discarded.

    `offset` (a non-negative integer or ``None``)
        If set, indicates that when extracting rows from the base flow,
        the first `offset` rows should be skipped.
    """

    # FIXME: Non-commutativity of the ordered flow may affect `prune`
    # and other functions.  Add class attribute `is_commutative`?
    # Or override `resembles` to return `True` only for equal nodes?

    def __init__(self, base, order, limit, offset, binding):
        assert isinstance(order, listof(tupleof(Code, int)))
        assert isinstance(limit, maybe(int))
        assert isinstance(offset, maybe(int))
        assert limit is None or limit >= 0
        assert offset is None or offset >= 0
        super(OrderedFlow, self).__init__(
                    base=base,
                    family=base.family,
                    is_contracting=True,
                    is_expanding=(limit is None and offset is None),
                    binding=binding)
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

    A code expression is a function on flows.  Specifically, it is a
    functional (possibly of several variables) that maps a flow
    (or a Cartesian product of several flows) to some scalar domain.
    :class:`Code` is an abstract base class for all code expressions;
    see its subclasses for concrete types of expressions.

    Among all code expressions, we distinguish *unit expressions*:
    elementary functions on flows.  There are several kinds of units:
    among them are table columns and aggregate functions (see :class:`Unit`
    for more detail).  A non-unit code could be expressed as
    a composition of a scalar function and one or several units:

        `f = f(a,b,...) = F(u(a),v(b),...)`,

    where

    - `f` is a code expression;
    - `F` is a scalar function;
    - `a`, `b`, ... are elements of flows `A`, `B`, ...;
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

    def __init__(self, domain, binding):
        assert isinstance(domain, Domain)
        super(Code, self).__init__(binding)
        self.domain = domain

    @cachedproperty
    def units(self):
        return self.get_units()

    @cachedproperty
    def segments(self):
        return self.get_segments()

    def get_units(self):
        return []

    def get_segments(self):
        return []


class SegmentCode(Code):
    """
    Represents a segment of an HTSQL query.

    `flow` (:class:`Flow`)
        The output flow of the segment.
    """

    def __init__(self, root, flow, code, binding):
        assert isinstance(root, Flow)
        assert isinstance(flow, Flow)
        assert isinstance(code, Code)
        assert isinstance(binding, SegmentBinding)
        super(SegmentCode, self).__init__(
                domain=ListDomain(code.domain),
                binding=binding)
        self.root = root
        self.flow = flow
        self.code = code

    def __basis__(self):
        return (self.root, self.flow, self.code)

    @property
    def segments(self):
        # Do not cache to avoid reference cycles.
        return [self]


class LiteralCode(Code):
    """
    Represents a literal value.

    `value` (valid type depends on the domain)
        The value.

    `domain` (:class:`htsql.core.domain.Domain`)
        The value type.
    """

    def __init__(self, value, domain, binding):
        super(LiteralCode, self).__init__(
                    domain=domain,
                    binding=binding)
        self.value = value

    def __basis__(self):
        return (self.value, self.domain)

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

    def __init__(self, base, domain, binding):
        super(CastCode, self).__init__(
                    domain=domain,
                    binding=binding)
        self.base = base

    def __basis__(self):
        return (self.base, self.domain)

    def get_units(self):
        return self.base.units

    def get_segments(self):
        return self.base.segments


class RecordCode(Code):

    def __init__(self, fields, domain, binding):
        assert isinstance(fields, listof(Code))
        super(RecordCode, self).__init__(
                domain=domain,
                binding=binding)
        self.fields = fields

    def __basis__(self):
        return (tuple(self.fields), self.domain)

    def get_units(self):
        units = []
        for field in self.fields:
            units.extend(field.units)
        return units

    def get_segments(self):
        segments = []
        for field in self.fields:
            segments.extend(field.segments)
        return segments


class IdentityCode(Code):

    def __init__(self, fields, binding):
        assert isinstance(fields, listof(Code))
        domain = IdentityDomain([field.domain for field in fields])
        super(IdentityCode, self).__init__(
                domain=domain,
                binding=binding)
        self.fields = fields

    def __basis__(self):
        return (tuple(self.fields),)

    def get_units(self):
        units = []
        for field in self.fields:
            units.extend(field.units)
        return units


class AnnihilatorCode(Code):

    def __init__(self, code, indicator, binding):
        assert isinstance(code, Code)
        assert isinstance(indicator, Unit)
        super(AnnihilatorCode, self).__init__(
                domain=code.domain,
                binding=binding)
        self.code = code
        self.indicator = indicator

    def __basis__(self):
        return (self.code, self.indicator)

    def get_units(self):
        return [self.indicator]+self.code.units

    def get_segments(self):
        return self.indicator.segments+self.code.segments


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

    def __init__(self, signature, domain, binding, **arguments):
        assert isinstance(signature, Signature)
        # Check that the arguments match the formula signature.
        arguments = Bag(**arguments)
        assert arguments.admits(Code, signature)
        # The first two arguments are processed by the `Formula`
        # constructor, the rest of them go to the `Binding` constructor.
        super(FormulaCode, self).__init__(
                    signature, arguments,
                    domain=domain,
                    binding=binding)

    def __basis__(self):
        return (self.signature, self.domain, self.arguments.freeze())

    def get_units(self):
        units = []
        for cell in self.arguments.cells():
            units.extend(cell.units)
        return units

    def get_segments(self):
        segments = []
        for cell in self.arguments.cells():
            segments.extend(cell.segments)
        return segments


class Unit(Code):
    """
    Represents a unit expression.

    A unit is an elementary function on a flow.  There are several kinds
    of units; see subclasses :class:`ColumnUnit`, :class:`ScalarUnit`,
    :class:`AggregateUnit`, and :class:`CorrelatedUnit` for more detail.

    Units are divided into two categories: *primitive* and *compound*.

    A primitive unit is an intrinsic function of its flow; no additional
    calculations are required to generate a primitive unit.  Currently,
    the only example of a primitive unit is :class:`ColumnUnit`.

    A compound unit requires calculating some non-intrinsic function
    on the target flow.  Among compound units there are :class:`ScalarUnit`
    and :class:`AggregateUnit`, which correspond respectively to
    scalar and aggregate functions on a flow.

    Note that it is easy to *lift* a unit code from one flow to another.
    Specifically, suppose a unit `u` is defined on a flow `A` and `B`
    is another flow such that `B` spans `A`.  Then for each row `b`
    from `B` there is no more than one row `a` from `A` such that `a <-> b`.
    Therefore we could define `u` on `B` as follows:

    - `u(b) = u(a)` if there exists `a` from `A` such that `a <-> b`;
    - `u(b) =` ``NULL`` if there is no rows in `A` convergent to `b`.

    When a flow `B` spans the flow `A` of a unit `u`, we say that
    `u` is *singular* on `B`.  By the previous argument, `u` could be
    lifted to `B`.  Thus any unit is well-defined not only on the
    flow where it is originally defined, but also on any flow where
    it is singular.

    Attributes:

    `flow` (:class:`Flow`)
        The flow on which the unit is defined.

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

    def __init__(self, flow, domain, binding):
        assert isinstance(flow, Flow)
        super(Unit, self).__init__(
                    domain=domain,
                    binding=binding)
        self.flow = flow

    @property
    def units(self):
        # Use `property` instead of `cachedproperty` to avoid
        # creating a reference cycle.
        return [self]

    def singular(self, flow):
        """
        Verifies if the unit is singular (well-defined) on the given flow.
        """
        return flow.spans(self.flow)


class PrimitiveUnit(Unit):
    """
    Represents a primitive unit.

    A primitive unit is an intrinsic function on a flow.

    This is an abstract class; for the (only) concrete subclass, see
    :class:`ColumnUnit`.
    """

    is_primitive = True


class CompoundUnit(Unit):
    """
    Represents a compound unit.

    A compound unit is some non-intrinsic function on a flow.

    This is an abstract class; for concrete subclasses, see
    :class:`ScalarUnit`, :class:`AggregateUnit`, etc.

    `code` (:class:`Code`)
        The expression to evaluate on the unit flow.
    """

    is_compound = True

    def __init__(self, code, flow, domain, binding):
        assert isinstance(code, Code)
        super(CompoundUnit, self).__init__(
                    flow=flow,
                    domain=domain,
                    binding=binding)
        self.code = code

    def get_segments(self):
        return self.code.segments


class ColumnUnit(PrimitiveUnit):
    """
    Represents a column unit.

    A column unit is a function on a flow that returns a column of the
    prominent table of the flow.

    `column` (:class:`htsql.core.entity.ColumnEntity`)
        The column produced by the unit.

    `flow` (:class:`Flow`)
        The unit flow.  The flow must be of a table family and the flow
        table must coincide with the column table.
    """

    def __init__(self, column, flow, binding):
        assert isinstance(column, ColumnEntity)
        assert (flow.family.is_table and
                flow.family.table == column.table)
        super(ColumnUnit, self).__init__(
                    flow=flow,
                    domain=column.domain,
                    binding=binding)
        self.column = column

    def __basis__(self):
        return (self.column, self.flow)


class ScalarUnit(CompoundUnit):
    """
    Represents a scalar unit.

    A scalar unit is an expression evaluated in the specified flow.

    Recall that any expression has the following form:

        `F(u(a),v(b),...)`,

    where

    - `F` is a scalar function;
    - `a`, `b`, ... are elements of flows `A`, `B`, ...;
    - `u`, `v`, ... are unit expressions on `A`, `B`, ....

    We require that the units of the expression are singular on the given
    flow.  If so, the expression units `u`, `v`, ... could be lifted to
    the given slace (see :class:`Unit`).  The scalar unit is defined as

        `F(u(x),v(x),...)`,

    where `x` is an element of the flow where the scalar unit is defined.

    `code` (:class:`Code`)
        The expression to evaluate.

    `flow` (:class:`Flow`)
        The flow on which the unit is defined.

    `companions` (list of :class:`Code`)
        An auxiliary hint to the compiler indicating that the term
        exporting the unit must also export extra scalar units.
        The value of this attribute has no effect on the semantics of
        the flow graph and node comparison.
    """

    def __init__(self, code, flow, binding, companions=[]):
        assert isinstance(companions, listof(Code))
        super(ScalarUnit, self).__init__(
                    code=code,
                    flow=flow,
                    domain=code.domain,
                    binding=binding)
        self.companions = companions

    def __basis__(self):
        return (self.code, self.flow)


class AggregateUnitBase(CompoundUnit):
    """
    Represents an aggregate unit.

    Aggregate units express functions on sets.  Specifically, let `A` and `B`
    be flows such that `B` spans `A`, but  `A` does not span `B`, and
    let `g` be a function that takes subsets of `B` as an argument.  Then
    we could define an aggregate unit `u` on `A` as follows:

        `u(a) = g({b | a <-> b})`

    Here, for each row `a` from `A`, we take the subset of convergent
    rows from `B` and apply `g` to it; the result is the value of `u(a)`.

    The flow `A` is the unit flow, the flow `B` is called *the plural
    flow* of an aggregate unit, and `g` is called *the composite expression*
    of an aggregate unit.

    `code` (:class:`Code`)
        The composite expression of the aggregate unit.

    `plural_flow` (:class:`Flow`)
        The plural flow of the aggregate unit, that is, the flow
        which subsets form the argument of the composite expression.

    `flow` (:class:`Flow`)
        The flow on which the unit is defined.
    """

    def __init__(self, code, plural_flow, flow, binding):
        assert isinstance(code, Code)
        assert isinstance(plural_flow, Flow)
        # FIXME: consider lifting the requirement that the plural
        # flow spans the unit flow.  Is it really necessary?
        assert plural_flow.spans(flow)
        assert not flow.spans(plural_flow)
        super(AggregateUnitBase, self).__init__(
                    code=code,
                    flow=flow,
                    domain=code.domain,
                    binding=binding)
        self.plural_flow = plural_flow

    def __basis__(self):
        return (self.code, self.plural_flow, self.flow)


class AggregateUnit(AggregateUnitBase):
    """
    Represents a regular aggregate unit.

    A regular aggregate unit is expressed in SQL using an aggregate
    expression with ``GROUP BY`` clause.

    `companions` (list of :class:`Code`)
        An auxiliary hint to the compiler indicating that the term
        exporting the unit must also export extra aggregate units.
        The value of this attribute has no effect on the semantics of
        the flow graph and node comparison.
    """

    def __init__(self, code, plural_flow, flow, binding, companions=[]):
        assert isinstance(companions, listof(Code))
        super(AggregateUnit, self).__init__(code, plural_flow, flow, binding)
        self.companions = companions


class CorrelatedUnit(AggregateUnitBase):
    """
    Represents a correlated aggregate unit.

    A correlated aggregate unit is expressed in SQL using a correlated
    subquery.
    """


class KernelUnit(CompoundUnit):
    """
    Represents a value generated by a quotient flow.

    A value generated by a quotient is either a part of a kernel
    expression or a unit from a ground flow.

    `code` (:class:`Code`)
        An expression (calculated against the seed flow of the quotient).

    `flow` (:class:`Flow`)
        The flow of the quotient family on which the unit is defined.
    """

    def __init__(self, code, flow, binding):
        assert flow.family.is_quotient
        super(KernelUnit, self).__init__(
                    code=code,
                    flow=flow,
                    domain=code.domain,
                    binding=binding)

    def __basis__(self):
        return (self.code, self.flow)


class CoveringUnit(CompoundUnit):
    """
    Represents a value generated by a covering flow.

    A covering flow represents another flow expression as
    a single axial flow operation.

    `code` (:class:`Code`)
        An expression (calculated against the seed flow of
        the covering flow).

    `flow` (:class:`Flow`)
        The flow on which the unit is defined.
    """

    def __init__(self, code, flow, binding):
        assert isinstance(flow, (ComplementFlow,
                                 MonikerFlow,
                                 ForkedFlow,
                                 LinkedFlow,
                                 ClippedFlow,
                                 LocatorFlow))
        super(CoveringUnit, self).__init__(
                    code=code,
                    flow=flow,
                    domain=code.domain,
                    binding=binding)

    def __basis__(self):
        return (self.code, self.flow)


class CorrelationCode(Code):

    def __init__(self, code):
        super(CorrelationCode, self).__init__(code.domain, code.binding)
        self.code = code

    def __basis__(self):
        return (self.code,)


