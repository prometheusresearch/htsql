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


from ..util import maybe, listof
from ..adapter import Adapter, adapts
from .error import AssembleError
from .code import (Expression, Code, Space, ScalarSpace, ProductSpace,
                   FilteredSpace, OrderedSpace, MaskedSpace,
                   Unit, ScalarUnit, ColumnUnit, AggregateUnit, CorrelatedUnit,
                   QueryExpression, SegmentExpression,
                   BatchExpression, ScalarBatchExpression,
                   AggregateBatchExpression)
from .term import (RoutingTerm, ScalarTerm, TableTerm, FilterTerm, JoinTerm,
                   CorrelationTerm, ProjectionTerm, OrderTerm, WrapperTerm,
                   SegmentTerm, QueryTerm, Tie, ParallelTie, SeriesTie)


class AssemblingState(object):
    """
    Encapsulates the state of the assembling process.

    State attributes:

    `scalar` (:class:`htsql.tr.code.ScalarSpace`)
        The scalar space.

    `baseline` (:class:`htsql.tr.code.Space`)
        When assembling a new term, indicates the leftmost axis that must
        exported by the term.  Note that the baseline space is always
        inflated.

    `mask` (:class:`htsql.tr.code.Space`)
        When assembling a new term, indicates that the term is going to be
        tie_termsed to a term that represents the `mask` space.
    """

    def __init__(self):
        # The next term tag to be produced by `tag`.
        self.next_tag = 1
        # The scalar space.
        self.scalar = None
        # The stack of previous baseline spaces.
        self.baseline_stack = []
        # The current baseline space.
        self.baseline = None
        # The stack of previous mask spaces.
        self.mask_stack = []
        # The current mask space.
        self.mask = None

    def tag(self):
        """
        Generates and returns a new unique term tag.
        """
        tag = self.next_tag
        self.next_tag += 1
        return tag

    def set_scalar(self, space):
        """
        Initializes the scalar, baseline and mask spaces.

        This function must be called before state attributes `scalar`,
        `baseline` and `mask` could be used.

        `space` (:class:`htsql.tr.code.ScalarSpace`)
            A scalar space.
        """
        assert isinstance(space, ScalarSpace)
        # Check that the state spaces are not yet initialized.
        assert self.scalar is None
        assert self.baseline is None
        assert self.mask is None
        self.scalar = space
        self.baseline = space
        self.mask = space

    def unset_scalar(self):
        """
        Clears the state spaces.
        """
        # Check that the state spaces are initialized and the space stacks
        # are exhausted.
        assert self.scalar is not None
        assert not self.baseline_stack
        assert self.baseline is self.scalar
        assert not self.mask_stack
        assert self.mask is self.scalar
        self.scalar = None
        self.baseline = None
        self.mask = None

    def push_baseline(self, baseline):
        """
        Sets a new baseline space.

        This function masks the current baseline space.  To restore
        the previous baseline space, use :meth:`pop_baseline`.

        `baseline` (:class:`htsql.tr.code.Space`)
            The new baseline space.  Note that the baseline space
            must be inflated.
        """
        assert isinstance(baseline, Space) and baseline.is_inflated
        self.baseline_stack.append(self.baseline)
        self.baseline = baseline

    def pop_baseline(self):
        """
        Restores the previous baseline space.
        """
        self.baseline = self.baseline_stack.pop()

    def push_mask(self, mask):
        """
        Sets a new mask space.

        This function hides the current mask space.  To restore the
        previous mask space, use :meth:`pop_mask`.

        `mask` (:class:`htsql.tr.code.Space`)
            The new mask space.
        """
        assert isinstance(mask, Space)
        self.mask_stack.append(self.mask)
        self.mask = mask

    def pop_mask(self):
        """
        Restores the previous mask space.
        """
        self.mask = self.mask_stack.pop()

    def assemble(self, expression, baseline=None, mask=None):
        """
        Assembles a new term node for the given expression.

        `expression` (:class:`htsql.tr.code.Expression`)
            An expression node.

        `baseline` (:class:`htsql.tr.code.Space` or ``None``)
            The baseline space.  Specifies an axis space that the assembled
            term must export.  If not set, the current baseline space of
            the state is used.

            When `expression` is a space, the generated term must
            export the space itself as well as all inflated prefixes
            up to the `baseline` space.  It may (but it is not required)
            export other axes as well.

        `mask` (:class:`htsql.tr.code.Space` or ``None``)
            The mask space.  Specifies the mask space against which
            a new term is assembled.  When not set, the current mask space
            of the state is used.

            A mask indicates that the new term is going to be tie_termsed
            to a term that represent the mask space.  Therefore the
            assembler could ignore any non-axis operations that are
            already enforced by the mask space.
        """
        # FIXME: potentially, we could implement a cache of `expression`
        # -> `term` to avoid generating the same term node more than once.
        # There are several complications though.  First, the term depends
        # not only on the expression, but also on the current baseline
        # and mask spaces.  Second, each assembled term must have a unique
        # tag, therefore we'd have to replace the tags and route tables
        # of the cached term node.
        return assemble(expression, self, baseline=baseline, mask=mask)

    def inject(self, term, expressions):
        """
        Augments a term to make it capable of producing the given expressions.

        This method takes a term node and a list of expressions.  It returns
        a term that could produce the same expressions as the given term, and,
        in addition, all the given expressions.

        Note that, technically, a term only exports unit expressions;
        we claim that a term could export an expression if it exports
        all the units of the expression.

        `term` (:class:`htsql.tr.term.RoutingTerm`)
            A term node.

        `expression` (a list of :class:`htsql.tr.code.Expression`)
            A list of expressions to inject into the given term.
        """
        assert isinstance(term, RoutingTerm)
        assert isinstance(expressions, listof(Expression))
        # Screen out expressions that the term could already export.
        # This filter only works with spaces and non-column units,
        # therefore the filtered list could still contain some
        # exportable expressions.
        expressions = [expression for expression in expressions
                                  if expression not in term.routes]
        # No expressions to inject, return the term unmodified.
        if not expressions:
            return term
        # At this moment, we could just apply the `Inject` adapter
        # sequentially to each of the expressions.  However, in some
        # cases, the assembler is able to generate a more optimal
        # term tree when it processes all units sharing the same
        # form simultaneously.  To handle this case, we collect
        # all expressions into an auxiliary expression node
        # `BatchExpression`.  When injected, the group expression
        # applies the multi-unit optimizations.
        if len(expressions) == 1:
            expression = expressions[0]
        else:
            expression = BatchExpression(expressions, term.binding)
        # Realize and apply the `Inject` adapter.
        inject = Inject(expression, term, self)
        return inject()


class Assemble(Adapter):
    """
    Translates an expression node to a term node.

    This is an interface adapter; see subclasses for implementations.

    The :class:`Assemble` adapter is implemented for two classes
    of expressions:

    - top-level expressions such as the whole query and the query segment,
      for which it builds respective top-level term nodes;

    - spaces, for which the adapter builds a corresponding relational
      algebraic expression.

    After a term is built, it is typically augmented using the
    :class:`Inject` adapter to have it export any exprected units.

    The :class:`Assemble` adapter has the following signature::

        Assemble: (Expression, AssemblingState) -> Term

    The adapter is polymorphic on the `Expression` argument.

    `expression` (:class:`htsql.tr.code.Expression`)
        An expression node.

    `state` (:class:`AssemblingState`)
        The current state of the assembling process.
    """

    adapts(Expression)

    def __init__(self, expression, state):
        assert isinstance(expression, Expression)
        assert isinstance(state, AssemblingState)
        self.expression = expression
        self.state = state

    def __call__(self):
        # This should never be reachable; if we are here, it indicates
        # either a bug or an incomplete implementation.  Since normally it
        # cannot be triggered by a user, we don't bother with generating
        # a user-level HTSQL exception.
        raise NotImplementedError("the assemble adapter is not implemented"
                                  " for a %r node" % self.expression)


class Inject(Adapter):
    """
    Augments a term to make it capable of producing the given expressions.

    This is an interface adapter; see subclasses for implementations.

    This adapter takes a term node and an expression (usually, a unit)
    and returns a new term (an augmentation of the given term) that is
    able to produce the given expression.

    The :class:`Inject` adapter has the following signature::

        Inject: (Expression, Term, AssemblingState) -> Term

    The adapter is polymorphic on the `Expression` argument.

    `expression` (:class:`htsql.tr.code.Expression`)
        An expression node to inject.

    `term` (:class:`htsql.tr.term.Term`)
        A term node to inject into.

    `state` (:class:`AssemblingState`)
        The current state of the assembling process.
    """

    adapts(Expression)

    def __init__(self, expression, term, state):
        assert isinstance(expression, Expression)
        assert isinstance(term, RoutingTerm)
        assert isinstance(state, AssemblingState)
        self.expression = expression
        self.term = term
        self.state = state

    def __call__(self):
        # Same as with `Assemble`, unless it's a bug or an incomplete
        # implementation, it should never be reachable.
        raise NotImplementedError("the inject adapter is not implemented"
                                  " for a %r node" % self.expression)

    # Utility functions used by implementations.

    def assemble_shoot(self, space, trunk_term, codes=None):
        """
        Assembles a term corresponding to the given space.

        The assembled term is called *a shoot term* (relatively to
        the given *trunk term*).

        `space` (:class:`htsql.tr.code.Space`)
            A space node, for which the we assemble a term.

        `trunk_term` (:class:`htsql.tr.term.RoutingTerm`)
           Expresses a promise that the assembled term will be
           (eventually) joined to `trunk_term` (see :meth:`join_terms`).

        `codes` (a list of :class:`htsql.tr.code.Expression` or ``None``)
           If provided, a list of expressions to be injected
           into the assembled term.
        """

        # Sanity check on the arguments.
        assert isinstance(space, Space)
        assert isinstance(trunk_term, RoutingTerm)
        assert isinstance(codes, maybe(listof(Expression)))

        # Determine the longest prefix of the space that either
        # contains no non-axis operations or has all its non-axis
        # operations enforced by the trunk space.  This prefix will
        # be used as the baseline of the assembled term (that is,
        # we ask the assembler not to generate any axes under
        # the baseline).

        # Start with removing any filters enforced by the trunk space.
        baseline = space.prune(trunk_term.space)
        # Now find the longest prefix that does not contain any
        # non-axis operations.
        while not baseline.is_inflated:
            baseline = baseline.base
        # Handle the case when the given space is not spanned by the
        # trunk space -- it happens when we construct a plural term
        # for an aggregate unit.  In this case, before joining it
        # to the trunk term, the shoot term will be projected to some
        # singular prefix of the given space.  To enable such projection,
        # at least the base of the shoot baseline must be spanned by
        # the trunk space (then, we can project on the columns of
        # a foreign key that attaches the baseline to its base).
        if not trunk_term.space.spans(baseline):
            while not trunk_term.space.spans(baseline.base):
                baseline = baseline.base

        # Assemble the term, use the found baseline and the trunk space
        # as the mask.
        term = self.state.assemble(space,
                                   baseline=baseline,
                                   mask=trunk_term.space)

        # If provided, inject the given expressions.
        if codes is not None:
            term = self.state.inject(term, codes)

        # SQL syntax does not permit us evaluating scalar or aggregate
        # expressions in terminal terms.  So when generating terms for
        # scalar or aggregate units, we need to cover terminal terms
        # with a no-op wrapper.  It may generate an unnecessary
        # wrapper if the term is used for other purposes, but we rely
        # on the outliner to flatten any unused wrappers.
        if term.is_nullary:
            term = WrapperTerm(self.state.tag(), term,
                               term.space, term.routes.copy())

        # Return the assembled shoot term.
        return term

    def tie_terms(self, trunk_term, shoot_term):
        """
        Returns ties to attach the shoot term to the trunk term.

        `trunk_term` (:class:`htsql.tr.term.RoutingTerm`)
            The left (trunk) operand of the join.

        `shoot_term` (:class:`htsql.tr.term.RoutingTerm`)
            The right (shoot) operand of the join.

        Note that the trunk term may not export all the units necessary
        to generate tie conditions.  Apply :meth:`inject_ties` on the trunk
        before using the ties to join the trunk and the shoot.
        """
        # Sanity check on the arguments.
        assert isinstance(trunk_term, RoutingTerm)
        assert isinstance(shoot_term, RoutingTerm)
        # Verify that it is possible to join the terms without
        # changing the cardinality of the trunk.
        assert (shoot_term.baseline.is_scalar or
                trunk_term.space.spans(shoot_term.baseline.base))

        # There are two ways the ties are generated:
        #
        # - when the shoot baseline is an axis of the trunk space,
        #   in this case we join the terms using parallel ties on
        #   the common axes;
        # - otherwise, join the terms using a series tie between
        #   the shoot baseline and its base.

        # Ties to attach the shoot to the trunk.
        ties = []
        # Check if the shoot baseline is an axis of the trunk space.
        if trunk_term.backbone.concludes(shoot_term.baseline):
            # In this case, we join the terms by all axes of the trunk
            # space that are exported by the shoot term.
            # Find the first inflated axis of the trunk exported
            # by the shoot.
            axis = trunk_term.backbone
            while axis not in shoot_term.routes:
                axis = axis.base
            # Now the axes between `axis` and `baseline` are common axes
            # of the trunk space and the shoot term.  For each of them,
            # generate a parallel tie.  Note that we do not verify
            # (and, in general, it is not required) that these axes
            # are exported by the trunk term.  Apply `inject_ties()` on
            # the trunk term before using the ties to join the terms.
            while axis != shoot_term.baseline.base:
                assert axis in shoot_term.routes
                # Skip non-expanding axes (but always include the baseline).
                if axis.is_expanding or axis == shoot_term.baseline:
                    tie = ParallelTie(axis)
                    ties.append(tie)
                axis = axis.base
            # We prefer (for no particular reason) the ties to go
            # from inner to outer axes.
            ties.reverse()
        else:
            # When shoot does not touch the trunk space, we attach it
            # using a series tie between the shoot baseline and its base.
            # Note that we do not verify (and it is not required) that
            # the trunk term export the base space.  Apply `inject_ties()`
            # on the trunk term to inject any necessary spaces before
            # joining the terms using the ties.
            tie = SeriesTie(shoot_term.baseline)
            ties.append(tie)

        # Return the generated ties.
        return ties

    def inject_ties(self, term, ties):
        """
        Augments the term to ensure it can export all units required
        to generate tie conditions.

        `term` (:class:`htsql.tr.term.RoutingTerm`)
            The term to update.

            It is assumed that `term` was the argument `trunk_term` of
            :meth:`tie_terms` when the ties were generated.

        `ties` (a list of :class:`htsql.tr.term.Tie`)
            The ties to inject.

            It is assumed the ties were generated by :meth:`tie_terms`.
        """
        # Sanity check on the arguments.
        assert isinstance(term, RoutingTerm)
        assert isinstance(ties, listof(Tie))

        # Accumulate the axes we need to inject.
        axes = []
        # Iterate over the ties.
        for tie in ties:
            # For a parallel tie, we inject the tie space.
            if tie.is_parallel:
                axes.append(tie.space)
            # For a series tie, we inject the base of the tie space
            # (the tie space itself goes to the shoot term).
            if tie.is_series:
                if tie.is_backward:
                    # Not really reachable since we never generate backward
                    # ties in `tie_term()`.  It is here for completeness.
                    axes.append(tie.space)
                else:
                    axes.append(tie.space.base)

        # Inject the required spaces and return the updated term.
        return self.state.inject(term, axes)

    def join_terms(self, trunk_term, shoot_term, extra_routes):
        """
        Attaches a shoot term to a trunk term.

        The produced join term uses the space and the routing
        table of the trunk term, but also includes the given
        extra routes.

        `trunk_term` (:class:`htsql.tr.term.RoutingTerm`)
            The left (trunk) operand of the join.

        `shoot_term` (:class:`htsql.tr.term.RoutingTerm`)
            The right (shoot) operand of the term.

            The shoot term must be singular relatively to the trunk term.

        `extra_routes` (a mapping from a unit/space to a term tag)
            Any extra routes provided by the join.
        """
        # Sanity check on the arguments.
        assert isinstance(trunk_term, RoutingTerm)
        assert isinstance(shoot_term, RoutingTerm)
        # FIXME: Unfortunately, we cannot properly verify that the trunk
        # space spans the shoot space since the term space is generated
        # incorrectly for projection terms.
        #assert trunk_term.space.dominates(shoot_term.space)
        assert isinstance(extra_routes, dict)

        # Ties to attach the terms.
        ties = self.tie_terms(trunk_term, shoot_term)
        # Make sure the trunk term could export tie conditions.
        trunk_term = self.inject_ties(trunk_term, ties)
        # Determine if we could use an inner join to attach the shoot
        # to the trunk.  We could do it if the inner join does not
        # decrease cardinality of the trunk.
        # FIXME: The condition that the shoot space dominates the
        # trunk space is sufficient, but not really necessary.
        # In general, we can use the inner join if the shoot space
        # dominates the prefix of the trunk space cut at the longest
        # common axis of trunk and the shoot spaces.
        is_inner = shoot_term.space.dominates(trunk_term.space)
        # Use the routing table of the trunk term, but also add
        # the given extra routes.
        routes = trunk_term.routes.copy()
        routes.update(extra_routes)
        # Generate and return a join term.
        return JoinTerm(self.state.tag(), trunk_term, shoot_term,
                        ties, is_inner, trunk_term.space, routes)


class AssembleQuery(Assemble):
    """
    Assembles a top-level query term.
    """

    adapts(QueryExpression)

    def __call__(self):
        # Assemble the segment term.
        segment = None
        if self.expression.segment is not None:
            segment = self.state.assemble(self.expression.segment)
        # Construct a query term.
        return QueryTerm(self.state.tag(), segment, self.expression)


class AssembleSegment(Assemble):
    """
    Assembles a segment term.
    """

    adapts(SegmentExpression)

    def __call__(self):
        # Initialize the all state spaces with a scalar space.
        self.state.set_scalar(self.expression.space.scalar)
        # Construct a term corresponding to the segment space.
        kid = self.state.assemble(self.expression.space)
        # Get the ordering of the segment space.
        order = self.expression.space.ordering()
        # List of expressions we need the term to export.
        codes = self.expression.elements + [code for code, direction in order]
        # Inject the expressions into the term.
        kid = self.state.inject(kid, codes)
        # The assembler does not guarantee that the produced term respects
        # the space ordering, so it is our responsitibity to wrap the term
        # with an order node.
        if order:
            kid = OrderTerm(self.state.tag(), kid, order, None, None,
                            kid.space, kid.routes.copy())
        # Shut down the state spaces.
        self.state.unset_scalar()
        # Construct a segment term.
        return SegmentTerm(self.state.tag(), kid, self.expression.elements,
                           kid.space, kid.routes.copy())


class AssembleSpace(Assemble):
    """
    Assemble a term corresponding to a space node.

    This is an abstract class; see subclasses for implementations.

    The general algorithm for assembling a term node for the given space
    looks as follows:

    - assemble a term for the base space;
    - inject any necessary expressions;
    - build a new term node that represents the space operation.

    When assembling terms, the following optimizations are applied:

    Removing unnecessary non-axis operations.  The current `mask` space
    expresses a promise that the generated term will be tie_termsed to
    a term representing the mask space.  Therefore the assembler
    could skip any non-axis filters that are already enforced by
    the mask space.

    Removing unnecessary axis operations.  The current `baseline` space
    denotes the leftmost axis that the term should be able to export.
    The assembler may (but does not have to) omit any axes nested under
    the `baseline` axis.

    Because of these optimizations, the shape and cardinality of the
    term rows may differ from that of the space.  Additionally, the
    term is not required to respect the ordering of its space.

    Constructor arguments:

    `space` (:class:`htsql.tr.code.Space`)
        A space node.

    `state` (:class:`AssemblingState`)
        The current state of the assembling process.

    Other attributes:

    `backbone` (:class:`htsql.tr.code.Space`)
        The inflation of the given space.

    `baseline` (:class:`htsql.tr.code.Space`)
        An alias to `state.baseline`.

    `mask` (:class:`htsql.tr.code.Space`)
        An alias to `state.mask`.
    """

    adapts(Space)

    def __init__(self, space, state):
        assert isinstance(space, Space)
        # The inflation of the space.
        backbone = space.inflate()
        # Check that the baseline space is an axis of the given space.
        assert backbone.concludes(state.baseline)
        super(AssembleSpace, self).__init__(space, state)
        self.space = space
        self.state = state
        self.backbone = backbone
        # Extract commonly used state properties.
        self.baseline = state.baseline
        self.mask = state.mask


class InjectSpace(Inject):

    adapts(Space)

    def __init__(self, space, term, state):
        assert isinstance(space, Space)
        assert term.space.spans(space)
        super(InjectSpace, self).__init__(space, term, state)
        self.space = space
        self.term = term
        self.state = state

    def __call__(self):
        if self.space in self.term.routes:
            return self.term
        unmasked_space = self.space.prune(self.term.space)
        if unmasked_space in self.term.routes:
            routes = self.term.routes.copy()
            routes[self.space] = routes[unmasked_space]
            return self.term.clone(routes=routes)
        if self.term.backbone.concludes(unmasked_space):
            next_axis = self.term.baseline
            while next_axis.base != unmasked_space:
                next_axis = next_axis.base
            lkid = self.state.inject(self.term, [next_axis])
            assert unmasked_space not in lkid.routes
            rkid = self.state.assemble(unmasked_space,
                                       baseline=unmasked_space,
                                       mask=unmasked_space.scalar)
            assert unmasked_space.base not in rkid.routes
            tie = SeriesTie(next_axis, is_backward=True)
            routes = lkid.routes.copy()
            routes[unmasked_space] = rkid[unmasked_space]
            routes[self.space] = rkid[unmasked_space]
            return JoinTerm(self.state.tag(), lkid, rkid, [tie], True,
                            lkid.space, routes)
        space_term = self.assemble_shoot(self.space, self.term)
        extra_routes = {}
        extra_routes[self.space] = space_term.routes[self.space]
        extra_routes[unmasked_space] = space_term.routes[self.space]
        return self.join_terms(self.term, space_term, extra_routes)


class AssembleScalar(AssembleSpace):
    """
    Assembles a term corresponding to a scalar space.
    """

    adapts(ScalarSpace)

    def __call__(self):
        # Generate a `ScalarTerm` instance.
        tag = self.state.tag()
        routes = { self.space: tag }
        return ScalarTerm(tag, self.space, routes)


class AssembleProduct(AssembleSpace):
    """
    Assembles a term corresponding to a (cross or join) product space.
    """

    adapts(ProductSpace)

    def __call__(self):
        # We start with identifying and handling special cases, where
        # we able to generate a more optimal, less compex term tree than
        # in the regular case.  If none of the special cases are applicable,
        # we use the generic algorithm.

        # The first special case: the given space is the leftmost axis
        # we must export.  Since `baseline` is always an inflated space,
        # we need to compare it with the inflation of the given space
        # rather than with the space itself.
        if self.backbone == self.baseline:
            # Generate a table term that exports rows from the prominent
            # table.
            tag = self.state.tag()
            # The routing table must always include the term space, and also,
            # for any space it includes, the inflation of the space.
            # In this case, `self.space` is the term space, `self.backbone`
            # is its inflation.
            routes = { self.space: tag, self.backbone: tag }
            return TableTerm(tag, self.space, routes)

        # Term corresponding to the space base.
        term = self.state.assemble(self.space.base)

        # The second special case, when the term of the base could also
        # serve as a term for the space itself.  It is possible if the
        # following two conditions are met:
        # - the term exports the inflation of the given space (`backbone`),
        # - the given space conforms (has the same cardinality as) its base.
        # This case usually corresponds to an HTSQL expression of the form:
        #   (A?f(B)).B,
        # where `B` is a singular, non-nullable link from `A` and `f(B)` is
        # an expression on `B`.
        if self.backbone in term.routes and self.space.conforms(term.space):
            # We need to add the given space to the routing table and
            # replace the term space.
            routes = term.routes.copy()
            routes[self.space] = routes[self.backbone]
            return term.clone(space=self.space, routes=routes)

        # Now the general case.  We take two terms:
        # - the term assembled for the space base
        # - and a table term corresponding to the prominent table,
        # and join them using the tie between the space and its base.

        # This is the term for the space base, we already generated it.
        lkid = term
        # This is a table term corresponding to the prominent table of
        # the space.  Instead of generating it directly, we call `assemble`
        # on the same space, but with a different baseline, so that it
        # will hit the first special case and produce a table term.
        rkid = self.state.assemble(self.space, baseline=self.backbone)
        # The tie tie_termsing the space to its base.
        tie = SeriesTie(self.backbone)
        is_inner = True
        # We use the routing table of the base term with extra routes
        # corresponding to the given space and its inflation which we
        # export from the table term.
        routes = lkid.routes.copy()
        routes[self.space] = rkid.routes[self.space]
        routes[self.backbone] = rkid.routes[self.backbone]
        # Generate a join term node.
        return JoinTerm(self.state.tag(), lkid, rkid, [tie], is_inner,
                        self.space, routes)


class AssembleFiltered(AssembleSpace):
    """
    Assembles a term corresponding to a filtered space.
    """

    adapts(FilteredSpace)

    def __call__(self):
        # To construct a term for a filtered space, we start with
        # a term for its base, ensure that it could generate the given
        # predicate expression and finally wrap it with a filter term
        # node.

        # The term corresponding to the space base.
        term = self.state.assemble(self.space.base)

        # Handle the special case when the filter is already enforced
        # by the mask.  There is no method to directly verify it, so
        # we prune the masked operations from the space itself and
        # its base.  When the filter belongs to the mask, the resulting
        # spaces will be equal.
        if self.space.prune(self.mask) == self.space.base.prune(self.mask):
            # We do not need to apply the filter since it is already
            # enforced by the mask.  We still need to add the space
            # to the routing table.
            routes = term.routes.copy()
            # The space itself and its base share the same inflated space
            # (`backbone`), therefore the backbone must be in the routing
            # table.
            routes[self.space] = routes[self.backbone]
            return term.clone(space=self.space, routes=routes)

        # Now wrap the base term with a filter term node.
        # Make sure the base term is able to produce the filter expression.
        kid = self.state.inject(term, [self.space.filter])
        # Inherit the routing table from the base term, add the given
        # space to the routing table.
        routes = kid.routes.copy()
        routes[self.space] = routes[self.backbone]
        # Generate a filter term node.
        return FilterTerm(self.state.tag(), kid, self.space.filter,
                          self.space, routes)


class AssembleOrdered(AssembleSpace):
    """
    Assembles a term corresponding to an ordered space.
    """

    adapts(OrderedSpace)

    def __call__(self):
        # An ordered space has two functions:
        # - adding explicit row ordering;
        # - extracting a slice from the row set.
        # Note the first function could be ignored since the assembled terms
        # are not required to respect the ordering of the underlying space.

        # There are two cases when we could reuse the base term without
        # wrapping it with an order term node:
        # - when the order space does not apply limit/offset to its base;
        # - when the order space is already enforced by the mask.
        if (self.space.is_expanding or
            self.space.prune(self.mask) == self.space.base.prune(self.mask)):
            # Generate a term for the space base.
            term = self.state.assemble(self.space.base)
            # Update its routing table to include the given space and
            # return the node.
            routes = term.routes.copy()
            routes[self.space] = routes[self.backbone]
            return term.clone(space=self.space, routes=routes)

        # Applying limit/offset requires special care.  Since slicing
        # relies on precise row numbering, the base term must produce
        # exactly the rows of the base.  Therefore we cannot apply any
        # optimizations as they change cardinality of the term.
        # Here we reset the current baseline and mask spaces to the
        # scalar space, which effectively disables any optimizations.
        kid = self.state.assemble(self.space.base,
                                  baseline=self.state.scalar,
                                  mask=self.state.scalar)
        # Extract the space ordering and make sure the base term is able
        # to produce the order expressions.
        order = self.space.ordering()
        codes = [code for code, direction in order]
        kid = self.state.inject(kid, codes)
        # Add the given space to the routing table.
        routes = kid.routes.copy()
        routes[self.space] = routes[self.backbone]
        # Generate an order term.
        return OrderTerm(self.state.tag(), kid, order,
                         self.space.limit, self.space.offset,
                         self.space, routes)


class InjectCode(Inject):

    adapts(Code)

    def __call__(self):
        return self.state.inject(self.term, self.expression.units)


class InjectUnit(Inject):

    adapts(Unit)

    def __init__(self, unit, term, state):
        assert isinstance(unit, Unit)
        super(InjectUnit, self).__init__(unit, term, state)
        self.unit = unit
        self.space = unit.space

    def __call__(self):
        raise NotImplementedError("the inject adapter is not implemented"
                                  " for a %r node" % self.unit)


class InjectColumn(Inject):

    adapts(ColumnUnit)

    def __call__(self):
        if not self.unit.singular(self.term.space):
            raise AssembleError("expected a singular expression",
                                self.unit.mark)
        return self.state.inject(self.term, [self.unit.space])


class InjectScalar(Inject):

    adapts(ScalarUnit)

    def __call__(self):
        if self.unit in self.term.routes:
            return self.term
        group = ScalarBatchExpression(self.unit.space, [self.unit],
                                      self.unit.binding)
        return self.state.inject(self.term, [group])


class InjectAggregate(Inject):

    adapts(AggregateUnit)

    def __call__(self):
        if self.unit in self.term.routes:
            return self.term
        group = AggregateBatchExpression(self.unit.plural_space,
                                         self.unit.space, [self.unit],
                                         self.unit.binding)
        return self.state.inject(self.term, [group])


class InjectCorrelated(Inject):

    adapts(CorrelatedUnit)

    def __call__(self):
        if not self.unit.singular(self.term.space):
            raise AssembleError("expected a singular expression",
                                self.unit.mark)
        if self.unit in self.term.routes:
            return self.term
        is_native = self.space.dominates(self.term.space)
        if is_native:
            trunk_term = self.term
        else:
            trunk_term = self.assemble_shoot(self.space, self.term)
        plural_term = self.assemble_shoot(self.unit.plural_space,
                                          trunk_term, [self.unit.code])
        ties = self.tie_terms(trunk_term, plural_term)
        trunk_term = self.inject_ties(trunk_term, ties)
        lkid = trunk_term
        rkid = plural_term
        routes = lkid.routes.copy()
        routes[self.unit] = plural_term.tag
        unit_term = CorrelationTerm(self.state.tag(), lkid, rkid, ties,
                                    lkid.space, routes)
        if is_native:
            return unit_term
        extra_routes = { self.unit: plural_term.tag }
        return self.join_terms(self.term, unit_term, extra_routes)


class InjectScalarBatch(Inject):

    adapts(ScalarBatchExpression)

    def __init__(self, expression, term, state):
        super(InjectScalarBatch, self).__init__(expression, term, state)
        self.space = expression.space

    def __call__(self):
        units = [unit for unit in self.collection
                      if unit not in self.term.routes]
        if not units:
            return self.term
        if not self.term.space.spans(self.space):
            raise AssembleError("expected a singular expression",
                                units[0].mark)
        codes = [unit.code for unit in units]
        if self.space.dominates(self.term.space):
            term = self.state.inject(self.term, codes)
            if term.is_nullary:
                term = WrapperTerm(self.state.tag(), term,
                                   term.space, term.routes)
            routes = term.routes.copy()
            for unit in units:
                routes[unit] = term.tag
            return term.clone(routes=routes)
        unit_term = self.assemble_shoot(self.space, self.term, codes)
        extra_routes = dict((unit, unit_term.tag) for unit in units)
        return self.join_terms(self.term, unit_term, extra_routes)


class InjectAggregateBatch(Inject):

    adapts(AggregateBatchExpression)

    def __init__(self, expression, term, state):
        super(InjectAggregateBatch, self).__init__(expression, term, state)
        self.plural_space = expression.plural_space
        self.space = expression.space

    def __call__(self):
        units = [unit for unit in self.collection
                      if unit not in self.term.routes]
        if not units:
            return self.term
        if not self.term.space.spans(self.space):
            raise AssembleError("expected a singular expression",
                                units[0].mark)
        codes = [unit.code for unit in units]
        is_native = self.space.dominates(self.term.space)
        if is_native:
            trunk_term = self.term
        else:
            trunk_term = self.assemble_shoot(self.space, self.term)
        plural_term = self.assemble_shoot(self.plural_space,
                                          trunk_term, codes)
        ties = self.tie_terms(trunk_term, plural_term)
        trunk_term = self.inject_ties(trunk_term, ties)
        projected_space = MaskedSpace(ties[-1].space, trunk_term.space,
                                      self.expression.binding)
        routes = {}
        for tie in ties:
            routes[tie.space] = plural_term.routes[tie.space]
        routes[projected_space] = routes[projected_space.base]
        projected_term = ProjectionTerm(self.state.tag(), plural_term,
                                        ties, projected_space, routes)
        extra_routes = dict((unit, projected_term.tag) for unit in units)
        unit_term = self.join_terms(trunk_term, projected_term, extra_routes)
        if is_native:
            return unit_term
        return self.join_terms(self.term, unit_term, extra_routes)


class InjectBatch(Inject):

    adapts(BatchExpression)

    def __init__(self, expression, term, state):
        super(InjectBatch, self).__init__(expression, term, state)
        self.collection = expression.collection

    def __call__(self):
        term = self.term

        units = []
        for expression in self.collection:
            if isinstance(expression, Code):
                for unit in expression.units:
                    if unit not in term.routes:
                        units.append(unit)

        scalar_spaces = []
        scalar_space_to_units = {}
        for unit in units:
            if isinstance(unit, ScalarUnit):
                space = unit.space
                if space not in scalar_space_to_units:
                    scalar_spaces.append(space)
                    scalar_space_to_units[space] = []
                scalar_space_to_units[space].append(unit)
        for space in scalar_spaces:
            group_units = scalar_space_to_units[space]
            group = ScalarBatchExpression(space, group_units,
                                          self.term.binding)
            term = self.state.inject(term, [group])

        aggregate_space_pairs = []
        aggregate_space_pair_to_units = {}
        for unit in units:
            if isinstance(unit, AggregateUnit):
                pair = (unit.plural_space, unit.space)
                if pair not in aggregate_space_pair_to_units:
                    aggregate_space_pairs.append(pair)
                    aggregate_space_pair_to_units[pair] = []
                aggregate_space_pair_to_units[pair].append(unit)
        for pair in aggregate_space_pairs:
            plural_space, space = pair
            group_units = aggregate_space_pair_to_units[pair]
            group = AggregateBatchExpression(plural_space, space, group_units,
                                             self.term.binding)
            term = self.state.inject(term, [group])

        for expression in self.collection:
            term = self.state.inject(term, [expression])
        return term


def assemble(expression, state=None, baseline=None, mask=None):
    """
    Assembles a new term node for the given expression.

    Returns a :class:`htsql.tr.term.Term` instance.

    `expression` (:class:`htsql.tr.code.Expression`)
        An expression node.

    `state` (:class:`AssemblingState` or ``None``)
        The assembling state to use.  If not set, a new assembling state
        is instantiated.

    `baseline` (:class:`htsql.tr.code.Space` or ``None``)
        The baseline space.  Specifies an axis that the assembled
        term must export.  If not set, the current baseline space of
        the state is used.

    `mask` (:class:`htsql.tr.code.Space` or ``None``)
        The mask space.  Specifies the mask space against which
        a new term is assembled.  When not set, the current mask space
        of the state is used.
    """
    # Instantiate a new assembling state if not given one.
    if state is None:
        state = AssemblingState()
    # If passed, assign new baseline and mask spaces.
    if baseline is not None:
        state.push_baseline(baseline)
    if mask is not None:
        state.push_mask(mask)
    # Realize and apply the `Assemble` adapter.
    assemble = Assemble(expression, state)
    term = assemble()
    # Restore old baseline and mask spaces.
    if baseline is not None:
        state.pop_baseline()
    if mask is not None:
        state.pop_mask()
    # Return the assembled term.
    return term


