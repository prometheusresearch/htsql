#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql.tr.compile`
=======================

This module implements the compiling process.
"""


from ..util import maybe, listof
from ..adapter import Adapter, adapts
from ..domain import BooleanDomain
from .error import CompileError
from .syntax import IdentifierSyntax
from .coerce import coerce
from .signature import IsNullSig, AndSig
from .flow import (Expression, Code, Flow, RootFlow, ScalarFlow, TableFlow,
                   DirectTableFlow, FiberTableFlow,
                   QuotientFlow, ComplementFlow, MonikerFlow, ForkedFlow,
                   LinkedFlow, FilteredFlow, OrderedFlow,
                   Unit, ScalarUnit, ColumnUnit, AggregateUnit, CorrelatedUnit,
                   KernelUnit, ComplementUnit, MonikerUnit, ForkedUnit,
                   LinkedUnit,
                   QueryExpr, SegmentExpr, BatchExpr, ScalarBatchExpr,
                   AggregateBatchExpr, FormulaCode)
from .term import (Term, ScalarTerm, TableTerm, FilterTerm, JoinTerm,
                   EmbeddingTerm, CorrelationTerm, ProjectionTerm, OrderTerm,
                   WrapperTerm, SegmentTerm, QueryTerm, Joint)


class CompilingState(object):
    """
    Encapsulates the state of the compiling process.

    State attributes:

    `root` (:class:`htsql.tr.flow.RootFlow`)
        The root flow.

    `baseline` (:class:`htsql.tr.flow.Flow`)
        When compiling a new term, indicates the leftmost axis that must
        exported by the term.  Note that the baseline flow is always
        inflated.

    `mask` (:class:`htsql.tr.flow.Flow`)
        When compiling a new term, indicates that the term is going to be
        attached to a term that represents the `mask` flow.
    """

    def __init__(self):
        # The next term tag to be produced by `tag`.
        self.next_tag = 1
        # The root scalar flow.
        self.root = None
        # The stack of previous baseline flows.
        self.baseline_stack = []
        # The current baseline flow.
        self.baseline = None
        ## The stack of previous mask flows.
        #self.mask_stack = []
        ## The current mask flow.
        #self.mask = None

    def tag(self):
        """
        Generates and returns a new unique term tag.
        """
        tag = self.next_tag
        self.next_tag += 1
        return tag

    def set_root(self, flow):
        """
        Initializes the root, baseline and mask flows.

        This function must be called before state attributes `root`,
        `baseline` and `mask` could be used.

        `flow` (:class:`htsql.tr.flow.RootFlow`)
            A root scalar flow.
        """
        assert isinstance(flow, RootFlow)
        # Check that the state flows are not yet initialized.
        assert self.root is None
        assert self.baseline is None
        #assert self.mask is None
        self.root = flow
        self.baseline = flow
        #self.mask = flow

    def flush(self):
        """
        Clears the state flows.
        """
        # Check that the state flows are initialized and the flow stacks
        # are exhausted.
        assert self.root is not None
        assert not self.baseline_stack
        assert self.baseline is self.root
        #assert not self.mask_stack
        #assert self.mask is self.root
        self.root = None
        self.baseline = None
        #self.mask = None

    def push_baseline(self, baseline):
        """
        Sets a new baseline flow.

        This function masks the current baseline flow.  To restore
        the previous baseline flow, use :meth:`pop_baseline`.

        `baseline` (:class:`htsql.tr.flow.Flow`)
            The new baseline flow.  Note that the baseline flow
            must be inflated.
        """
        assert isinstance(baseline, Flow) and baseline.is_inflated
        self.baseline_stack.append(self.baseline)
        self.baseline = baseline

    def pop_baseline(self):
        """
        Restores the previous baseline flow.
        """
        self.baseline = self.baseline_stack.pop()

    def push_mask(self, mask):
        """
        Sets a new mask flow.

        This function hides the current mask flow.  To restore the
        previous mask flow, use :meth:`pop_mask`.

        `mask` (:class:`htsql.tr.flow.Flow`)
            The new mask flow.
        """
        #assert isinstance(mask, Flow)
        #self.mask_stack.append(self.mask)
        #self.mask = mask

    def pop_mask(self):
        """
        Restores the previous mask flow.
        """
        #self.mask = self.mask_stack.pop()

    def compile(self, expression, baseline=None, mask=None):
        """
        Compiles a new term node for the given expression.

        `expression` (:class:`htsql.tr.flow.Expression`)
            An expression node.

        `baseline` (:class:`htsql.tr.flow.Flow` or ``None``)
            The baseline flow.  Specifies an axis flow that the compiled
            term must export.  If not set, the current baseline flow of
            the state is used.

            When `expression` is a flow, the generated term must
            export the flow itself as well as all inflated prefixes
            up to the `baseline` flow.  It may (but it is not required)
            export other axes as well.

        `mask` (:class:`htsql.tr.flow.Flow` or ``None``)
            The mask flow.  Specifies the mask flow against which
            a new term is compiled.  When not set, the current mask flow
            of the state is used.

            A mask indicates that the new term is going to be attached
            to a term that represent the mask flow.  Therefore the
            compiler could ignore any non-axis operations that are
            already enforced by the mask flow.
        """
        # FIXME: potentially, we could implement a cache of `expression`
        # -> `term` to avoid generating the same term node more than once.
        # There are several complications though.  First, the term depends
        # not only on the expression, but also on the current baseline
        # and mask flows.  Second, each compiled term must have a unique
        # tag, therefore we'd have to replace the tags and route tables
        # of the cached term node.
        return compile(expression, self, baseline=baseline, mask=mask)

    def inject(self, term, expressions):
        """
        Augments a term to make it capable of producing the given expressions.

        This method takes a term node and a list of expressions.  It returns
        a term that could produce the same expressions as the given term, and,
        in addition, all the given expressions.

        Note that, technically, a term only exports unit expressions;
        we claim that a term could export an expression if it exports
        all the units of the expression.

        `term` (:class:`htsql.tr.term.Term`)
            A term node.

        `expression` (a list of :class:`htsql.tr.flow.Expression`)
            A list of expressions to inject into the given term.
        """
        assert isinstance(term, Term)
        assert isinstance(expressions, listof(Expression))
        # Screen out expressions that the term could already export.
        # This filter only works with flows and non-column units,
        # therefore the filtered list could still contain some
        # exportable expressions.
        expressions = [expression for expression in expressions
                                  if expression not in term.routes]
        # No expressions to inject, return the term unmodified.
        if not expressions:
            return term
        # At this moment, we could just apply the `Inject` adapter
        # sequentially to each of the expressions.  However, in some
        # cases, the compiler is able to generate a more optimal
        # term tree when it processes all units sharing the same
        # form simultaneously.  To handle this case, we collect
        # all expressions into an auxiliary expression node
        # `BatchExpr`.  When injected, the group expression
        # applies the multi-unit optimizations.
        if len(expressions) == 1:
            expression = expressions[0]
        else:
            expression = BatchExpr(expressions, term.binding)
        # Realize and apply the `Inject` adapter.
        inject = Inject(expression, term, self)
        return inject()


class CompileBase(Adapter):

    adapts(Expression)

    # Utility functions used by implementations.

    def compile_shoot(self, flow, trunk_term, codes=None):
        """
        Compiles a term corresponding to the given flow.

        The compiled term is called *a shoot term* (relatively to
        the given *trunk term*).

        `flow` (:class:`htsql.tr.flow.Flow`)
            A flow node, for which the we compile a term.

        `trunk_term` (:class:`htsql.tr.term.Term`)
           Expresses a promise that the compiled term will be
           (eventually) joined to `trunk_term` (see :meth:`join_terms`).

        `codes` (a list of :class:`htsql.tr.flow.Expression` or ``None``)
           If provided, a list of expressions to be injected
           into the compiled term.
        """

        # Sanity check on the arguments.
        assert isinstance(flow, Flow)
        assert isinstance(trunk_term, Term)
        assert isinstance(codes, maybe(listof(Expression)))

        # Determine the longest prefix of the flow that either
        # contains no non-axis operations or has all its non-axis
        # operations enforced by the trunk flow.  This prefix will
        # be used as the baseline of the compiled term (that is,
        # we ask the compiler not to generate any axes under
        # the baseline).

        ## Start with removing any filters enforced by the trunk flow.
        #baseline = flow.prune(trunk_term.flow)
        baseline = flow
        assert baseline == flow.prune(trunk_term.flow)

        # Now find the longest prefix that does not contain any
        # non-axis operations.
        while not baseline.is_inflated:
            baseline = baseline.base
        # Handle the case when the given flow is not spanned by the
        # trunk flow -- it happens when we construct a plural term
        # for an aggregate unit.  In this case, before joining it
        # to the trunk term, the shoot term will be projected to some
        # singular prefix of the given flow.  To enable such projection,
        # at least the base of the shoot baseline must be spanned by
        # the trunk flow (then, we can project on the columns of
        # a foreign key that attaches the baseline to its base).
        if not trunk_term.flow.spans(baseline):
            while not trunk_term.flow.spans(baseline.base):
                baseline = baseline.base

        # Compile the term, use the found baseline and the trunk flow
        # as the mask.
        term = self.state.compile(flow,
                                  baseline=baseline)

        # If provided, inject the given expressions.
        if codes is not None:
            term = self.state.inject(term, codes)

        # Return the compiled shoot term.
        return term

    def tie_terms(self, trunk_term, shoot_term):
        """
        Returns ties to attach the shoot term to the trunk term.

        `trunk_term` (:class:`htsql.tr.term.Term`)
            The left (trunk) operand of the join.

        `shoot_term` (:class:`htsql.tr.term.Term`)
            The right (shoot) operand of the join.

        Note that the trunk term may not export all the units necessary
        to generate tie conditions.  Apply :meth:`inject_ties` on the trunk
        before using the ties to join the trunk and the shoot.
        """
        # Sanity check on the arguments.
        assert isinstance(trunk_term, Term)
        assert isinstance(shoot_term, Term)
        # Verify that it is possible to join the terms without
        # changing the cardinality of the trunk.
        assert (shoot_term.baseline.is_root or
                trunk_term.flow.spans(shoot_term.baseline.base))

        # There are two ways the ties are generated:
        #
        # - when the shoot baseline is an axis of the trunk flow,
        #   in this case we join the terms using parallel ties on
        #   the common axes;
        # - otherwise, join the terms using a serial tie between
        #   the shoot baseline and its base.

        # Ties to attach the shoot to the trunk.
        joints = []
        # Check if the shoot baseline is an axis of the trunk flow.
        if trunk_term.backbone.concludes(shoot_term.baseline):
            # In this case, we join the terms by all axes of the trunk
            # flow that are exported by the shoot term.
            # Find the first inflated axis of the trunk exported
            # by the shoot.
            axis = trunk_term.backbone
            while not shoot_term.backbone.concludes(axis):
                axis = axis.base
            # Now the axes between `axis` and `baseline` are common axes
            # of the trunk flow and the shoot term.  For each of them,
            # generate a parallel tie.  Note that we do not verify
            # (and, in general, it is not required) that these axes
            # are exported by the trunk term.  Apply `inject_ties()` on
            # the trunk term before using the ties to join the terms.
            axes = []
            while axis != shoot_term.baseline.base:
                # Skip non-expanding axes (but always include the baseline).
                if not axis.is_contracting or axis == shoot_term.baseline:
                    axes.append(axis)
                axis = axis.base
            # We prefer (for no particular reason) the ties to go
            # from inner to outer axes.
            axes.reverse()
            for axis in axes:
                joints.extend(sew(axis))
        else:
            # When the shoot does not touch the trunk flow, we attach it
            # using a serial tie between the shoot baseline and its base.
            # Note that we do not verify (and it is not required) that
            # the trunk term export the base flow.  Apply `inject_ties()`
            # on the trunk term to inject any necessary flows before
            # joining the terms using the ties.
            joints = tie(shoot_term.baseline)

        # Return the generated ties.
        return joints

    def inject_ties(self, term, joints):
        """
        Augments the term to ensure it can export all units required
        to generate tie conditions.

        `term` (:class:`htsql.tr.term.Term`)
            The term to update.

            It is assumed that `term` was the argument `trunk_term` of
            :meth:`tie_terms` when the ties were generated.

        `ties` (a list of :class:`Tie`)
            The ties to inject.

            It is assumed the ties were generated by :meth:`tie_terms`.
        """
        # Sanity check on the arguments.
        assert isinstance(term, Term)

        units = [lunit for lunit, runit in joints]
        return self.state.inject(term, units)

    def join_terms(self, trunk_term, shoot_term, extra_routes):
        """
        Attaches a shoot term to a trunk term.

        The produced join term uses the flow and the routing
        table of the trunk term, but also includes the given
        extra routes.

        `trunk_term` (:class:`htsql.tr.term.Term`)
            The left (trunk) operand of the join.

        `shoot_term` (:class:`htsql.tr.term.Term`)
            The right (shoot) operand of the term.

            The shoot term must be singular relatively to the trunk term.

        `extra_routes` (a mapping from a unit/flow to a term tag)
            Any extra routes provided by the join.
        """
        # Sanity check on the arguments.
        assert isinstance(trunk_term, Term)
        assert isinstance(shoot_term, Term)
        # FIXME: Unfortunately, we cannot properly verify that the trunk
        # flow spans the shoot flow since the term flow is generated
        # incorrectly for projection terms.
        #assert trunk_term.flow.dominates(shoot_term.flow)
        assert isinstance(extra_routes, dict)

        # Ties that combine the terms.
        joints = self.tie_terms(trunk_term, shoot_term)
        # Make sure the trunk term could export tie conditions.
        trunk_term = self.inject_ties(trunk_term, joints)
        # Determine if we could use an inner join to attach the shoot
        # to the trunk.  We could do it if the inner join does not
        # decrease cardinality of the trunk.
        # FIXME: The condition that the shoot flow dominates the
        # trunk flow is sufficient, but not really necessary.
        # In general, we can use the inner join if the shoot flow
        # dominates the prefix of the trunk flow cut at the longest
        # common axis of trunk and the shoot flows.
        is_left = (not shoot_term.flow.dominates(trunk_term.flow))
        is_right = False
        # Use the routing table of the trunk term, but also add
        # the given extra routes.
        routes = trunk_term.routes.copy()
        routes.update(extra_routes)
        # Generate and return a join term.
        return JoinTerm(self.state.tag(), trunk_term, shoot_term,
                        joints, is_left, is_right,
                        trunk_term.flow, trunk_term.baseline, routes)


class Compile(CompileBase):
    """
    Translates an expression node to a term node.

    This is an interface adapter; see subclasses for implementations.

    The :class:`Compile` adapter is implemented for two classes
    of expressions:

    - top-level expressions such as the whole query and the query segment,
      for which it builds respective top-level term nodes;

    - flows, for which the adapter builds a corresponding relational
      algebraic expression.

    After a term is built, it is typically augmented using the
    :class:`Inject` adapter to have it export any exprected units.

    The :class:`Compile` adapter has the following signature::

        Compile: (Expression, CompilingState) -> Term

    The adapter is polymorphic on the `Expression` argument.

    `expression` (:class:`htsql.tr.flow.Expression`)
        An expression node.

    `state` (:class:`CompilingState`)
        The current state of the compiling process.
    """

    def __init__(self, expression, state):
        assert isinstance(expression, Expression)
        assert isinstance(state, CompilingState)
        self.expression = expression
        self.state = state

    def __call__(self):
        # This should never be reachable; if we are here, it indicates
        # either a bug or an incomplete implementation.  Since normally it
        # cannot be triggered by a user, we don't bother with generating
        # a user-level HTSQL exception.
        raise NotImplementedError("the compile adapter is not implemented"
                                  " for a %r node" % self.expression)


class Inject(CompileBase):
    """
    Augments a term to make it capable of producing the given expression.

    This is an interface adapter; see subclasses for implementations.

    This adapter takes a term node and an expression (usually, a unit)
    and returns a new term (an augmentation of the given term) that is
    able to produce the given expression.

    The :class:`Inject` adapter has the following signature::

        Inject: (Expression, Term, CompilingState) -> Term

    The adapter is polymorphic on the `Expression` argument.

    `expression` (:class:`htsql.tr.flow.Expression`)
        An expression node to inject.

    `term` (:class:`htsql.tr.term.Term`)
        A term node to inject into.

    `state` (:class:`CompilingState`)
        The current state of the compiling process.
    """

    def __init__(self, expression, term, state):
        assert isinstance(expression, Expression)
        assert isinstance(term, Term)
        assert isinstance(state, CompilingState)
        self.expression = expression
        self.term = term
        self.state = state

    def __call__(self):
        # Same as with `Compile`, unless it's a bug or an incomplete
        # implementation, it should never be reachable.
        raise NotImplementedError("the inject adapter is not implemented"
                                  " for a %r node" % self.expression)


class CompileQuery(Compile):
    """
    Compiles a top-level query term.
    """

    adapts(QueryExpr)

    def __call__(self):
        # Compile the segment term.
        segment = None
        if self.expression.segment is not None:
            segment = self.state.compile(self.expression.segment)
        # Construct a query term.
        return QueryTerm(segment, self.expression)


class CompileSegment(Compile):
    """
    Compiles a segment term.
    """

    adapts(SegmentExpr)

    def __call__(self):
        # Initialize the all state flows with a root scalar flow.
        self.state.set_root(self.expression.flow.root)
        # Construct a term corresponding to the segment flow.
        kid = self.state.compile(self.expression.flow)
        # Get the ordering of the segment flow.
        order = ordering(self.expression.flow)
        # List of expressions we need the term to export.
        codes = self.expression.elements + [code for code, direction in order]
        # Inject the expressions into the term.
        kid = self.state.inject(kid, codes)
        # The compiler does not guarantee that the produced term respects
        # the flow ordering, so it is our responsitibity to wrap the term
        # with an order node.
        if order:
            kid = OrderTerm(self.state.tag(), kid, order, None, None,
                            kid.flow, kid.baseline, kid.routes.copy())
        # Shut down the state flows.
        self.state.flush()
        # Construct a segment term.
        return SegmentTerm(self.state.tag(), kid, self.expression.elements,
                           kid.flow, kid.routes.copy())


class CompileFlow(Compile):
    """
    Compile a term corresponding to a flow node.

    This is an abstract class; see subclasses for implementations.

    The general algorithm for compiling a term node for the given flow
    looks as follows:

    - compile a term for the base flow;
    - inject any necessary expressions;
    - build a new term node that represents the flow operation.

    When compiling terms, the following optimizations are applied:

    Removing unnecessary non-axis operations.  The current `mask` flow
    expresses a promise that the generated term will be attached to
    a term representing the mask flow.  Therefore the compiler
    could skip any non-axis filters that are already enforced by
    the mask flow.

    Removing unnecessary axis operations.  The current `baseline` flow
    denotes the leftmost axis that the term should be able to export.
    The compiler may (but does not have to) omit any axes nested under
    the `baseline` axis.

    Because of these optimizations, the shape and cardinality of the
    term rows may differ from that of the flow.  Additionally, the
    term is not required to respect the ordering of its flow.

    Constructor arguments:

    `flow` (:class:`htsql.tr.flow.Flow`)
        A flow node.

    `state` (:class:`CompilingState`)
        The current state of the compiling process.

    Other attributes:

    `backbone` (:class:`htsql.tr.flow.Flow`)
        The inflation of the given flow.

    `baseline` (:class:`htsql.tr.flow.Flow`)
        An alias to `state.baseline`.

    `mask` (:class:`htsql.tr.flow.Flow`)
        An alias to `state.mask`.
    """

    adapts(Flow)

    def __init__(self, flow, state):
        assert isinstance(flow, Flow)
        # The inflation of the flow.
        backbone = flow.inflate()
        # Check that the baseline flow is an axis of the given flow.
        assert flow.concludes(state.baseline)
        super(CompileFlow, self).__init__(flow, state)
        self.flow = flow
        self.state = state
        self.backbone = backbone
        # Extract commonly used state properties.
        self.baseline = state.baseline
        #self.mask = state.mask


class InjectFlow(Inject):
    """
    Augments the term to make it produce the given flow.

    `flow` (:class:`htsql.tr.flow.Flow`)
        A flow node to inject.

    `term` (:class:`htsql.tr.term.Term`)
        A term node to inject into.

        The term flow must span the given flow.

    `state` (:class:`CompilingState`)
        The current state of the compiling process.
    """

    adapts(Flow)

    def __init__(self, flow, term, state):
        assert isinstance(flow, Flow)
        # It is a bug if we get the `flow` plural for the `term` here.
        # It is a responsibility of `InjectUnit` to guard against unexpected
        # plural expressions and to issue an appropriate HTSQL error.
        assert term.flow.spans(flow)
        super(InjectFlow, self).__init__(flow, term, state)
        self.flow = flow
        self.term = term
        self.state = state

    def __call__(self):
        # Note that this function works for all flow classes universally.
        # We start with checking for and handling several special cases;
        # if none of them apply, we grow a shoot term for the given flow
        # and attach it to the main term.

        # Check if the flow is already exported.
        if all(unit.clone(flow=self.flow) in self.term.routes
               for unit in spread(self.flow)):
            return self.term

        ## Remove any non-axis filters that are enforced by the term flow.
        #unmasked_flow = self.flow.prune(self.term.flow)
        assert self.flow == self.flow.prune(self.term.flow)

        ## When converged with the term flow, `flow` and `unmasked_flow`
        ## contains the same set of rows, therefore in the context of the
        ## given term, they could be used interchangeably.
        ## In particular, if `unmasked_flow` is already exported, we could
        ## use the same route for `flow`.
        #if unmasked_flow in self.term.routes:
        #    routes = self.term.routes.copy()
        #    routes[self.flow] = routes[unmasked_flow]
        #    return WrapperTerm(self.state.tag(), self.term, self.term.flow,
        #                       routes)

        # A special case when the given flow is an axis prefix of the term
        # flow.  The fact that the flow is not exported by the term means
        # that the term tree is optimized by cutting all axes below some
        # baseline.  Now we need to grow these axes back.
        if self.term.flow.concludes(self.flow):
            assert self.term.baseline.base.concludes(self.flow)
            # Here we compile a table term corresponding to the flow and
            # attach it to the axis directly above it using a serial tie.

            # Compile a term corresponding to the axis itself.
            lkid = self.state.compile(self.term.baseline.base,
                                       baseline=self.flow)
            ## We expect to get a table or a scalar term here.
            ## FIXME: No longer valid since the axis could be a quotient flow.
            #assert lkid.is_nullary

            ## Find the axis directly above the flow.  Note that here
            ## `unmasked_flow` is the inflation of the given flow.
            #next_axis = self.term.baseline
            #while next_axis.base != unmasked_flow:
            #    next_axis = next_axis.base

            ## It is possible that `next_axis` is also not exported by
            ## the term (specifically, when `next_axis` is below the term
            ## baseline).  So we call `inject()` with `next_axis`, which
            ## should match the same special case and recursively add
            ## `next_axis` to the routing table.  Bugs in the compiler
            ## and in the compare-by-value code often cause an infinite
            ## loop or recursion here!
            #rkid = self.state.inject(self.term, [next_axis])
            rkid = self.term
            ## Injecting an axis prefix should never add any axes below
            ## (but will add all the axis prefixes above).
            #assert unmasked_flow not in rkid.routes

            # Join the terms using a serial tie.
            joints = tie(self.term.baseline)
            lkid = self.inject_ties(lkid, joints)
            # Since we are expanding the term baseline, the join is always
            # inner.
            is_left = False
            is_right = False
            # Re-use the old routing table, but add the new axis.
            routes = {}
            routes.update(lkid.routes)
            routes.update(rkid.routes)
            # Compile and return a join term.
            return JoinTerm(self.state.tag(), lkid, rkid, joints,
                            is_left, is_right,
                            rkid.flow, lkid.baseline, routes)

        # None of the special cases apply, so we use a general method:
        # - grow a shoot term for the given flow;
        # - attach the shoot to the main term.

        # Compile a shoot term for the flow.
        flow_term = self.compile_shoot(self.flow, self.term)
        # The routes to add.
        extra_routes = {}
        for unit in spread(self.flow):
            extra_routes[unit.clone(flow=self.flow)] = flow_term.routes[unit]
        # Join the shoot to the main term.
        return self.join_terms(self.term, flow_term, extra_routes)


class CompileRoot(CompileFlow):
    """
    Compiles a term corresponding to a root scalar flow.
    """

    adapts(RootFlow)

    def __call__(self):
        # Generate a `ScalarTerm` instance.
        tag = self.state.tag()
        routes = {}
        return ScalarTerm(tag, self.flow, self.flow, routes)


class CompileScalar(CompileFlow):

    adapts(ScalarFlow)

    def __call__(self):
        if self.flow == self.baseline:
            tag = self.state.tag()
            routes = {}
            return ScalarTerm(tag, self.flow, self.flow, routes)
        term = self.state.compile(self.flow.base)
        return WrapperTerm(self.state.tag(), term,
                           self.flow, term.baseline, term.routes)


class CompileTable(CompileFlow):
    """
    Compiles a term corresponding to a (direct or fiber) table flow.
    """

    adapts(TableFlow)

    def __call__(self):
        # We start with identifying and handling special cases, where
        # we able to generate a more optimal, less compex term tree than
        # in the regular case.  If none of the special cases are applicable,
        # we use the generic algorithm.

        # The first special case: the given flow is the leftmost axis
        # we must export.  Since `baseline` is always an inflated flow,
        # we need to compare it with the inflation of the given flow
        # rather than with the flow itself.
        if self.flow == self.baseline:
            # Generate a table term that exports rows from the prominent
            # table.
            tag = self.state.tag()
            # The routing table must always include the term flow, and also,
            # for any flow it includes, the inflation of the flow.
            # In this case, `self.flow` is the term flow, `self.backbone`
            # is its inflation.
            routes = {}
            for unit in spread(self.flow):
                routes[unit] = tag
            return TableTerm(tag, self.flow, self.baseline, routes)

        # Term corresponding to the flow base.
        term = self.state.compile(self.flow.base)

        # The second special case, when the term of the base could also
        # serve as a term for the flow itself.  It is possible if the
        # following two conditions are met:
        # - the term exports the inflation of the given flow (`backbone`),
        # - the given flow conforms (has the same cardinality as) its base.
        # This case usually corresponds to an HTSQL expression of the form:
        #   (A?f(B)).B,
        # where `B` is a singular, non-nullable link from `A` and `f(B)` is
        # an expression on `B`.
        if (self.flow.conforms(term.flow) and
            all(unit in term.routes for unit in spread(self.flow))):
            # We need to add the given flow to the routing table and
            # replace the term flow.
            routes = term.routes.copy()
            for unit in spread(self.flow):
                routes[unit.clone(flow=self.flow)] = routes[unit]
            return WrapperTerm(self.state.tag(), term,
                               self.flow, term.baseline, routes)

        # Now the general case.  We take two terms:
        # - the term compiled for the flow base
        # - and a table term corresponding to the prominent table,
        # and join them using the tie between the flow and its base.

        # This is the term for the flow base, we already generated it.
        lkid = term
        # This is a table term corresponding to the prominent table of
        # the flow.  Instead of generating it directly, we call `compile`
        # on the same flow, but with a different baseline, so that it
        # will hit the first special case and produce a table term.
        rkid = self.state.compile(self.backbone, baseline=self.backbone)
        # The connections between the flow to its base.
        joints = tie(self.flow)
        is_left = False
        is_right = False
        # We use the routing table of the base term with extra routes
        # corresponding to the given flow and its inflation which we
        # export from the table term.
        routes = lkid.routes.copy()
        routes = {}
        routes.update(lkid.routes)
        routes.update(rkid.routes)
        for unit in spread(self.flow):
            routes[unit.clone(flow=self.flow)] = routes[unit]
        # Generate a join term node.
        return JoinTerm(self.state.tag(), lkid, rkid, joints,
                        is_left, is_right, self.flow, lkid.baseline, routes)


class CompileQuotient(CompileFlow):

    adapts(QuotientFlow)

    def __call__(self):
        baseline = self.flow.seed_baseline
        while not baseline.is_inflated:
            baseline = baseline.base
        seed_term = self.state.compile(self.flow.seed, baseline=baseline)
        if self.flow.kernel:
            seed_term = self.state.inject(seed_term, self.flow.kernel)
            filters = []
            for code in self.flow.kernel:
                filter = FormulaCode(IsNullSig(-1), coerce(BooleanDomain()),
                                     code.binding, op=code)
                filters.append(filter)
            if len(filters) == 1:
                [filter] = filters
            else:
                filter = FormulaCode(AndSig(), coerce(BooleanDomain()),
                                     self.flow.binding, ops=filters)
            seed_term = FilterTerm(self.state.tag(), seed_term, filter,
                                   seed_term.flow, seed_term.baseline,
                                   seed_term.routes.copy())
        if (self.flow == self.baseline and
                seed_term.baseline == self.flow.seed_baseline):
            tag = self.state.tag()
            basis = []
            routes = {}
            joints = tie(seed_term.baseline)
            for lunit, runit in joints:
                basis.append(runit)
                unit = KernelUnit(runit, self.flow, runit.binding)
                routes[unit] = tag
            for code in self.flow.kernel:
                basis.append(code)
                unit = KernelUnit(code, self.flow, code.binding)
                routes[unit] = tag
            term = ProjectionTerm(tag, seed_term, basis,
                                  self.flow, self.flow, routes)
            return term
        baseline = self.baseline
        if baseline == self.flow:
            baseline = baseline.base
        lkid = self.state.compile(self.flow.base, baseline=baseline)
        joints = self.tie_terms(lkid, seed_term)
        lkid = self.inject_ties(lkid, joints)
        tag = self.state.tag()
        basis = []
        routes = {}
        joints_copy = joints
        joints = []
        for joint in joints_copy:
            basis.append(joint.rop)
            rop = KernelUnit(joint.rop, self.backbone, joint.rop.binding)
            routes[rop] = tag
            joints.append(joint.clone(rop=rop))
        quotient_joints = tie(self.flow.seed_baseline)
        if seed_term.baseline != self.flow.seed_baseline:
            for joint in quotient_joints:
                basis.append(joint.rop)
                unit = KernelUnit(joint.rop, self.backbone, joint.rop.binding)
                routes[unit] = tag
        else:
            assert quotient_joints == joints_copy
        for code in self.flow.kernel:
            basis.append(code)
            unit = KernelUnit(code, self.backbone, code.binding)
            routes[unit] = tag
        rkid = ProjectionTerm(tag, seed_term, basis,
                              self.backbone, self.backbone, routes)
        is_left = False
        is_right = False
        routes = {}
        routes.update(lkid.routes)
        routes.update(rkid.routes)
        for unit in spread(self.flow):
            routes[unit.clone(flow=self.flow)] = routes[unit]
        return JoinTerm(self.state.tag(), lkid, rkid, joints,
                        is_left, is_right, self.flow, lkid.baseline, routes)


class CompileComplement(CompileFlow):

    adapts(ComplementFlow)

    def __call__(self):
        family = self.flow.base.family
        baseline = family.seed_baseline
        while not baseline.is_inflated:
            baseline = baseline.base
        seed_term = self.state.compile(family.seed, baseline=baseline)
        seed_term = self.state.inject(seed_term, family.kernel)
        if self.flow.extra_codes is not None:
            seed_term = self.state.inject(seed_term, self.flow.extra_codes)
        if family.kernel:
            filters = []
            for code in family.kernel:
                filter = FormulaCode(IsNullSig(-1), coerce(BooleanDomain()),
                                     code.binding, op=code)
                filters.append(filter)
            if len(filters) == 1:
                [filter] = filters
            else:
                filter = FormulaCode(AndSig(), coerce(BooleanDomain()),
                                     self.flow.binding, ops=filters)
            seed_term = FilterTerm(self.state.tag(), seed_term, filter,
                                   seed_term.flow, seed_term.baseline,
                                   seed_term.routes.copy())
        seed_term = WrapperTerm(self.state.tag(), seed_term,
                                seed_term.flow, seed_term.baseline,
                                seed_term.routes.copy())
        if (self.flow == self.baseline and
                seed_term.baseline == family.seed_baseline):
            routes = {}
            for unit in seed_term.routes:
                unit = ComplementUnit(unit, self.flow, unit.binding)
                routes[unit] = seed_term.tag
            for code in family.kernel:
                unit = ComplementUnit(code, self.flow, unit.binding)
                routes[unit] = seed_term.tag
            if self.flow.extra_codes is not None:
                for code in self.flow.extra_codes:
                    unit = ComplementUnit(code, self.flow, code.binding)
                    routes[unit] = seed_term.tag
            for unit in spread(family.seed):
                routes[unit.clone(flow=self.flow)] = seed_term.routes[unit]
            term = WrapperTerm(self.state.tag(), seed_term,
                               self.flow, self.flow, routes)
            return term
        baseline = self.baseline
        if baseline == self.flow:
            baseline = baseline.base
        lkid = self.state.compile(self.flow.base, baseline=baseline)
        seed_joints = []
        if seed_term.baseline != family.seed_baseline:
            seed_joints = self.tie_terms(lkid, seed_term)
            lkid = self.inject_ties(lkid, seed_joints)
        routes = {}
        for unit in seed_term.routes:
            unit = ComplementUnit(unit, self.backbone, unit.binding)
            routes[unit] = seed_term.tag
        for code in family.kernel:
            unit = ComplementUnit(code, self.backbone, unit.binding)
            routes[unit] = seed_term.tag
        if self.flow.extra_codes is not None:
            for code in self.flow.extra_codes:
                unit = ComplementUnit(code, self.backbone, code.binding)
                routes[unit] = seed_term.tag
        for unit in spread(family.seed):
            routes[unit.clone(flow=self.backbone)] = seed_term.routes[unit]
        seed_joints_copy = seed_joints
        seed_joints = []
        for joint in seed_joints:
            rop = ComplementUnit(joint.rop, self.backbone, joint.rop.binding)
            routes[rop] = seed_term.tag
            seed_joints.append(joint.clone(rop=rop))
        rkid = WrapperTerm(self.state.tag(), seed_term,
                           self.backbone, self.backbone, routes)
        joints = seed_joints + tie(self.flow)
        is_left = False
        is_right = False
        routes = {}
        routes.update(lkid.routes)
        routes.update(rkid.routes)
        for unit in spread(self.flow):
            routes[unit.clone(flow=self.flow)] = routes[unit]
        return JoinTerm(self.state.tag(), lkid, rkid, joints,
                        is_left, is_right, self.flow, lkid.baseline, routes)


class CompileMoniker(CompileFlow):

    adapts(MonikerFlow)

    def __call__(self):
        if (self.flow.seed_baseline.base is not None and
            self.flow.base.conforms(self.flow.seed_baseline.base) and
            not self.flow.base.spans(self.flow.seed_baseline)):
            baseline = self.flow.seed_baseline
            if not (baseline.is_inflated and
                    self.flow == self.state.baseline):
                while not self.state.baseline.concludes(baseline):
                    baseline = baseline.base
            seed_term = self.state.compile(self.flow.seed, baseline=baseline)
            if self.flow.extra_codes is not None:
                seed_term = self.state.inject(seed_term,
                                              self.flow.extra_codes)
            if seed_term.baseline != self.flow.seed_baseline:
                flow = self.flow.base
                seed_term = self.state.inject(seed_term, [flow])
                while not seed_term.baseline.concludes(flow):
                    seed_term = self.state.inject(seed_term, [flow])
                    flow = flow.base
            seed_term = WrapperTerm(self.state.tag(), seed_term,
                                    seed_term.flow, seed_term.baseline,
                                    seed_term.routes.copy())
            baseline = seed_term.baseline
            if baseline == self.flow.seed_baseline:
                baseline = self.flow
            routes = {}
            for unit in seed_term.routes:
                if self.flow.base.spans(unit.flow):
                    routes[unit] = seed_term.routes[unit]
                seed_unit = MonikerUnit(unit, self.flow, unit.binding)
                routes[seed_unit] = seed_term.tag
                seed_unit = MonikerUnit(unit, self.backbone, unit.binding)
                routes[seed_unit] = seed_term.tag
            if self.flow.extra_codes is not None:
                for code in self.flow.extra_codes:
                    unit = MonikerUnit(code, self.flow, code.binding)
                    routes[unit] = seed_term.tag
                    unit = MonikerUnit(code, self.backbone, code.binding)
                    routes[unit] = seed_term.tag
            for unit in spread(self.flow.seed):
                seed_unit = unit.clone(flow=self.flow)
                routes[seed_unit] = seed_term.routes[unit]
                seed_unit = unit.clone(flow=self.backbone)
                routes[seed_unit] = seed_term.routes[unit]
            term = WrapperTerm(self.state.tag(), seed_term,
                               self.flow, baseline, routes)
            return term
        baseline = self.state.baseline
        if baseline == self.flow:
            baseline = baseline.base
        trunk_term = self.state.compile(self.flow.base, baseline=baseline)
        seed_term = self.compile_shoot(self.flow.seed, trunk_term,
                                        self.flow.extra_codes)
        seed_term = WrapperTerm(self.state.tag(), seed_term,
                                seed_term.flow, seed_term.baseline,
                                seed_term.routes)
        joints = self.tie_terms(trunk_term, seed_term)
        trunk_term = self.inject_ties(trunk_term, joints)
        routes = trunk_term.routes.copy()
        for unit in seed_term.routes:
            seed_unit = MonikerUnit(unit, self.flow, unit.binding)
            routes[seed_unit] = seed_term.tag
            seed_unit = MonikerUnit(unit, self.backbone, unit.binding)
            routes[seed_unit] = seed_term.tag
        if self.flow.extra_codes is not None:
            for code in self.flow.extra_codes:
                unit = MonikerUnit(code, self.flow, code.binding)
                routes[unit] = seed_term.tag
                unit = MonikerUnit(code, self.backbone, code.binding)
                routes[unit] = seed_term.tag
        for unit in spread(self.flow.seed):
            seed_unit = unit.clone(flow=self.flow)
            routes[seed_unit] = seed_term.routes[unit]
            seed_unit = unit.clone(flow=self.backbone)
            routes[seed_unit] = seed_term.routes[unit]
        return JoinTerm(self.state.tag(), trunk_term, seed_term,
                        joints, False, False,
                        self.flow, trunk_term.baseline, routes)


class CompileForked(CompileFlow):

    adapts(ForkedFlow)

    def __call__(self):
        seed = self.flow.seed
        baseline = seed
        while not baseline.is_inflated:
            baseline = baseline.base
        seed_term = self.state.compile(seed, baseline=baseline)
        extra_codes = self.flow.kernel[:]
        if self.flow.extra_codes is not None:
            extra_codes.extend(self.flow.extra_codes)
        seed_term = self.state.inject(seed_term, extra_codes)
        seed_term = WrapperTerm(self.state.tag(), seed_term,
                                seed_term.flow, seed_term.baseline,
                                seed_term.routes.copy())
        if (self.state.baseline == self.flow and
                seed_term.baseline == self.flow.seed_baseline):
            routes = {}
            for unit in seed_term.routes:
                seed_unit = ForkedUnit(unit, self.flow, unit.binding)
                routes[seed_unit] = seed_term.tag
                seed_unit = ForkedUnit(unit, self.backbone, unit.binding)
                routes[seed_unit] = seed_term.tag
            extra_codes = self.flow.kernel[:]
            if self.flow.extra_codes is not None:
                extra_codes.extend(self.flow.extra_codes)
            for code in extra_codes:
                unit = ForkedUnit(code, self.flow, code.binding)
                routes[unit] = seed_term.tag
                unit = ForkedUnit(code, self.backbone, code.binding)
                routes[unit] = seed_term.tag
            for unit in spread(self.flow.seed):
                seed_unit = unit.clone(flow=self.flow)
                routes[seed_unit] = seed_term.routes[unit]
                seed_unit = unit.clone(flow=self.backbone)
                routes[seed_unit] = seed_term.routes[unit]
            term = WrapperTerm(self.state.tag(), seed_term,
                               self.flow, self.flow, routes)
            return term
        baseline = self.state.baseline
        if baseline == self.flow:
            baseline = baseline.base
        trunk_term = self.state.compile(self.flow.base, baseline=baseline)
        joints = []
        assert (trunk_term.baseline.concludes(seed_term.baseline) or
                seed_term.baseline.concludes(trunk_term.baseline))
        axes = []
        axis = trunk_term.backbone
        while axis != seed_term.baseline:
            axis = axis.base
            if not axis.is_contracting or axis == seed_term.baseline:
                axes.append(axis)
        axes.reverse()
        if axes:
            for axis in axes:
                joints.extend(sew(axis))
        else:
            for joint in tie(trunk_term.backbone):
                joint = joint.clone(lop=joint.rop)
                joints.append(joint)
        for code in self.flow.kernel:
            joint = Joint(code, code)
            joints.append(joint)
        units = [lunit for lunit, runit in joints]
        trunk_term = self.state.inject(trunk_term, units)
        routes = trunk_term.routes.copy()
        for unit in seed_term.routes:
            seed_unit = ForkedUnit(unit, self.flow, unit.binding)
            routes[seed_unit] = seed_term.tag
            seed_unit = ForkedUnit(unit, self.backbone, unit.binding)
            routes[seed_unit] = seed_term.tag
        extra_codes = self.flow.kernel[:]
        if self.flow.extra_codes is not None:
            extra_codes.extend(self.flow.extra_codes)
        for code in extra_codes:
            unit = ForkedUnit(code, self.flow, code.binding)
            routes[unit] = seed_term.tag
            unit = ForkedUnit(code, self.backbone, code.binding)
            routes[unit] = seed_term.tag
        for unit in spread(self.flow.seed):
            seed_unit = unit.clone(flow=self.flow)
            routes[seed_unit] = seed_term.routes[unit]
            seed_unit = unit.clone(flow=self.backbone)
            routes[seed_unit] = seed_term.routes[unit]
        return JoinTerm(self.state.tag(), trunk_term, seed_term,
                        joints, False, False,
                        self.flow, trunk_term.baseline, routes)


class CompileLinked(CompileFlow):

    adapts(LinkedFlow)

    def __call__(self):
        baseline = self.flow.seed_baseline
        while not baseline.is_inflated:
            baseline = baseline.base
        seed_term = self.state.compile(self.flow.seed, baseline=baseline)
        codes = self.flow.kernel[:]
        if self.flow.extra_codes is not None:
            codes += self.flow.extra_codes
        seed_term = self.state.inject(seed_term, codes)
        extra_axes = []
        joints = []
        if seed_term.baseline != self.flow.seed_baseline:
            backbone = self.flow.base.inflate()
            axis = seed_term.baseline
            while not backbone.concludes(axis):
                axis = axis.base
            seed_term = self.state.inject(seed_term, axis)
            axis = backbone
            while not seed_term.backbone.concludes(axis):
                axis = axis.base
            while axis != seed_term.baseline.base:
                if not axis.is_contracting or axis == seed_term.baseline:
                    extra_axes.append(axis)
                axis = axis.base
            extra_axes.reverse()
            for axis in extra_axes:
                for joint in sew(axis):
                    rop = LinkedUnit(self.flow.inflate(), rop, rop.binding)
                    joint = joint.clone(rop=rop)
                    joints.append(joint)
        joints.extend(tie(self.flow))
        trunk_term = None
        if extra_axes or self.state.baseline != self.flow:
            baseline = self.state.baseline
            if baseline == self.flow:
                baseline = baseline.base
            trunk_term = self.state.compile(self.flow.base, baseline=baseline)
            trunk_term = self.state.inject(trunk_term,
                                           [joint.lop for joint in joints])
        seed_term = WrapperTerm(self.state.tag(), seed_term,
                                seed_term.flow, seed_term.baseline,
                                seed_term.routes.copy())
        baseline = seed_term.baseline
        if baseline == self.flow.seed_baseline:
            baseline = self.flow
        routes = {}
        for unit in seed_term.routes:
            if self.flow.base.spans(unit.flow):
                routes[unit] = seed_term.routes[unit]
            seed_unit = LinkedUnit(unit, self.flow, unit.binding)
            routes[seed_unit] = seed_term.tag
            seed_unit = LinkedUnit(unit, self.backbone, unit.binding)
            routes[seed_unit] = seed_term.tag
        for joint in joints:
            code = joint.rop.code
            unit = LinkedUnit(code, self.flow, code.binding)
            routes[unit] = seed_term.tag
            unit = LinkedUnit(code, self.backbone, code.binding)
            routes[unit] = seed_term.tag
        if self.flow.extra_codes is not None:
            for code in self.flow.extra_codes:
                unit = LinkedUnit(code, self.flow, code.binding)
                routes[unit] = seed_term.tag
                unit = LinkedUnit(code, self.backbone, code.binding)
                routes[unit] = seed_term.tag
        for unit in spread(self.flow.seed):
            seed_unit = unit.clone(flow=self.flow)
            routes[seed_unit] = seed_term.routes[unit]
            seed_unit = unit.clone(flow=self.backbone)
            routes[seed_unit] = seed_term.routes[unit]
        term = WrapperTerm(self.state.tag(), seed_term,
                           self.backbone, self.backbone, routes)
        if trunk_term is not None:
            routes = trunk_term.routes.copy()
            routes.update(term.routes)
            term = JoinTerm(self.state.tag(), trunk_term, term,
                            joints, False, False,
                            self.flow, trunk_term.baseline, routes)
        return term


class CompileFiltered(CompileFlow):
    """
    Compiles a term corresponding to a filtered flow.
    """

    adapts(FilteredFlow)

    def __call__(self):
        # To construct a term for a filtered flow, we start with
        # a term for its base, ensure that it could generate the given
        # predicate expression and finally wrap it with a filter term
        # node.

        # The term corresponding to the flow base.
        term = self.state.compile(self.flow.base)

        ## Handle the special case when the filter is already enforced
        ## by the mask.  There is no method to directly verify it, so
        ## we prune the masked operations from the flow itself and
        ## its base.  When the filter belongs to the mask, the resulting
        ## flows will be equal.
        #if self.flow.prune(self.mask) == self.flow.base.prune(self.mask):
        #    # We do not need to apply the filter since it is already
        #    # enforced by the mask.  We still need to add the flow
        #    # to the routing table.
        #    routes = term.routes.copy()
        #    # The flow itself and its base share the same inflated flow
        #    # (`backbone`), therefore the backbone must be in the routing
        #    # table.
        #    routes[self.flow] = routes[self.backbone]
        #    return WrapperTerm(self.state.tag(), term, self.flow, routes)

        # Now wrap the base term with a filter term node.
        # Make sure the base term is able to produce the filter expression.
        kid = self.state.inject(term, [self.flow.filter])
        # Inherit the routing table from the base term, add the given
        # flow to the routing table.
        routes = kid.routes.copy()
        for unit in spread(self.flow):
            routes[unit.clone(flow=self.flow)] = routes[unit]
        # Generate a filter term node.
        return FilterTerm(self.state.tag(), kid, self.flow.filter,
                          self.flow, kid.baseline, routes)


class CompileOrdered(CompileFlow):
    """
    Compiles a term corresponding to an ordered flow.
    """

    adapts(OrderedFlow)

    def __call__(self):
        # An ordered flow has two functions:
        # - adding explicit row ordering;
        # - extracting a slice from the row set.
        # Note the first function could be ignored since the compiled terms
        # are not required to respect the ordering of the underlying flow.

        # There are two cases when we could reuse the base term without
        # wrapping it with an order term node:
        # - when the order flow does not apply limit/offset to its base;
        # - when the order flow is already enforced by the mask.
        #if (self.flow.is_expanding or
        #    self.flow.prune(self.mask) == self.flow.base.prune(self.mask)):
        if self.flow.is_expanding:
            # Generate a term for the flow base.
            term = self.state.compile(self.flow.base)
            # Update its routing table to include the given flow and
            # return the node.
            routes = term.routes.copy()
            for unit in spread(self.flow):
                routes[unit.clone(flow=self.flow)] = routes[unit]
            return WrapperTerm(self.state.tag(), term,
                               self.flow, term.baseline, routes)

        # Applying limit/offset requires special care.  Since slicing
        # relies on precise row numbering, the base term must produce
        # exactly the rows of the base.  Therefore we cannot apply any
        # optimizations as they change cardinality of the term.
        # Here we reset the current baseline and mask flows to the
        # scalar flow, which effectively disables any optimizations.
        kid = self.state.compile(self.flow.base,
                                  baseline=self.state.root,
                                  mask=self.state.root)
        # Extract the flow ordering and make sure the base term is able
        # to produce the order expressions.
        order = ordering(self.flow)
        codes = [code for code, direction in order]
        kid = self.state.inject(kid, codes)
        # Add the given flow to the routing table.
        routes = kid.routes.copy()
        for unit in spread(self.flow):
            routes[unit.clone(flow=self.flow)] = routes[unit]
        # Generate an order term.
        return OrderTerm(self.state.tag(), kid, order,
                         self.flow.limit, self.flow.offset,
                         self.flow, kid.baseline, routes)


class InjectCode(Inject):
    """
    Augments the term to make it capable of producing the given expression.
    """

    adapts(Code)

    def __call__(self):
        # Inject all the units that compose the expression.
        return self.state.inject(self.term, self.expression.units)


class InjectUnit(Inject):
    """
    Augments the term to make it produce the given unit.

    Constructor arguments:

    `unit` (:class:`htsql.tr.flow.Unit`)
        A unit node to inject.

    `term` (:class:`htsql.tr.term.Term`)
        A term node to inject into.

    `state` (:class:`CompilingState`)
        The current state of the compiling process.

    Other attributes:

    `flow` (:class:`htsql.tr.flow.Flow`)
        An alias to `unit.flow`.
    """

    adapts(Unit)

    def __init__(self, unit, term, state):
        assert isinstance(unit, Unit)
        super(InjectUnit, self).__init__(unit, term, state)
        self.unit = unit
        # Extract the unit attributes.
        self.flow = unit.flow

    def __call__(self):
        # Normally, this should never be reachable.  We raise an error here
        # to prevent an infinite recursion via `InjectCode` in cases when
        # `Inject` is not implemented for some unit type.
        raise NotImplementedError("the inject adapter is not implemented"
                                  " for a %r node" % self.unit)


class InjectColumn(Inject):
    """
    Injects a column unit into a term.
    """

    adapts(ColumnUnit)

    def __call__(self):
        # We don't keep column units in the routing table (there are too
        # many of them).  Instead presence of a flow node in the routing
        # table indicates that all columns of the prominent table of the
        # flow are exported from the term.

        # To avoid an extra `inject()` call, check if the unit flow
        # is already exported by the term.
        if self.unit in self.term.routes:
            return self.term
        # Verify that the unit is singular on the term flow.
        if not self.term.flow.spans(self.flow):
            raise CompileError("expected a singular expression",
                               self.unit.mark)
        # Inject the unit flow into the term.
        return self.state.inject(self.term, [self.unit.flow])


class InjectScalar(Inject):
    """
    Injects a scalar unit into a term.
    """

    adapts(ScalarUnit)

    def __call__(self):
        # Injecting is already implemented for a batch of scalar units
        # that belong to the same flow.  To avoid code duplication,
        # we delegate injecting to a batch consisting of just one unit.

        # Check if the unit is already exported by the term.
        if self.unit in self.term.routes:
            return self.term
        # Form a batch consisting of a single unit.
        batch = ScalarBatchExpr(self.unit.flow, [self.unit],
                                self.unit.binding)
        # Delegate the injecting to the batch.
        return self.state.inject(self.term, [batch])


class InjectAggregate(Inject):
    """
    Inject an aggregate unit into a term.
    """

    adapts(AggregateUnit)

    def __call__(self):
        # Injecting is already implemented for a batch of aggregate units
        # that share the same base and plural flows.  To avoid code
        # duplication, we delegate injecting to a batch consisting of
        # just one unit.

        # Check if the unit is already exported by the term.
        if self.unit in self.term.routes:
            return self.term
        # Form a batch consisting of a single unit.
        batch = AggregateBatchExpr(self.unit.plural_flow,
                                   self.unit.flow, [self.unit],
                                   self.unit.binding)
        # Delegate the injecting to the batch.
        return self.state.inject(self.term, [batch])


class InjectCorrelated(Inject):
    """
    Injects a correlated unit into a term.
    """

    adapts(CorrelatedUnit)

    def __call__(self):
        # In the term tree, correlated subqueries are represented using
        # a pair of correlation and embedding term nodes.  A correlation
        # term connects its operand to an external *link* term.  An embedding
        # term implants the correlation term into the term tree.

        # Check if the unit is already exported by the term.
        if self.unit in self.term.routes:
            return self.term
        # Verify that the unit is singular on the term flow.
        if not self.term.flow.spans(self.flow):
            raise CompileError("expected a singular expression",
                               self.unit.mark)

        # The general chain of operations is as follows:
        #   - compile a term for the unit flow;
        #   - inject the unit into the unit term;
        #   - attach the unit term to the main term.
        # However, when the unit flow coincides with the term flow,
        # it could be reduced to:
        #   - inject the unit directly into the main term.
        # We say that the unit is *native* to the term if the unit
        # flow coincides with the term flow (or dominates over it).

        # Note that currently the latter is always the case because
        # all correlated units are wrapped with a scalar unit sharing
        # the same unit flow.

        # Check if the unit is native to the term.
        is_native = self.flow.dominates(self.term.flow)
        if is_native:
            # If so, we are going to inject the unit directly into the term.
            unit_term = self.term
        else:
            # Otherwise, compile a separate term for the unit flow.
            # Note: currently, not reachable.
            unit_term = self.compile_shoot(self.flow, self.term)

        # Compile a term for the correlated subquery.
        plural_term = self.compile_shoot(self.unit.plural_flow,
                                         unit_term, [self.unit.code])
        # The ties connecting the correlated subquery to the main query.
        joints = self.tie_terms(unit_term, plural_term)
        # Make sure that the unit term could export tie conditions.
        unit_term = self.inject_ties(unit_term, joints)
        # Connect the plural term to the unit term.
        plural_term = CorrelationTerm(self.state.tag(), plural_term,
                                      unit_term, joints, plural_term.flow,
                                      plural_term.baseline, plural_term.routes)
        # Implant the correlation term into the term tree.
        routes = unit_term.routes.copy()
        routes[self.unit] = plural_term.tag
        unit_term = EmbeddingTerm(self.state.tag(), unit_term, plural_term,
                                  unit_term.flow, unit_term.baseline, routes)
        # If we attached the unit directly to the main term, we are done.
        if is_native:
            return unit_term
        # Otherwise, we need to attach the unit term to the main term.
        extra_routes = { self.unit: plural_term.tag }
        return self.join_terms(self.term, unit_term, extra_routes)


class InjectKernel(Inject):

    adapts(KernelUnit)

    def __call__(self):
        if self.unit in self.term.routes:
            return self.term
        if not self.term.flow.spans(self.flow):
            raise CompileError("expected a singular expression",
                               self.unit.mark)
        term = self.state.inject(self.term, [self.flow])
        assert self.unit in term.routes
        return term


class InjectComplement(Inject):

    adapts(ComplementUnit)

    def __call__(self):
        if self.unit in self.term.routes:
            return self.term
        if not self.term.flow.spans(self.flow):
            raise CompileError("expected a singular expression",
                               self.unit.mark)
        flow = self.flow.clone(extra_codes=[self.unit.code])
        baseline = flow
        while not baseline.is_inflated:
            baseline = baseline.base
        unit_term = self.state.compile(flow, baseline=baseline)
        assert self.unit in unit_term.routes
        extra_routes = { self.unit: unit_term.routes[self.unit] }
        return self.join_terms(self.term, unit_term, extra_routes)


class InjectMoniker(Inject):

    adapts(MonikerUnit)

    def __call__(self):
        if self.unit in self.term.routes:
            return self.term
        if not self.term.flow.spans(self.flow):
            raise CompileError("expected a singular expression",
                               self.unit.mark)
        flow = self.flow.clone(extra_codes=[self.unit.code])
        baseline = flow
        while not baseline.is_inflated:
            baseline = baseline.base
        unit_term = self.state.compile(flow, baseline=baseline)
        assert self.unit in unit_term.routes
        extra_routes = { self.unit: unit_term.routes[self.unit] }
        return self.join_terms(self.term, unit_term, extra_routes)


class InjectForked(Inject):

    adapts(ForkedUnit)

    def __call__(self):
        if self.unit in self.term.routes:
            return self.term
        if not self.term.flow.spans(self.flow):
            raise CompileError("expected a singular expression",
                               self.unit.mark)
        flow = self.flow.clone(extra_codes=[self.unit.code])
        baseline = flow
        while not baseline.is_inflated:
            baseline = baseline.base
        unit_term = self.state.compile(flow, baseline=baseline)
        assert self.unit in unit_term.routes
        extra_routes = { self.unit: unit_term.routes[self.unit] }
        return self.join_terms(self.term, unit_term, extra_routes)


class InjectLinked(Inject):

    adapts(LinkedUnit)

    def __call__(self):
        if self.unit in self.term.routes:
            return self.term
        if not self.term.flow.spans(self.flow):
            raise CompileError("expected a singular expression",
                               self.unit.mark)
        flow = self.flow.clone(extra_codes=[self.unit.code])
        baseline = flow
        while not baseline.is_inflated:
            baseline = baseline.base
        unit_term = self.state.compile(flow, baseline=baseline)
        assert self.unit in unit_term.routes
        extra_routes = { self.unit: unit_term.routes[self.unit] }
        return self.join_terms(self.term, unit_term, extra_routes)


class InjectBatch(Inject):
    """
    Injects a batch of expressions into a term.
    """

    adapts(BatchExpr)

    def __init__(self, expression, term, state):
        super(InjectBatch, self).__init__(expression, term, state)
        # Extract attributes of the batch.
        self.collection = expression.collection

    def __call__(self):
        # The easiest way to inject a group of expressions is to inject
        # them into the term one by one.  However, it will not necessarily
        # generate the most optimal term tree.  We could obtain a better
        # tree structure if we group all units of the same form and inject
        # them all together.
        # Here we group similar scalar and aggregate units into scalar
        # and aggregate batch nodes and then inject the batches.  We do not
        # need to do the same for column units since injecting a column
        # unit effectively injects the unit flow making any column from
        # the flow exportable.

        # We start with the given term, at the end, it will be capable of
        # exporting all expressions from the given collection.
        term = self.term

        # Gather all the units from the given collection of expressions.
        units = []
        for expression in self.collection:
            # Ignore flows and other non-code expressions.
            if isinstance(expression, Code):
                for unit in expression.units:
                    # We are only interested in units that are not already
                    # exportable by the term.
                    if unit not in term.routes:
                        units.append(unit)

        # Find all scalar units and group them by the unit flow.  We
        # maintain a separate list of scalar flows to ensure we process
        # the batches in some deterministic order.
        scalar_flows = []
        scalar_flow_to_units = {}
        for unit in units:
            if isinstance(unit, ScalarUnit):
                flow = unit.flow
                if flow not in scalar_flow_to_units:
                    scalar_flows.append(flow)
                    scalar_flow_to_units[flow] = []
                scalar_flow_to_units[flow].append(unit)
        # Form and inject batches of matching scalar units.
        for flow in scalar_flows:
            batch_units = scalar_flow_to_units[flow]
            batch = ScalarBatchExpr(flow, batch_units,
                                    self.term.binding)
            term = self.state.inject(term, [batch])

        # Find all aggregate units and group them by their plural and unit
        # flows.  Maintain a list of pairs of flows to ensure deterministic
        # order of processing the batches.
        aggregate_flow_pairs = []
        aggregate_flow_pair_to_units = {}
        for unit in units:
            if isinstance(unit, AggregateUnit):
                pair = (unit.plural_flow, unit.flow)
                if pair not in aggregate_flow_pair_to_units:
                    aggregate_flow_pairs.append(pair)
                    aggregate_flow_pair_to_units[pair] = []
                aggregate_flow_pair_to_units[pair].append(unit)
        # Form and inject batches of matching aggregate units.
        for pair in aggregate_flow_pairs:
            plural_flow, flow = pair
            group_units = aggregate_flow_pair_to_units[pair]
            group = AggregateBatchExpr(plural_flow, flow, group_units,
                                       self.term.binding)
            term = self.state.inject(term, [group])

        # Finally, just take and inject all the given expressions.  We don't
        # have to bother with filtering out duplicates or expressions that
        # are already injected.
        for expression in self.collection:
            term = self.state.inject(term, [expression])
        return term


class InjectScalarBatch(Inject):
    """
    Injects a batch of scalar units sharing the same flow.
    """

    adapts(ScalarBatchExpr)

    def __init__(self, expression, term, state):
        super(InjectScalarBatch, self).__init__(expression, term, state)
        # Extract attributes of the batch.
        self.flow = expression.flow

    def __call__(self):
        # To inject a scalar unit into a term, we need to do the following:
        # - compile a term for the unit flow;
        # - inject the unit into the unit term;
        # - attach the unit term to the main term.
        # If we do this for each unit individually, we may end up with
        # a lot of identical unit terms in our term tree.  To optimize
        # the term tree in this scenario, we collect all scalar units
        # sharing the same flow into a batch expression.  Then, when
        # injecting the batch, we use the same unit term for all units
        # in the batch.

        # Get the list of units that are not already exported by the term.
        units = [unit for unit in self.collection
                      if unit not in self.term.routes]
        # If none, there is nothing to be done.
        if not units:
            return self.term
        # Verify that the units are singular relative to the term.
        # To report an error, we could point to any unit node.
        if not self.term.flow.spans(self.flow):
            raise CompileError("expected a singular expression",
                               units[0].mark)
        # Extract the unit expressions.
        codes = [unit.code for unit in units]

        # Handle the special case when the unit flow is equal to the
        # term flow or dominates it.  In this case, we could inject
        # the units directly to the main term and avoid creating
        # a separate unit term.
        if self.flow.dominates(self.term.flow):
            # Make sure the term could export all the units.
            term = self.state.inject(self.term, codes)
            # Add all the units to the routing table.  Note that we point
            # the units to the wrapper because the given term could be
            # terminal (i.e., a table) and SQL syntax does not permit
            # exporting arbitrary expressions from tables.
            tag = self.state.tag()
            routes = term.routes.copy()
            for unit in units:
                routes[unit] = tag
            # Wrap the term with the updated routing table.
            return WrapperTerm(tag, term, term.flow, term.baseline, routes)

        # The general case: compile a term for the unit flow.
        unit_term = self.compile_shoot(self.flow, self.term, codes)
        # SQL syntax does not permit us evaluating arbitrary
        # expressions in terminal terms, so we wrap such terms with
        # a no-op wrapper.
        if unit_term.is_nullary:
            unit_term = WrapperTerm(self.state.tag(), unit_term,
                                    unit_term.flow, unit_term.baseline,
                                    unit_term.routes.copy())
        # And join it to the main term.
        extra_routes = dict((unit, unit_term.tag) for unit in units)
        return self.join_terms(self.term, unit_term, extra_routes)


class InjectAggregateBatch(Inject):
    """
    Injects a batch of aggregate units sharing the same plural and unit flows.
    """

    adapts(AggregateBatchExpr)

    def __init__(self, expression, term, state):
        super(InjectAggregateBatch, self).__init__(expression, term, state)
        # Extract attributes of the batch.
        self.plural_flow = expression.plural_flow
        self.flow = expression.flow

    def __call__(self):
        # To inject an aggregate unit into a term, we do the following:
        # - compile a term for the unit flow;
        # - compile a term for the plural flow relative to the unit term;
        # - inject the unit expression into the plural term;
        # - project plural term into the unit flow;
        # - attach the projected term to the unit term;
        # - attach the unit term to the main term.
        # When the unit flow coincides with the main term flow, we could
        # avoid compiling a separate unit term, and instead attach the
        # projected term directly to the main term.

        # In any case, if we perform this procedure for each unit
        # individually, we may end up with a lot of identical unit terms
        # in the final term tree.  So when there are more than one aggregate
        # unit with the same plural and unit flows, it make sense to
        # collect all of them into a batch expression.  Then, when injecting
        # the batch, we could reuse the same unit and plural terms for all
        # aggregates in the batch.

        # Get the list of units that are not already exported by the term.
        units = [unit for unit in self.collection
                      if unit not in self.term.routes]
        # If none, there is nothing to do.
        if not units:
            return self.term
        # Verify that the units are singular relative to the term.
        # To report an error, we could point to any unit node available.
        if not self.term.flow.spans(self.flow):
            raise CompileError("expected a singular expression",
                               units[0].mark)
        # Extract the aggregate expressions.
        codes = [unit.code for unit in units]

        # Check if the unit flow coincides with or dominates the term
        # flow.  In this case we could avoid compiling a separate unit
        # term and instead attach the projected term directly to the main
        # term.
        is_native = self.flow.dominates(self.term.flow)
        if is_native:
            unit_term = self.term
        else:
            # Compile a separate term for the unit flow.
            # Note: currently it is not reachable since we wrap every
            # aggregate with a scalar unit sharing the same flow.
            unit_term = self.compile_shoot(self.flow, self.term)

        # Compile a term for the plural flow against the unit flow,
        # and inject all the aggregate expressions into it.
        plural_term = self.compile_shoot(self.plural_flow,
                                         unit_term, codes)
        # Generate ties to attach the projected term to the unit term.
        joints = self.tie_terms(unit_term, plural_term)
        # Make sure the unit term could export the tie conditions.
        unit_term = self.inject_ties(unit_term, joints)

        # Now we are going to project the plural term onto the unit
        # flow.  As the projection basis, we are using the ties.
        # There are two kinds of ties we could get from `tie_terms()`:
        # - a list of parallel ties;
        # - or a single serial tie.
        #
        # If we get a list of parallel ties, the projection basis
        # comprises the primary keys of the tie flows.  Otherwise,
        # the basis is the foreign key that joins the tie flow to
        # its base.  These are also the columns connecting the
        # projected term to the unit term.
        basis = [runit for lunit, runit in joints]

        # Determine the flow of the projected term.
        projected_flow = QuotientFlow(self.flow.inflate(),
                                        self.plural_flow, [],
                                        self.expression.binding)
        # The routing table of the projected term.
        # FIXME: the projected term should be able to export the tie
        # conditions, so we add the tie flows to the routing table.
        # However we should never attempt to export any columns than
        # those that form the tie condition -- it will generate invalid
        # SQL.  It is not clear how to fix this, perhaps the routing
        # table should contain entries for each of the columns, or
        # a special entry for just the tie conditions?
        # FIXME: alternatively, convert the kernel into a scalar unit
        # and export only the aggregate and the kernel units from
        # the projected term.  This seems to be the most correct approach,
        # but then what to do with the requirement that each term exports
        # its own flow and backbone?
        tag = self.state.tag()
        routes = {}
        joints_copy = joints
        joints = []
        for joint in joints_copy:
            rop = KernelUnit(joint.rop, projected_flow, joint.rop.binding)
            routes[rop] = tag
            joints.append(joint.clone(rop=rop))

        ## The term flow must always be in the routing table.  The actual
        ## route does not matter since it should never be used.
        #routes[projected_flow] = plural_term.tag
        # Project the plural term onto the basis of the unit flow.
        projected_term = ProjectionTerm(tag, plural_term, basis,
                                        projected_flow, projected_flow,
                                        routes)
        # Attach the projected term to the unit term, add extra entries
        # to the routing table for each of the unit in the collection.
        is_left = (not projected_flow.dominates(unit_term.flow))
        is_right = False
        # Use the routing table of the trunk term, but also add
        # the given extra routes.
        routes = unit_term.routes.copy()
        for unit in units:
            routes[unit] = projected_term.tag
        # Generate and return a join term.
        unit_term = JoinTerm(self.state.tag(), unit_term, projected_term,
                             joints, is_left, is_right,
                             unit_term.flow, unit_term.baseline, routes)
        # For native units, we are done since we use the main term as
        # the unit term.  Note: currently this condition always holds.
        if is_native:
            return unit_term
        # Otherwise, attach the unit term to the main term.
        extra_routes = dict((unit, projected_term.tag) for unit in units)
        return self.join_terms(self.term, unit_term, extra_routes)


class OrderFlow(Adapter):

    adapts(Flow)

    def __init__(self, flow, with_strong=True, with_weak=True):
        assert isinstance(flow, Flow)
        assert isinstance(with_strong, bool)
        assert isinstance(with_weak, bool)
        self.flow = flow
        self.with_strong = with_strong
        self.with_weak = with_weak

    def __call__(self):
        return ordering(self.flow.base, self.with_strong, self.with_weak)


class SpreadFlow(Adapter):

    adapts(Flow)

    def __init__(self, flow):
        assert isinstance(flow, Flow)
        self.flow = flow

    def __call__(self):
        if not self.flow.is_axis:
            return spread(self.flow.base)
        return []


class SewFlow(Adapter):

    adapts(Flow)

    def __init__(self, flow):
        assert isinstance(flow, Flow)
        self.flow = flow

    def __call__(self):
        if not self.flow.is_axis:
            return sew(self.flow.base)
        return []


class TieFlow(Adapter):

    adapts(Flow)

    def __init__(self, flow):
        assert isinstance(flow, Flow)
        self.flow = flow

    def __call__(self):
        if not self.flow.is_axis:
            return tie(self.flow.base)
        return []


class OrderRoot(OrderFlow):

    adapts(RootFlow)

    def __call__(self):
        return []


class OrderScalar(OrderFlow):

    adapts(ScalarFlow)

    def __call__(self):
        return ordering(self.flow.base, with_strong=self.with_strong,
                                         with_weak=self.with_weak)


class OrderTable(OrderFlow):

    adapts(TableFlow)

    def __call__(self):
        # A table flow complements the weak ordering of its base with
        # implicit table ordering.

        for code, direction in ordering(self.flow.base,
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


class SpreadTable(SpreadFlow):

    adapts(TableFlow)

    def __call__(self):
        flow = self.flow.inflate()
        for column in flow.family.table.columns:
            yield ColumnUnit(column, flow, self.flow.binding)


class SewTable(SewFlow):

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


class TieFiberTable(TieFlow):

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


class OrderQuotient(OrderFlow):

    adapts(QuotientFlow)

    def __call__(self):
        for code, direction in ordering(self.flow.base,
                                        with_strong=self.with_strong,
                                        with_weak=self.with_weak):
            yield (code, direction)
        if self.with_weak:
            flow = self.flow.inflate()
            for code in self.flow.family.kernel:
                code = KernelUnit(code, flow, code.binding)
                yield (code, +1)


class SpreadQuotient(SpreadFlow):

    adapts(QuotientFlow)

    def __call__(self):
        flow = self.flow.inflate()
        for lunit, runit in tie(flow.family.seed_baseline):
            yield KernelUnit(runit, flow, runit.binding)
        for code in self.flow.family.kernel:
            yield KernelUnit(code, flow, code.binding)


class SewQuotient(SewFlow):

    adapts(QuotientFlow)

    def __call__(self):
        flow = self.flow.inflate()
        for joint in tie(flow.family.seed_baseline):
            op = KernelUnit(joint.rop, flow, joint.rop.binding)
            yield joint.clone(lop=op, rop=op)
        for code in flow.family.kernel:
            unit = KernelUnit(code, flow, code.binding)
            yield Joint(unit, unit)


class TieQuotient(TieFlow):

    adapts(QuotientFlow)

    def __call__(self):
        flow = self.flow.inflate()
        for joint in tie(flow.family.seed_baseline):
            rop = KernelUnit(joint.rop, flow, joint.rop.binding)
            yield joint.clone(rop=rop)


class OrderComplement(OrderFlow):

    adapts(ComplementFlow)

    def __call__(self):
        for code, direction in ordering(self.flow.base,
                                        with_strong=self.with_strong,
                                        with_weak=self.with_weak):
            yield (code, direction)
        if self.with_weak:
            flow = self.flow.inflate()
            for code, direction in ordering(self.flow.base.family.seed):
                if any(not self.flow.base.spans(unit.flow)
                       for unit in code.units):
                    code = ComplementUnit(code, flow, code.binding)
                    yield (code, direction)


class SpreadComplement(SpreadFlow):

    adapts(ComplementFlow)

    def __call__(self):
        flow = self.flow.inflate()
        seed = self.flow.base.family.seed.inflate()
        for unit in spread(seed):
            yield unit.clone(flow=flow)


class SewComplement(SewFlow):

    adapts(ComplementFlow)

    def __call__(self):
        flow = self.flow.inflate()
        seed = self.flow.base.family.seed.inflate()
        baseline = self.flow.base.family.seed_baseline.inflate()
        axes = []
        axis = seed
        while axis is not None and axis.concludes(baseline):
            axes.append(axis)
            axis = axis.base
        axes.reverse()
        for axis in axes:
            if not axis.is_contracting or axis == baseline:
                for joint in sew(axis):
                    op = ComplementUnit(joint.lop, flow, joint.lop.binding)
                    yield joint.clone(lop=op, rop=op)


class TieComplement(TieFlow):

    adapts(ComplementFlow)

    def __call__(self):
        flow = self.flow.inflate()
        for joint in tie(flow.base.family.seed_baseline):
            lop = KernelUnit(joint.rop, flow.base, joint.rop.binding)
            rop = ComplementUnit(joint.rop, flow, joint.rop.binding)
            yield joint.clone(lop=lop, rop=rop)
        for code in flow.base.family.kernel:
            lop = KernelUnit(code, flow.base, code.binding)
            rop = ComplementUnit(code, flow, code.binding)
            yield Joint(lop=lop, rop=rop)


class OrderMoniker(OrderFlow):

    adapts(MonikerFlow)

    def __call__(self):
        for code, direction in ordering(self.flow.base,
                                        with_strong=self.with_strong,
                                        with_weak=self.with_weak):
            yield (code, direction)
        if self.with_weak:
            flow = self.flow.inflate()
            for code, direction in ordering(self.flow.seed):
                if any(not self.flow.base.spans(unit.flow)
                       for unit in code.units):
                    code = MonikerUnit(code, flow, code.binding)
                    yield (code, direction)


class SpreadMoniker(SpreadFlow):

    adapts(MonikerFlow)

    def __call__(self):
        flow = self.flow.inflate()
        seed = self.flow.seed.inflate()
        for unit in spread(seed):
            yield unit.clone(flow=flow)


class SewMoniker(SewFlow):

    adapts(MonikerFlow)

    def __call__(self):
        flow = self.flow.inflate()
        seed = self.flow.seed.inflate()
        baseline = self.flow.seed_baseline.inflate()
        axes = []
        axis = seed
        while axis is not None and axis.concludes(baseline):
            axes.append(axis)
            axis = axis.base
        axes.reverse()
        for axis in axes:
            if not axis.is_contracting or axis == baseline:
                for joint in sew(axis):
                    op = MonikerUnit(joint.lop, flow, joint.lop.binding)
                    yield joint.clone(lop=op, rop=op)


class TieMoniker(TieFlow):

    adapts(MonikerFlow)

    def __call__(self):
        flow = self.flow.inflate()
        for joint in tie(flow.seed_baseline):
            rop = MonikerUnit(joint.rop, flow, joint.rop.binding)
            yield joint.clone(rop=rop)


class OrderForked(OrderFlow):

    adapts(ForkedFlow)

    def __call__(self):
        for code, direction in ordering(self.flow.base,
                                        with_strong=self.with_strong,
                                        with_weak=self.with_weak):
            yield (code, direction)
        if self.with_weak and not self.flow.is_contracting:
            flow = self.flow.inflate()
            for code, direction in ordering(self.flow.seed):
                if all(self.flow.seed_baseline.base.spans(unit.flow)
                       for unit in code.units):
                    continue
                code = ForkedUnit(code, flow, code.binding)
                yield (code, direction)


class SpreadForked(SpreadFlow):

    adapts(ForkedFlow)

    def __call__(self):
        flow = self.flow.inflate()
        seed = self.flow.seed.inflate()
        for unit in spread(seed):
            yield unit.clone(flow=flow)


class SewForked(SewFlow):

    adapts(ForkedFlow)

    def __call__(self):
        flow = self.flow.inflate()
        seed = self.flow.seed.inflate()
        for joint in sew(seed):
            op = ForkedUnit(joint.lop, flow, joint.lop.binding)
            yield joint.clone(lop=op, rop=op)


class TieForked(TieFlow):

    adapts(ForkedFlow)

    def __call__(self):
        flow = self.flow.inflate()
        seed = self.flow.seed.inflate()
        for joint in tie(seed):
            lop = joint.rop
            rop = ForkedUnit(lop, flow, lop.binding)
            yield joint.clone(lop=lop, rop=rop)
        for code in self.flow.kernel:
            lop = code
            rop = ForkedUnit(code, flow, code.binding)
            yield Joint(lop, rop)


class OrderLinked(OrderFlow):

    adapts(LinkedFlow)

    def __call__(self):
        for code, direction in ordering(self.flow.base,
                                        with_strong=self.with_strong,
                                        with_weak=self.with_weak):
            yield (code, direction)
        if self.with_weak:
            flow = self.flow.inflate()
            for code, direction in ordering(self.flow.seed):
                if any(not self.flow.base.spans(unit.flow)
                       for unit in code.units):
                    code = LinkedUnit(code, flow, code.binding)
                    yield (code, direction)


class SpreadLinked(SpreadFlow):

    adapts(LinkedFlow)

    def __call__(self):
        flow = self.flow.inflate()
        seed = self.flow.seed.inflate()
        for unit in spread(seed):
            yield unit.clone(flow=flow)


class SewLinked(SewFlow):

    adapts(LinkedFlow)

    def __call__(self):
        flow = self.flow.inflate()
        seed = self.flow.seed.inflate()
        baseline = self.flow.seed_baseline.inflate()
        axes = []
        axis = seed
        while axis is not None and axis.concludes(baseline):
            axes.append(axis)
            axis = axis.base
        axes.reverse()
        for axis in axes:
            if not axis.is_contracting or axis == baseline:
                for joint in sew(axis):
                    op = LinkedUnit(joint.lop, flow, joint.lop.binding)
                    yield joint.clone(lop=op, rop=op)


class TieLinked(TieFlow):

    adapts(LinkedFlow)

    def __call__(self):
        flow = self.flow.inflate()
        for lop, rop in zip(flow.counter_kernel, flow.kernel):
            rop = LinkedUnit(rop, flow, rop.binding)
            yield Joint(lop, rop)


class OrderOrdered(OrderFlow):

    adapts(OrderedFlow)

    def __call__(self):
        if self.with_strong:
            for code, direction in ordering(self.flow.base,
                                            with_strong=True, with_weak=False):
                yield (code, direction)
            for code, direction in self.flow.order:
                yield (code, direction)
        if self.with_weak:
            for code, direction in ordering(self.flow.base,
                                            with_strong=False, with_weak=True):
                yield (code, direction)


def ordering(flow, with_strong=True, with_weak=True):
    ordering = OrderFlow(flow, with_strong, with_weak)
    return list(ordering())


def spread(flow):
    spread = SpreadFlow(flow)
    return list(spread())


def sew(flow):
    sew = SewFlow(flow)
    return list(sew())


def tie(flow):
    tie = TieFlow(flow)
    return list(tie())


def compile(expression, state=None, baseline=None, mask=None):
    """
    Compiles a new term node for the given expression.

    Returns a :class:`htsql.tr.term.Term` instance.

    `expression` (:class:`htsql.tr.flow.Expression`)
        An expression node.

    `state` (:class:`CompilingState` or ``None``)
        The compiling state to use.  If not set, a new compiling state
        is instantiated.

    `baseline` (:class:`htsql.tr.flow.Flow` or ``None``)
        The baseline flow.  Specifies an axis that the compiled
        term must export.  If not set, the current baseline flow of
        the state is used.

    `mask` (:class:`htsql.tr.flow.Flow` or ``None``)
        The mask flow.  Specifies the mask flow against which
        a new term is compiled.  When not set, the current mask flow
        of the state is used.
    """
    # Instantiate a new compiling state if not given one.
    if state is None:
        state = CompilingState()
    # If passed, assign new baseline and mask flows.
    if baseline is not None:
        state.push_baseline(baseline)
    if mask is not None:
        state.push_mask(mask)
    # Realize and apply the `Compile` adapter.
    compile = Compile(expression, state)
    term = compile()
    # Restore old baseline and mask flows.
    if baseline is not None:
        state.pop_baseline()
    if mask is not None:
        state.pop_mask()
    # Return the compiled term.
    return term


