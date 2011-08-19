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
from .coerce import coerce
from .signature import IsNullSig, AndSig
from .flow import (Expression, QueryExpr, SegmentExpr, Code,
                   FormulaCode, Flow, RootFlow, ScalarFlow,
                   TableFlow, DirectTableFlow, FiberTableFlow,
                   QuotientFlow, ComplementFlow, MonikerFlow,
                   ForkedFlow, LinkedFlow, FilteredFlow, OrderedFlow,
                   Unit, ScalarUnit, ColumnUnit, AggregateUnit, CorrelatedUnit,
                   KernelUnit, CoveringUnit)
from .term import (Term, ScalarTerm, TableTerm, FilterTerm, JoinTerm,
                   EmbeddingTerm, CorrelationTerm, ProjectionTerm, OrderTerm,
                   WrapperTerm, SegmentTerm, QueryTerm, Joint)
from .stitch import arrange, spread, sew, tie


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
        self.root = flow
        self.baseline = flow

    def flush(self):
        """
        Clears the state flows.
        """
        # Check that the state flows are initialized and the flow stacks
        # are exhausted.
        assert self.root is not None
        assert not self.baseline_stack
        assert self.baseline is self.root
        self.root = None
        self.baseline = None

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

    def compile(self, expression, baseline=None):
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
        """
        # FIXME: potentially, we could implement a cache of `expression`
        # -> `term` to avoid generating the same term node more than once.
        # There are several complications though.  First, the term depends
        # not only on the expression, but also on the current baseline
        # and mask flows.  Second, each compiled term must have a unique
        # tag, therefore we'd have to replace the tags and route tables
        # of the cached term node.
        return compile(expression, self, baseline=baseline)

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

        `expressions` (a list of :class:`htsql.tr.flow.Expression`)
            A list of expressions to inject into the given term.
        """
        assert isinstance(term, Term)
        assert isinstance(expressions, listof(Expression))
        # Iterate over the expressions to inject.
        for expression in expressions:
            # A quick check to avoid a costly adapter call.  This
            # only works if the expression is a unit.
            if expression in term.routes:
                continue
            # Inject the expression into the term.
            inject = Inject(expression, term, self)
            term = inject()
        # Return the augmented term node.
        return term


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

        # This condition is enforced by unmasking process -- all
        # non-axial operations in the trunk flow are pruned from
        # the given flow.
        assert flow == flow.prune(trunk_term.flow)

        # Determine the longest ancestor of the flow that contains
        # no non-axial operations.
        baseline = flow
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

        # Compile the term for the given flow up to the baseline.
        term = self.state.compile(flow, baseline=baseline)

        # If provided, inject the given expressions.
        if codes is not None:
            term = self.state.inject(term, codes)

        # Return the compiled shoot term.
        return term

    def glue_terms(self, trunk_term, shoot_term):
        """
        Returns joints to attach the shoot term to the trunk term.

        `trunk_term` (:class:`htsql.tr.term.Term`)
            The left (trunk) operand of the join.

        `shoot_term` (:class:`htsql.tr.term.Term`)
            The right (shoot) operand of the join.

        Note that the trunk term may not export all the units necessary
        to generate join conditions.  Apply :meth:`inject_joints` on the
        trunk before using the joints to join the trunk and the shoot.
        """
        # Sanity check on the arguments.
        assert isinstance(trunk_term, Term)
        assert isinstance(shoot_term, Term)
        # Verify that it is possible to join the terms without
        # changing the cardinality of the trunk.
        assert (shoot_term.baseline.is_root or
                trunk_term.flow.spans(shoot_term.baseline.base))

        # There are two ways the joints are generated:
        #
        # - when the shoot baseline is an axis of the trunk flow,
        #   in this case we join the terms using parallel joints on
        #   the common axes;
        # - otherwise, join the terms using a serial joint between
        #   the shoot baseline and its base.

        # Joints to attach the shoot to the trunk.
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
            # generate a parallel joint.  Note that we do not verify
            # (and, in general, it is not required) that these axes
            # are exported by the trunk term.  Apply `inject_joints()` on
            # the trunk term before using the joints to join the terms.
            axes = []
            while axis != shoot_term.baseline.base:
                # Skip non-expanding axes (but always include the baseline).
                if not axis.is_contracting or axis == shoot_term.baseline:
                    axes.append(axis)
                axis = axis.base
            # We prefer (for no particular reason) the joints to go
            # from shortest to longest axes.
            axes.reverse()
            for axis in axes:
                joints.extend(sew(axis))
        else:
            # When the shoot does not touch the trunk flow, we attach it
            # using a serial joint between the shoot baseline and its base.
            # Note that we do not verify (and it is not required) that
            # the trunk term exports the base flow.  Apply `inject_joints()`
            # on the trunk term to inject any necessary flows before
            # joining the terms using the joints.
            joints = tie(shoot_term.baseline)

        # Return the generated joints.
        return joints

    def inject_joints(self, term, joints):
        """
        Augments the term to ensure it can export all units required
        to generate join conditions.

        `term` (:class:`htsql.tr.term.Term`)
            The term to update.

            It is assumed that `term` was the argument `trunk_term` of
            :meth:`glue_terms` when the joints were generated.

        `joints` (a list of :class:`htsql.tr.term.Joint`)
            The joints to inject.

            It is assumed the ties were generated by :meth:`glue_terms`.
        """
        # Sanity check on the arguments.
        assert isinstance(term, Term)
        assert isinstance(joints, listof(Joint))

        codes = [lop for lop, rop in joints]
        return self.state.inject(term, codes)

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
        assert trunk_term.flow.spans(shoot_term.flow)
        assert isinstance(extra_routes, dict)

        # Join conditions that glue the terms.
        joints = self.glue_terms(trunk_term, shoot_term)
        # Make sure the trunk term could export the joints (this
        # may change the baseline of the trunk term).
        trunk_term = self.inject_joints(trunk_term, joints)
        # Determine if we could use an inner join to attach the shoot
        # to the trunk.  We could do it if the inner join does not
        # decrease cardinality of the trunk.
        # FIXME: The condition that the shoot flow dominates the
        # trunk flow is sufficient, but not really necessary.
        # In general, we can use the inner join if the shoot flow
        # dominates the ancestor of the trunk flow cut at the longest
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

    adapts(QueryExpr)

    def __call__(self):
        # Initialize the all state flows with a root scalar flow.
        self.state.set_root(RootFlow(None, self.expression.binding))
        # Compile the segment term.
        segment = None
        if self.expression.segment is not None:
            segment = self.state.compile(self.expression.segment)
        # Shut down the state flows.
        self.state.flush()
        # Construct a query term.
        return QueryTerm(segment, self.expression)


class CompileSegment(Compile):

    adapts(SegmentExpr)

    def __call__(self):
        # Get the ordering of the segment flow.
        order = arrange(self.expression.flow)
        # List of expressions we need the term to export.
        codes = (self.expression.elements +
                 [code for code, direction in order])
        # Construct a term corresponding to the segment flow.
        kid = self.state.compile(self.expression.flow)
        # Inject the expressions into the term.
        kid = self.state.inject(kid, codes)
        # The compiler does not guarantee that the produced term respects
        # the flow ordering, so it is our responsitibity to wrap the term
        # with an order node.
        if order:
            kid = OrderTerm(self.state.tag(), kid, order, None, None,
                            kid.flow, kid.baseline, kid.routes.copy())
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

    When compiling a term for a flow node, the current `baseline` flow
    denotes the leftmost axis that the term should be able to export.
    The compiler may (but does not have to) omit any axes nested under
    the `baseline` axis.

    The generated term is not required to respect the ordering of the flow.

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


class InjectFlow(Inject):

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
        if all(unit in self.term.routes for unit in spread(self.flow)):
            return self.term

        # Check that the flow does not contain any non-axial operations
        # of the term flow -- that's enforced by unmasking process.
        assert self.flow == self.flow.prune(self.term.flow)

        # A special case when the given flow is an ancestor of the term
        # flow.  The fact that the flow is not exported by the term means
        # that the term tree is optimized by cutting all axes below some
        # baseline.  Now we need to grow these axes back.
        if self.term.flow.concludes(self.flow):
            # Verify that the flow is not in the term.
            assert self.term.baseline.base.concludes(self.flow)

            # Here we compile a term corresponding to the flow and
            # attach it to the axis directly above it using a serial joint.

            # Compile a term for the missing axes.
            lkid = self.state.compile(self.term.baseline.base,
                                       baseline=self.flow)
            rkid = self.term

            # Join the terms using a serial joint.
            joints = tie(self.term.baseline)
            lkid = self.inject_joints(lkid, joints)
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
            extra_routes[unit] = flow_term.routes[unit]
        # Join the shoot to the main term.
        return self.join_terms(self.term, flow_term, extra_routes)


class CompileRoot(CompileFlow):

    adapts(RootFlow)

    def __call__(self):
        # Generate a scalar term (the baseline must coincide with the flow).
        return ScalarTerm(self.state.tag(), self.flow, self.flow, {})


class CompileScalar(CompileFlow):

    adapts(ScalarFlow)

    def __call__(self):
        # If we are at the baseline, generate a scalar term.
        if self.flow == self.baseline:
            return ScalarTerm(self.state.tag(), self.flow, self.flow, {})
        # Otherwise, compile a term for the parent flow and reuse
        # it for the scalar flow.
        term = self.state.compile(self.flow.base)
        return WrapperTerm(self.state.tag(), term,
                           self.flow, term.baseline, term.routes)


class CompileTable(CompileFlow):

    # Used for both direct and fiber table flows.
    adapts(TableFlow)

    def __call__(self):
        # We start with identifying and handling special cases, where
        # we able to generate a more optimal, less compex term tree than
        # in the regular case.  If none of the special cases are applicable,
        # we use the generic algorithm.

        # The first special case: we are at the baseline flow.
        if self.flow == self.baseline:
            # Generate a single table term.
            tag = self.state.tag()
            # The routing table includes all the columns of the table.
            routes = {}
            for unit in spread(self.flow):
                routes[unit] = tag
            return TableTerm(tag, self.flow, self.baseline, routes)

        # Otherwise, we need a term corresponding to the parent flow.
        term = self.state.compile(self.flow.base)

        # The second special case, when the term of the parent flow could also
        # serve as a term for the flow itself.  It is possible if the
        # following two conditions are met:
        # - the term exports the inflation of the given flow (`backbone`),
        # - the given flow conforms (has the same cardinality as) its base.
        # This case usually corresponds to an HTSQL expression of the form:
        #   (A?p(B)).B,
        # where `B` is a singular, non-nullable link from `A` and `p(B)` is
        # a predicate expression on `B`.
        if (self.flow.conforms(term.flow) and
            all(unit in term.routes for unit in spread(self.backbone))):
            # We need to add the given flow to the routing table and
            # replace the term flow.
            routes = term.routes.copy()
            for unit in spread(self.flow):
                routes[unit] = routes[unit.clone(flow=self.backbone)]
            return WrapperTerm(self.state.tag(), term,
                               self.flow, term.baseline, routes)

        # Now the general case.  We take two terms:
        # - the term compiled for the parent flow
        # - and a table term corresponding to the flow table,
        # and join them using the tie between the flow and its parent.

        # This is the term for the flow base, we already generated it.
        lkid = term
        # This is a table term corresponding to the flow table.
        # Instead of generating it directly, we call `compile`
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
            routes[unit] = routes[unit.clone(flow=self.backbone)]
        # Generate a join term node.
        return JoinTerm(self.state.tag(), lkid, rkid, joints,
                        is_left, is_right, self.flow, lkid.baseline, routes)


class CompileQuotient(CompileFlow):

    adapts(QuotientFlow)

    def __call__(self):
        # Normally, a quotient flow is represented by a seed term with
        # the baseline at the ground term.  If we can generate a term
        # with this shape, it is wrapped by a filter term to eliminate
        # `NULL` from the kernel and then by a projection term to
        # generate a proper quotient term.

        # However it may happen that the seed term has the baseline
        # shorter than the ground.  In this case, the term has irregular
        # parallel and serial ties and therefore cannot represent
        # the quotient axis.  To hide the irregular structure, we are
        # forced to generate a trunk term from the parent flow and
        # manually project and attach the seed term to the trunk term.

        # In addition, we may be asked to export some aggregates
        # over the complement flow.  We generate aggregate expressions
        # by pretending that the seed term actually represents
        # the complement flow and injecting the expressions into it.

        # Start with generating a term for the seed flow.

        # The ground flow is expected to be the baseline of the seed term.
        baseline = self.flow.ground
        # However, the ground may not be inflated, so we need to find
        # an inflated ancestor.
        while not baseline.is_inflated:
            baseline = baseline.base
        # The seed term.
        seed_term = self.state.compile(self.flow.seed, baseline=baseline)
        # Inject the kernel and filter out `NULL` kernel values.
        if self.flow.kernels:
            # Make sure the kernel expressions are exportable.
            seed_term = self.state.inject(seed_term, self.flow.kernels)
            # Generate filters:
            #   !is_null(kernel)&...
            filters = []
            for code in self.flow.kernels:
                filter = FormulaCode(IsNullSig(-1), coerce(BooleanDomain()),
                                     code.binding, op=code)
                filters.append(filter)
            if len(filters) == 1:
                [filter] = filters
            else:
                filter = FormulaCode(AndSig(), coerce(BooleanDomain()),
                                     self.flow.binding, ops=filters)
            # The final seed term.
            seed_term = FilterTerm(self.state.tag(), seed_term, filter,
                                   seed_term.flow, seed_term.baseline,
                                   seed_term.routes.copy())

        # Indicates that the seed term has the regular shape.
        is_regular = (seed_term.baseline == self.flow.ground)

        # Inject aggregates suggested by the rewriter.

        # List of injected aggregate expressions.
        aggregates = []
        # The plural space for the aggregates.
        complement = ComplementFlow(self.backbone, self.flow.binding)
        # We can only inject aggregates if the seed term has the regular shape.
        if self.flow.companions and is_regular:
            # We are going to disguise the seed term as a complement.
            # The routing table for the complement term.
            routes = {}
            for code in seed_term.routes:
                unit = CoveringUnit(code, complement, code.binding)
                routes[unit] = seed_term.tag
            for code in self.flow.kernels:
                unit = CoveringUnit(code, complement, code.binding)
                routes[unit] = seed_term.tag
            for unit in spread(self.flow.seed.inflate()):
                routes[unit.clone(flow=complement)] = seed_term.routes[unit]
            # Disguise the seed term as a complement term.
            complement_term = WrapperTerm(self.state.tag(), seed_term,
                                          complement, complement, routes)
            # Inject aggregate expressions.
            complement_term = self.state.inject(complement_term,
                                                self.flow.companions)
            # Abort if the shape of the term changed.
            if complement_term.baseline == complement:
                # Remember what we just injected.
                aggregates = self.flow.companions
                # Convert the complement term back to the seed term.
                # The routing table of the seed term will now have
                # extra aggregate expressions.
                routes = {}
                for code in aggregates:
                    for unit in code.units:
                        routes[unit] = complement_term.routes[unit]
                routes.update(seed_term.routes)
                # Back to the seed term.
                seed_term = WrapperTerm(self.state.tag(), complement_term,
                                        seed_term.flow, seed_term.baseline,
                                        routes)

        # Prepare for generating the quotient term.

        # The term for the parent flow (may remain `None` if the baseline
        # is at the quotient).
        trunk_term = None
        # The basis of the projection.
        basis = []
        # The units exported by the projection (against the inflated flow).
        units = []
        # The join conditions attaching the quotient term to the parent term.
        joints = []

        # Generate the trunk term and the join conditions.

        # Handle the regular case first.
        if is_regular:
            # Check if the term for the parent flow is necessary.
            if self.flow != self.baseline:
                # Generate the parent flow and the ties.
                trunk_term = self.state.compile(self.flow.base)
                joints = tie(self.flow)

        # The irregular case, the seed baseline is below the ground.
        else:
            # The trunk term is a must, even if the baseline is at
            # the current flow.  In that case, we need to lower the baseline.
            baseline = self.baseline
            if baseline == self.flow:
                baseline = baseline.base
            # Generate the trunk term.
            trunk_term = self.state.compile(self.flow.base, baseline=baseline)
            # Join conditions between the trunk and the seed terms.
            seed_joints = self.glue_terms(trunk_term, seed_term)
            # Convert the join conditions to joints between the trunk
            # and the projection terms.  Also prepopulate the basis
            # and the list of units.
            for joint in seed_joints:
                basis.append(joint.rop)
                unit = KernelUnit(joint.rop, self.backbone, joint.rop.binding)
                units.append(unit)
                joints.append(joint.clone(rop=unit))

        # Generate the the projection basis and a list of exported units.
        # Note that in the irregular case, those are already prepopulated
        # from the join conditions.
        # The units attaching the seed ground to the parent flow.
        for lop, rop in tie(self.flow.ground):
            basis.append(rop)
            unit = KernelUnit(rop, self.backbone, rop.binding)
            units.append(unit)
        # The kernel expressions.
        for code in self.flow.kernels:
            basis.append(code)
            unit = KernelUnit(code, self.backbone, code.binding)
            units.append(unit)
        # Injected complement aggregates (regular case only).
        for code in aggregates:
            unit = AggregateUnit(code, complement, self.backbone,
                                 code.binding)
            units.append(unit)

        # Generate the projection term.
        tag = self.state.tag()
        # Convert the list of units to a routing table.
        routes = {}
        for unit in units:
            routes[unit] = tag
        # Generate a term node.
        term = ProjectionTerm(tag, seed_term, basis,
                              self.backbone, self.backbone, routes)

        # If there is no parent term, we are done.
        if trunk_term is None:
            return term

        # Otherwise, join the terms.
        lkid = self.inject_joints(trunk_term, joints)
        rkid = term
        # The joined routing table.
        routes = {}
        routes.update(lkid.routes)
        routes.update(rkid.routes)
        # Reparent exported units from the backbone to the original flow.
        for unit in units:
            routes[unit.clone(flow=self.flow)] = rkid.tag
        # Generate and return a join node.
        is_left = False
        is_right = False
        return JoinTerm(self.state.tag(), lkid, rkid, joints,
                        is_left, is_right, self.flow, lkid.baseline, routes)


class CompileComplement(CompileFlow):

    adapts(ComplementFlow)

    def __call__(self):
        # A complement term, just like a quotient term is represented
        # by a seed term with a baseline at the seed ground.  As opposed
        # to the quotient term, we don't have to filter out `NULL` kernel
        # values as this filter is enforced by the quotient anyway.

        # Since the quotient and the complement terms share the same
        # shape, we could reuse the complement term to export the respective
        # quotient flow.  In this case, we need to apply kernel filters.

        # As in the quotient case, the seed term may have an irregular
        # shape, that is, the term baseline lies below the seed ground.
        # In this case, we manually attach the seed term to the trunk.

        # The flow node may contain extra code objects -- `companions`,
        # which indicate that the generated term should export covering
        # units wrapping the companions.

        # Generate the seed term.

        # The baseline of the seed term is expected to be the seed ground flow.
        baseline = self.flow.ground
        # However it may be not inflated, in which case we find the closest
        # inflated axis.
        while not baseline.is_inflated:
            baseline = baseline.base
        # Create the seed term.
        seed_term = self.state.compile(self.flow.seed, baseline=baseline)
        # Make sure the seed term can export the quotient kernel and the
        # extra companion expressions.
        seed_term = self.state.inject(seed_term,
                                      self.flow.kernels + self.flow.companions)

        # Indicates whether the seed term has a regular shape.
        is_regular = (seed_term.baseline == self.flow.ground)

        # Indicates that the generated term can export the quotient flow:
        # - we cannot omit generating the parent term because the baseline
        #   is below the current flow or the seed term is irregular.
        # - there are no filters or other non-axial operations between
        #   the complement and its quotient;
        # - the quotient flow does not have to export any aggregates.
        # Note that the seed term may have an irregular shape.
        has_quotient = ((self.baseline != self.flow or not is_regular) and
                        isinstance(self.flow.base, QuotientFlow) and
                        not self.flow.base.companions)

        # If the term exports the quotient flow, we need to enforce the
        # condition: `!is_null(kernel)`.
        if has_quotient and self.flow.kernels:
            # Generate a filter around the seed term.
            filters = []
            for code in self.flow.kernels:
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

        # Wrap the term to have a target for covering units.
        seed_term = WrapperTerm(self.state.tag(), seed_term,
                                seed_term.flow, seed_term.baseline,
                                seed_term.routes.copy())

        # Prepare for generating the complement term.

        # The term for the parent (or grandparent if `has_quotient`) flow.
        # May remain unset if the baseline at the current or the parent flow.
        trunk_term = None
        # Flow units exported by the term.
        covering_units = []
        # Units from the parent quotient flow exported by the term.
        quotient_units = []
        # Join conditions attaching the term to the trunk.
        joints = []

        # Generate the trunk term if needed.

        # The trunk flow.
        axis = self.flow.base
        # Use the grandparent flow if the quotient is already included
        # in the complement flow.
        if has_quotient:
            axis = axis.base
        # Determine the baseline.
        baseline = self.baseline
        # If the baseline is above the trunk flow, we can avoid generating
        # the trunk term, but only if the seed term has the regular shape.
        # Otherwise, lower the baseline till it reaches the trunk flow.
        if not is_regular:
            while not axis.concludes(baseline):
                baseline = baseline.base
        # Generate the trunk term if needed.
        if axis.concludes(baseline):
            trunk_term = self.state.compile(axis, baseline=baseline)

        # Generate the links to the trunk.
        if trunk_term is not None:
            # Add custom joints for the irregular case.
            if not is_regular:
                seed_joints = self.glue_terms(trunk_term, seed_term)
                for joint in seed_joints:
                    unit = CoveringUnit(joint.rop, self.backbone,
                                        joint.rop.binding)
                    joints.append(joint.clone(rop=unit))
                    # Make sure the joint is exported by the complement term.
                    covering_units.append(unit)

            # Add regular joints: the serial joints from the complement
            # flow (or the parent flow if it is included).
            if has_quotient:
                joints += tie(self.flow.base)
            else:
                joints += tie(self.flow)

        # Populate units exported by the complement.

        # Add units from the parent quotient flow if needed.
        if has_quotient:
            quotient_backbone = self.flow.base.inflate()
            quotient_units = spread(quotient_backbone)

        # Wrap everything produced by the seed term.
        for code in seed_term.routes:
            unit = CoveringUnit(code, self.backbone, code.binding)
            covering_units.append(unit)
        # Ensure we export serial ties.
        for lop, rop in tie(self.flow.ground):
            unit = CoveringUnit(rop, self.backbone, rop.binding)
            covering_units.append(unit)
        # Export the kernel and any requested companion units.
        for code in self.flow.kernels + self.flow.companions:
            unit = CoveringUnit(code, self.backbone, code.binding)
            covering_units.append(unit)

        # Generate the routing table and the complement term.
        routes = {}
        # Export units from the quotient flow, if any.
        for unit in quotient_units:
            routes[unit] = seed_term.tag
        # Export complement units.
        for unit in covering_units:
            routes[unit] = seed_term.tag
        # Export native units.
        for unit in spread(self.flow.seed):
            routes[unit.clone(flow=self.backbone)] = seed_term.routes[unit]
        # The baseline for the complement term.
        baseline = self.backbone
        if has_quotient:
            baseline = baseline.base
        # The complement term.
        term = WrapperTerm(self.state.tag(), seed_term,
                           self.backbone, baseline, routes)

        # If there is no parental term, we are done.
        if trunk_term is None:
            return term

        # Attach the complement term to the trunk.
        lkid = self.inject_joints(trunk_term, joints)
        rkid = term
        # Merge the routing table.
        routes = {}
        routes.update(lkid.routes)
        routes.update(rkid.routes)
        # Now reparent the exported units to the given flow
        # (rather than the backbone).
        for unit in quotient_units:
            routes[unit.clone(flow=self.flow.base)] = seed_term.tag
        for unit in covering_units:
            routes[unit.clone(flow=self.flow)] = seed_term.tag
        for unit in spread(self.flow.seed):
            routes[unit.clone(flow=self.flow)] = seed_term.routes[unit]
        is_left = False
        is_right = False
        # Generate and return the join term node.
        return JoinTerm(self.state.tag(), lkid, rkid, joints,
                        is_left, is_right, self.flow, lkid.baseline, routes)


class CompileMoniker(CompileFlow):

    adapts(MonikerFlow)

    def __call__(self):
        extra_codes = self.flow.companions
        if (self.flow.ground.base is not None and
            self.flow.base.conforms(self.flow.ground.base) and
            not self.flow.base.spans(self.flow.ground)):
            baseline = self.flow.ground
            if not (baseline.is_inflated and
                    self.flow == self.state.baseline):
                while not self.state.baseline.concludes(baseline):
                    baseline = baseline.base
            seed_term = self.state.compile(self.flow.seed, baseline=baseline)
            seed_term = self.state.inject(seed_term, extra_codes)
            if seed_term.baseline != self.flow.ground:
                flow = self.flow.base
                seed_term = self.state.inject(seed_term, [flow])
                while not seed_term.baseline.concludes(flow):
                    seed_term = self.state.inject(seed_term, [flow])
                    flow = flow.base
            seed_term = WrapperTerm(self.state.tag(), seed_term,
                                    seed_term.flow, seed_term.baseline,
                                    seed_term.routes.copy())
            baseline = seed_term.baseline
            if baseline == self.flow.ground:
                baseline = self.flow
            routes = {}
            for unit in seed_term.routes:
                if self.flow.base.spans(unit.flow):
                    routes[unit] = seed_term.routes[unit]
                seed_unit = CoveringUnit(unit, self.flow, unit.binding)
                routes[seed_unit] = seed_term.tag
                seed_unit = CoveringUnit(unit, self.backbone, unit.binding)
                routes[seed_unit] = seed_term.tag
            if extra_codes is not None:
                for code in extra_codes:
                    unit = CoveringUnit(code, self.flow, code.binding)
                    routes[unit] = seed_term.tag
                    unit = CoveringUnit(code, self.backbone, code.binding)
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
                                       extra_codes)
        seed_term = WrapperTerm(self.state.tag(), seed_term,
                                seed_term.flow, seed_term.baseline,
                                seed_term.routes)
        joints = self.glue_terms(trunk_term, seed_term)
        trunk_term = self.inject_joints(trunk_term, joints)
        routes = trunk_term.routes.copy()
        for unit in seed_term.routes:
            seed_unit = CoveringUnit(unit, self.flow, unit.binding)
            routes[seed_unit] = seed_term.tag
            seed_unit = CoveringUnit(unit, self.backbone, unit.binding)
            routes[seed_unit] = seed_term.tag
        if extra_codes is not None:
            for code in extra_codes:
                unit = CoveringUnit(code, self.flow, code.binding)
                routes[unit] = seed_term.tag
                unit = CoveringUnit(code, self.backbone, code.binding)
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
        extra_codes = self.flow.kernels[:] + self.flow.companions
        seed_term = self.state.compile(seed, baseline=baseline)
        seed_term = self.state.inject(seed_term, extra_codes)
        seed_term = WrapperTerm(self.state.tag(), seed_term,
                                seed_term.flow, seed_term.baseline,
                                seed_term.routes.copy())
        if (self.state.baseline == self.flow and
                seed_term.baseline == self.flow.ground):
            routes = {}
            for unit in seed_term.routes:
                seed_unit = CoveringUnit(unit, self.flow, unit.binding)
                routes[seed_unit] = seed_term.tag
                seed_unit = CoveringUnit(unit, self.backbone, unit.binding)
                routes[seed_unit] = seed_term.tag
            for code in extra_codes:
                unit = CoveringUnit(code, self.flow, code.binding)
                routes[unit] = seed_term.tag
                unit = CoveringUnit(code, self.backbone, code.binding)
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
        for code in self.flow.kernels:
            joint = Joint(code, code)
            joints.append(joint)
        units = [lunit for lunit, runit in joints]
        trunk_term = self.state.inject(trunk_term, units)
        routes = trunk_term.routes.copy()
        for unit in seed_term.routes:
            seed_unit = CoveringUnit(unit, self.flow, unit.binding)
            routes[seed_unit] = seed_term.tag
            seed_unit = CoveringUnit(unit, self.backbone, unit.binding)
            routes[seed_unit] = seed_term.tag
        for code in extra_codes:
            unit = CoveringUnit(code, self.flow, code.binding)
            routes[unit] = seed_term.tag
            unit = CoveringUnit(code, self.backbone, code.binding)
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
        baseline = self.flow.ground
        while not baseline.is_inflated:
            baseline = baseline.base
        extra_codes = ([rop for lop, rop in self.flow.images]
                       + self.flow.companions)
        seed_term = self.state.compile(self.flow.seed, baseline=baseline)
        seed_term = self.state.inject(seed_term, extra_codes)
        extra_axes = []
        joints = []
        if seed_term.baseline != self.flow.ground:
            backbone = self.flow.base.inflate()
            axis = seed_term.baseline
            while not backbone.concludes(axis):
                axis = axis.base
            seed_term = self.state.inject(seed_term, [axis])
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
                    rop = CoveringUnit(rop, self.flow.inflate(), rop.binding)
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
        if baseline == self.flow.ground:
            baseline = self.flow
        routes = {}
        for unit in seed_term.routes:
            if self.flow.base.spans(unit.flow):
                routes[unit] = seed_term.routes[unit]
            seed_unit = CoveringUnit(unit, self.flow, unit.binding)
            routes[seed_unit] = seed_term.tag
            seed_unit = CoveringUnit(unit, self.backbone, unit.binding)
            routes[seed_unit] = seed_term.tag
        for joint in joints:
            code = joint.rop.code
            unit = CoveringUnit(code, self.flow, code.binding)
            routes[unit] = seed_term.tag
            unit = CoveringUnit(code, self.backbone, code.binding)
            routes[unit] = seed_term.tag
        if extra_codes is not None:
            for code in extra_codes:
                unit = CoveringUnit(code, self.flow, code.binding)
                routes[unit] = seed_term.tag
                unit = CoveringUnit(code, self.backbone, code.binding)
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

    adapts(FilteredFlow)

    def __call__(self):
        # The term corresponding to the parent flow.
        term = self.state.compile(self.flow.base)
        # Make sure the base term is able to produce the filter expression.
        kid = self.state.inject(term, [self.flow.filter])
        # Inherit the routing table from the base term, but add native
        # units of the given flow.
        routes = kid.routes.copy()
        for unit in spread(self.flow):
            routes[unit] = routes[unit.clone(flow=self.backbone)]
        # Generate a filter term node.
        return FilterTerm(self.state.tag(), kid, self.flow.filter,
                          self.flow, kid.baseline, routes)


class CompileOrdered(CompileFlow):

    adapts(OrderedFlow)

    def __call__(self):
        # An ordered flow has two functions:
        # - adding explicit row ordering;
        # - extracting a slice from the row set.
        # Note the first function could be ignored since the compiled terms
        # are not required to respect the ordering of the underlying flow.

        # When the order flow does not apply limit/offset, we could simply
        # reuse the base term.
        if self.flow.is_expanding:
            # Generate a term for the flow base.
            term = self.state.compile(self.flow.base)
            # Update its routing table to include the given flow and
            # return the node.
            routes = term.routes.copy()
            for unit in spread(self.flow):
                routes[unit] = routes[unit.clone(flow=self.backbone)]
            return WrapperTerm(self.state.tag(), term,
                               self.flow, term.baseline, routes)

        # Applying limit/offset requires special care.  Since slicing
        # relies on precise row numbering, the base term must produce
        # exactly the rows of the base.  Therefore we cannot use any
        # baseline or unmask non-axial operations.

        # Extract the flow ordering and make sure the base term is able
        # to produce the order expressions.
        order = arrange(self.flow)
        codes = [code for code, direction in order]
        kid = self.state.compile(self.flow.base,
                                  baseline=self.state.root)
        kid = self.state.inject(kid, codes)
        # Add the given flow to the routing table.
        routes = kid.routes.copy()
        for unit in spread(self.flow):
            routes[unit] = routes[unit.clone(flow=self.backbone)]
        # Generate an order term.
        return OrderTerm(self.state.tag(), kid, order,
                         self.flow.limit, self.flow.offset,
                         self.flow, kid.baseline, routes)


class InjectCode(Inject):

    adapts(Code)

    def __call__(self):
        # Inject all the units that compose the expression.
        return self.state.inject(self.term, self.expression.units)


class InjectUnit(Inject):

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

    adapts(ColumnUnit)

    def __call__(self):
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

    adapts(ScalarUnit)

    def __call__(self):
        # Injects a batch of scalar units sharing the same flow.

        # To inject a scalar unit into a term, we need to do the following:
        # - compile a term for the unit flow;
        # - inject the unit into the unit term;
        # - attach the unit term to the main term.

        # If we compile a unit term for each unit individually, we may
        # end up with a lot of identical unit terms in the term tree.
        # To optimize the structure of the term tree, the rewriter
        # collects all scalar units sharing the same flow and groups
        # them together so that the compiler could reuse the same term
        # for the whole group.

        # Check if the unit is already exported by the term.
        if self.unit in self.term.routes:
            return self.term

        # List of units to inject.  This includes the given unit itself
        # and the units suggested be injected together with it.
        units = [self.unit]
        for code in self.unit.companions:
            companion_unit = ScalarUnit(code, self.flow, code.binding)
            if companion_unit not in self.term.routes:
                units.append(companion_unit)

        # Verify that the unit is singular relative to the term.
        if not self.term.flow.spans(self.flow):
            raise CompileError("expected a singular expression",
                               self.unit.mark)
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


class InjectAggregate(Inject):

    adapts(AggregateUnit)

    def __init__(self, unit, term, state):
        super(InjectAggregate, self).__init__(unit, term, state)
        # Extract attributes of the unit.
        self.plural_flow = unit.plural_flow

    def __call__(self):
        # Injects a batch of aggregate units sharing the same plural
        # and unit flows.

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

        # Check if the unit is already exported by the term.
        if self.unit in self.term.routes:
            return self.term

        # When we inject many aggregates to the main term individually,
        # we may end up with a lot of identical subtrees in the final
        # term tree.  Therefore, the rewritter collects aggregates
        # sharing the same plural and unit flows and groups them together
        # so that the compiler could reuse the same term subtree for
        # the whole group.

        # Get the list of units to inject.
        units = [self.unit]
        for code in self.unit.companions:
            companion_unit = AggregateUnit(code, self.plural_flow,
                                           self.flow, code.binding)
            if companion_unit not in self.term.routes:
                units.append(companion_unit)

        # Verify that the units are singular relative to the term.
        if not self.term.flow.spans(self.flow):
            raise CompileError("expected a singular expression",
                               self.unit.mark)
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
            # FIXME: is it really so?
            unit_term = self.compile_shoot(self.flow, self.term)

        # Compile a term for the plural flow against the unit flow,
        # and inject all the aggregate expressions into it.
        plural_term = self.compile_shoot(self.plural_flow,
                                         unit_term, codes)
        # Generate joints to attach the projected term to the unit term.
        unit_joints = self.glue_terms(unit_term, plural_term)
        # Make sure the unit term could export the join conditions.
        unit_term = self.inject_joints(unit_term, unit_joints)

        # Now we are going to project the plural term onto the unit
        # flow.  As the projection basis, we are using the joints
        # generated by `glue_terms()`.  The flow corresponding to
        # the projection term is a quotient with the kernel formed
        # from the projection basis.
        basis = [runit for lunit, runit in unit_joints]

        # Determine the flow of the projected term (not necessarily
        # accurate, but we don't care).
        # FIXME: should the kernel of the quotient be `basis`?
        projected_flow = QuotientFlow(self.flow.inflate(),
                                      self.plural_flow, [],
                                      self.expression.binding)
        # The routing table of the projected term and join conditions
        # connecting the projected term to the unit term.
        tag = self.state.tag()
        joints = []
        routes = {}
        for joint in unit_joints:
            rop = KernelUnit(joint.rop, projected_flow, joint.rop.binding)
            routes[rop] = tag
            joints.append(joint.clone(rop=rop))

        # The term that computes aggregate expressions.
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


class InjectCorrelated(Inject):

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
        joints = self.glue_terms(unit_term, plural_term)
        # Make sure that the unit term could export tie conditions.
        unit_term = self.inject_joints(unit_term, joints)
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
        # Check if the unit is already exported by the term.
        if self.unit in self.term.routes:
            return self.term
        # Check if the unit is singular against the term flow.
        if not self.term.flow.spans(self.flow):
            raise CompileError("expected a singular expression",
                               self.unit.mark)
        # Inject the quotient space -- this should automatically
        # provide the unit.
        # FIXME: is it reachable?
        term = self.state.inject(self.term, [self.flow])
        # Verify that the unit is injected.
        assert self.unit in term.routes
        # Return an augmented term.
        return term


class InjectCovering(Inject):

    adapts(CoveringUnit)

    def __call__(self):
        # Check if the unit is already exported by the term.
        if self.unit in self.term.routes:
            return self.term
        # Ensure that the unit is singular against the term flow.
        if not self.term.flow.spans(self.flow):
            raise CompileError("expected a singular expression",
                               self.unit.mark)
        # FIXME: the rewritter should optimize the flow graph
        # so that this code is not reachable.
        # Add a hint to the flow node to ask the compiler generate
        # the unit when compiling the flow term.
        companions = self.flow.companions+[self.unit.code]
        flow = self.flow.clone(companions=companions)

        # In general, we can't inject the flow into the term
        # directly as we could hit the special case when the
        # flow is an ancestor of the term flow.  Instead, we
        # inject the flow manually.

        # Compile a shoot term for the flow.
        flow_term = self.compile_shoot(flow, self.term)
        # The routes to add.
        extra_routes = {}
        # Add native units of the injected flow in case someone
        # may need them later (but only do it if the trunk term
        # does not export them already).
        for unit in spread(flow):
            if unit not in self.term.routes:
                extra_routes[unit] = flow_term.routes[unit]
        # Add the route to the new unit.
        extra_routes[self.unit] = flow_term.routes[self.unit]
        # Join the shoot to the main term.
        term = self.join_terms(self.term, flow_term, extra_routes)
        # Verify that the unit is injected.
        assert self.unit in term.routes
        # Return the augmented term.
        return term


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


