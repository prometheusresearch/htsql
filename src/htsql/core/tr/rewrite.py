#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql.core.tr.rewrite`
============================

This module implements the rewriting process.
"""


from ..adapter import Utility, Adapter, adapt, adapt_many
from ..domain import BooleanDomain
from .error import EncodeError
from .coerce import coerce
from .flow import (Expression, QueryExpr, SegmentCode, Flow, RootFlow,
        FiberTableFlow, QuotientFlow, ComplementFlow, MonikerFlow, ForkedFlow,
        LinkedFlow, ClippedFlow, LocatorFlow, FilteredFlow, OrderedFlow, Code,
        LiteralCode, CastCode, RecordCode, IdentityCode, AnnihilatorCode,
        FormulaCode, Unit, ColumnUnit, CompoundUnit, ScalarUnit,
        AggregateUnitBase, AggregateUnit, KernelUnit, CoveringUnit)
from .signature import Signature, OrSig, AndSig
# FIXME: move `IfSig` and `SwitchSig` to `htsql.core.tr.signature`.
from .fn.signature import IfSig


class RewritingState(object):
    """
    Encapsulates the state of the rewriting process.

    State attributes:

    `root` (:class:`htsql.core.tr.flow.RootFlow`)
        The root data flow.

    `mask` (:class:`htsql.core.tr.flow.Flow`)
        The dominant flow; used to prune dependent flows on
        the *unmasking* phase.

    `collection` (list of :class:`htsql.core.tr.flow.Unit`)
        A list of units accumulated on the *collecting* phase.
    """

    def __init__(self):
        # The root flow.
        self.root = None
        # The current mask flow.
        self.mask = None
        # Stack of saved previous mask flows.
        self.mask_stack = []
        # List of collected units.
        self.collection = None
        # Dictionaries caching the results of `rewrite`, `unmask` and `replace`
        # phases.
        self.rewrite_cache = {}
        self.unmask_cache = {}
        self.replace_cache = {}

    def set_root(self, flow):
        """
        Set the root data flow.

        This function initializes the rewriting state.

        `root` (:class:`htsql.core.tr.flow.RootFlow`)
            The root flow.
        """
        assert isinstance(flow, RootFlow)
        # Check that it is not initialized already.
        assert self.root is None
        assert self.mask is None
        assert self.collection is None
        self.root = flow
        self.mask = flow
        self.collection = []

    def flush(self):
        """
        Clears the state.
        """
        assert self.root is not None
        assert self.mask is self.root
        assert not self.mask_stack
        self.root = None
        self.mask = None
        self.collection = None
        self.rewrite_cache = {}
        self.unmask_cache = {}
        self.replace_cache = {}

    def spawn(self):
        """
        Creates an empty copy of the state.
        """
        copy = RewritingState()
        copy.set_root(self.root)
        return copy

    def push_mask(self, mask):
        """
        Sets a new mask flow.

        `mask` (:class:`htsql.core.tr.flow.Flow`)
            A new mask flow.
        """
        assert isinstance(mask, Flow)
        self.mask_stack.append(self.mask)
        self.mask = mask

    def pop_mask(self):
        """
        Restores the previous mask flow.
        """
        self.mask = self.mask_stack.pop()

    def memorize(self, expression, replacement):
        """
        Memorizes a replacement node for the given expression node.

        `expression` (:class:`htsql.core.tr.flow.Expression`)
            The expression node to replace.

        `replacement` (:class:`htsql.core.tr.flow.Expression`)
            The replacement.
        """
        assert isinstance(expression, Expression)
        assert isinstance(replacement, Expression)
        assert expression not in self.replace_cache
        self.replace_cache[expression] = replacement

    def rewrite(self, expression):
        """
        Rewrites the given expression node.

        Returns an expression node semantically equivalent to the given node,
        but optimized for compilation.  May return the same node.

        `expression` (:class:`htsql.core.tr.flow.Expression`)
            The expression to rewrite.
        """
        # Check if the expression was already rewritten
        if expression in self.rewrite_cache:
            return self.rewrite_cache[expression]
        # Apply `Rewrite` adapter.
        replacement = rewrite(expression, self)
        # Cache the output.
        self.rewrite_cache[expression] = replacement
        return replacement

    def unmask(self, expression, mask=None):
        """
        Unmasks the given expression node.

        Unmasking prunes non-axial flow operations that are already
        enforced by the mask flow.

        `expression` (:class:`htsql.core.tr.flow.Expression`)
            The expression to unmask.

        `mask` (:class:`htsql.core.tr.flow.Flow` or ``None``)
            If set, specifies the mask to use; otherwise, the current
            mask is to be used.
        """
        # Set the new mask if provided.
        if mask is not None:
            self.push_mask(mask)
        # The result of the unmasking operation depends on both the expression
        # and the current mask, so they make a key in the cache.
        key = (self.mask, expression)
        # If the key is not in the cache, apply the `Unmask` adapter and store
        # the result in the cache.
        if key not in self.unmask_cache:
            replacement = Unmask.__invoke__(expression, self)
            self.unmask_cache[key] = replacement
        # Otherwise, fetch the result from the cache.
        else:
            replacement = self.unmask_cache[key]
        # Restore the current mask.
        if mask is not None:
            self.pop_mask()
        # Return the result of the operation.
        return replacement

    def collect(self, expression):
        """
        Collects scalar and aggregate units from the given expression.

        The collected units are stored in the state attribute
        :attr:`collection`.

        `expression` (:class:`htsql.core.tr.flow.Expression`)
            The expression to collect units from.
        """
        Collect.__invoke__(expression, self)

    def recombine(self):
        """
        Recombines scalar and aggregate units.

        This process adds compilation hints to facilitate merging
        similar scalar and aggregate units into shared SQL frames.

        Updated units are stored in the replace cache.
        """
        # Apply `Recombine` utility.
        Recombine.__invoke__(self)

    def replace(self, expression):
        """
        Replaces the given expression with a recombined clone.

        Returns a new expression node with scalar and aggregate units
        recombined.

        `expression` (:class:`htsql.core.tr.flow.Expression`)
            The expression to replace.
        """
        # Check if the expression is in the cache.
        if expression in self.replace_cache:
            return self.replace_cache[expression]
        # If not, apply the `Replace` adapter.
        replacement = Replace.__invoke__(expression, self)
        # Store the result in the cache and return it.
        self.replace_cache[expression] = replacement
        return replacement


class Recombine(Utility):
    """
    Recombines scalar and aggregate units.

    This utility adds compilation hints to collected scalar and aggregate
    units that help the compiler to use shared frames for similar units.

    `state` (:class:`RewritingState`)
        The current state of the rewriting process.
    """

    def __init__(self, state):
        assert isinstance(state, RewritingState)
        self.state = state

    def __call__(self):
        # Recombine scalar units.
        self.recombine_scalars()
        # Recombine aggregate units.
        self.recombine_aggregates()

    def recombine_scalars(self):
        # Recombines scalar units in the collection.

        # Duplicate unit nodes.
        duplicates = set()
        # List of unique flows of the units.
        flows = []
        # A mapping: flow -> units with this flow.
        flow_to_units = {}

        # Iterate over all collected units.
        for unit in self.state.collection:
            # We are only interested in scalar units.
            if not isinstance(unit, ScalarUnit):
                continue
            # Skip duplicates.
            if unit in duplicates:
                continue
            duplicates.add(unit)
            # If the unit flow is new, add it to the list of unique flows.
            flow = unit.flow
            if flow not in flow_to_units:
                flows.append(flow)
                flow_to_units[flow] = []
            # Store the unit.
            flow_to_units[flow].append(unit)

        # Iterate over all unique unit flows.
        for flow in flows:
            # Take all units with this flow.
            units = flow_to_units[flow]
            # Recombine the units.
            self.recombine_scalar_batch(flow, units)

    def recombine_aggregates(self):
        # Recombine aggregate units in the collection.

        # Duplicate unit nodes.
        duplicates = set()
        # Unique pairs of `(plural_flow, flow)` taken from aggregate units.
        flow_pairs = []
        # A mapping: (plural_flow, flow) -> associated aggregate units.
        flow_pair_to_units = {}
        # Note that we strip top filtering operations from the plural flow;
        # that's because aggregates which plural flows differ only by
        # filtering could still use a shared frame; so we need them in
        # the same batch.

        # Iterate over all collected units.
        for unit in self.state.collection:
            # We are only interested in aggregate units.
            if not isinstance(unit, AggregateUnit):
                continue
            # Skip duplicates.
            if unit in duplicates:
                continue
            duplicates.add(unit)
            # The base flow of the unit.
            flow = unit.flow
            # The flow of the unit argument.
            plural_flow = unit.plural_flow
            # Strip top filtering operations from the plural flow.
            while isinstance(plural_flow, FilteredFlow):
                plural_flow = plural_flow.base
            # The flow pair associated with the unit.
            pair = (plural_flow, flow)
            # Check if the flow pair is new.
            if pair not in flow_pair_to_units:
                flow_pairs.append(pair)
                flow_pair_to_units[pair] = []
            # Store the unit.
            flow_pair_to_units[pair].append(unit)

        # Iterate over all unique flow pairs.
        for pair in flow_pairs:
            plural_flow, flow = pair
            # Aggregates associated with the pair.
            units = flow_pair_to_units[pair]
            # Recombine the aggregates.
            self.recombine_aggregate_batch(plural_flow, flow, units)

    def recombine_scalar_batch(self, flow, units):
        # Recombines a batch of scalar units sharing the same unit flow.

        # Nothing to recombine if there are less than 2 units.
        if len(units) <= 1:
            return

        # Expressions associated with the units.
        codes = [unit.code for unit in units]
        # Recombine the unit flow and unit expressions against a blank state.
        substate = self.state.spawn()
        substate.collect(flow)
        for code in codes:
            substate.collect(code)
        substate.recombine()
        flow = substate.replace(flow)
        codes = [substate.replace(code) for code in codes]

        # Iterate over the units, generating a replacement for each.
        for idx, unit in enumerate(units):
            # New unit expression.
            code = codes[idx]
            # Expressions for companion units to be injected together with
            # the selected unit.
            companions = codes[:idx]+codes[idx+1:]
            # Generate and memorize the replacement.
            batch = unit.clone(code=code, flow=flow,
                               companions=companions)
            self.state.memorize(unit, batch)

    def recombine_aggregate_batch(self, plural_flow, flow, units):
        # Recombines a batch of aggregate units sharing the same
        # unit and operand flows.

        # This flag indicates that the units belong to a quotient
        # flow and the unit operands belong to the complement to
        # the quotient.  In this case, the aggregates could reuse
        # the frame that generates quotient flow.
        is_quotient = (isinstance(flow, QuotientFlow) and
                       isinstance(plural_flow, ComplementFlow) and
                       plural_flow.base == flow)

        # Nothing to recombine if we don't have at least two units.
        # However, continue in case when the aggregate could be
        # embedded into a quotient frame.
        if len(units) <= 1 and not is_quotient:
            return

        # The common base flow of all units.
        base_flow = flow

        # Plural flows of the units may differ from each other
        # since they may have extra filters attached to the common parent.
        # Here we find the longest common ancestor of all plural flows.

        # Candidate common ancestors, longest last.
        candidate_flows = []
        candidate_flow = units[0].plural_flow
        candidate_flows.append(candidate_flow)
        while isinstance(candidate_flow, FilteredFlow):
            candidate_flow = candidate_flow.base
            candidate_flows.append(candidate_flow)
        candidate_flows.reverse()
        # Iterate over the units reducing the number of common ancestors.
        for unit in units[1:]:
            # Ancestors of the selected unit, longest first.
            alternate_flows = []
            alternate_flow = unit.plural_flow
            alternate_flows.append(alternate_flow)
            while isinstance(alternate_flow, FilteredFlow):
                alternate_flow = alternate_flow.base
                alternate_flows.append(alternate_flow)
            alternate_flows.reverse()
            # Find the common prefix of `candidate_flows` and
            # `alternate_flows`.
            if len(alternate_flows) < len(candidate_flows):
                candidate_flows = candidate_flows[:len(alternate_flows)]
            for idx in range(len(candidate_flows)):
                if candidate_flows[idx] != alternate_flows[idx]:
                    assert idx > 0
                    candidate_flows = candidate_flows[:idx]
                    break
        # Take the longest of the common ancestors.
        shared_flow = candidate_flows[-1]
        # But when the aggregate is over a complement, ignore any shared
        # filter and take the axis flow instead; that's because in this case,
        # applying filters does not provide any performance benefits, but
        # prevents the units from being embedded into the quotient frame.
        if isinstance(plural_flow, ComplementFlow):
            shared_flow = plural_flow

        # Move non-shared filters from the operand flow to the operand, i.e.,
        #   unit(plural_flow{op}?filter) => unit(plural_flow{if(filter,op)})

        # Rewritten operands.
        codes = []
        # Non-shared filters, to be `OR`-ed and applied to the shared flow.
        filters = []
        # Iterate over the given aggregates.
        for unit in units:
            # The original operand of the aggregate.
            code = unit.code
            # A list of all non-shared filters on the unit operand flow.
            code_filters = []
            unit_flow = unit.plural_flow
            while unit_flow != shared_flow:
                code_filters.append(unit_flow.filter)
                unit_flow = unit_flow.base
            # If there are any filters, we need to rewrite the operand.
            if code_filters:
                # Merge all filters using `AND`.
                if len(code_filters) > 1:
                    code_filter = FormulaCode(AndSig(),
                                              coerce(BooleanDomain()),
                                              unit.flow.binding,
                                              ops=code_filters)
                else:
                    [code_filter] = code_filters
                # Add the filter to the list of all non-shared filters.
                filters.append(code_filter)
                # Rewrite the operand:
                #   op => if(filter,op)
                # FIXME: we assume `code` is a formula with an aggregate
                # signature, and that the aggregate ignores `NULL` values;
                # need a way to check this and abort if it's not true.
                op = code.op
                op = FormulaCode(IfSig(), op.domain, op.binding,
                                 predicates=[code_filter],
                                 consequents=[op],
                                 alternative=None)
                code = code.clone(op=op)
            # Add the (possibly) rewritten operand to the list.
            codes.append(code)

        # Check if we can apply the non-shared filters to the shared
        # flow.  Technically, it is not necessary, but may improve
        # performance in some cases.  So we can do it only if every
        # aggregate has some filter applied on top of the shared flow.
        # Also, we don't apply the filters on top of a complement flow
        # as it cannot improve the performace.
        if (not isinstance(shared_flow, ComplementFlow) and
                    all(unit.plural_flow != shared_flow for unit in units)):
            if len(filters) > 1:
                filter = FormulaCode(OrSig(), coerce(BooleanDomain()),
                                     shared_flow.binding, ops=filters)
            else:
                [filter] = filters
            shared_flow = FilteredFlow(shared_flow, filter,
                                       shared_flow.binding)

        # Now that the content of new units is generated, recombine
        # it against a blank state.
        substate = self.state.spawn()
        substate.collect(base_flow)
        substate.collect(shared_flow)
        for code in codes:
            substate.collect(code)
        substate.recombine()
        base_flow = substate.replace(base_flow)
        shared_flow = substate.replace(shared_flow)
        codes = [substate.replace(code) for code in codes]

        # Iterate over original units generating replacements.
        for idx, unit in enumerate(units):
            # The new unit expression and companions.
            code = codes[idx]
            # Generate and memorize the replacement.
            companions = codes[:idx]+codes[idx+1:]
            batch = unit.clone(code=code, plural_flow=shared_flow,
                               flow=base_flow, companions=companions)
            self.state.memorize(unit, batch)

        # The case when the aggregates could be embedded into the quotient
        # frame.
        if is_quotient:
            base_flow = base_flow.clone(companions=codes)
            self.state.memorize(flow, base_flow)


class RewriteBase(Adapter):
    """
    Applies the rewriting process to the given node.

    This is a base class for all rewriting adapters, it encapsulates
    common attributes and methods shared by all its subclasses.

    Most rewriting adapters have the following signature:

        Rewrite: (Expression, RewritingState) -> Expression

    The adapters are polymorphic on the first argument.

    `expression` (:class:`htsql.core.tr.flow.Expression`)
        The expression node to rewrite.

    `state` (:class:`RewritingState`)
        The current state of rewriting process.
    """

    adapt(Expression)

    def __init__(self, expression, state):
        assert isinstance(expression, Expression)
        assert isinstance(state, RewritingState)
        self.expression = expression
        self.state = state

    def __call__(self):
        # Must not be reachable.
        raise NotImplementedError("the rewrite adapter is not implemented"
                                  " for a %r node" % self.expression)


class Rewrite(RewriteBase):
    """
    Rewrites the given expression node.

    Returns an expression node semantically equivalent to the given node,
    but optimized for compilation.  May return the same node.
    """


class Unmask(RewriteBase):
    """
    Unmasks an expression node.

    Unmasking prunes non-axial flow nodes that are already enforced
    by the current mask flow.
    """


class Collect(RewriteBase):
    """
    Collects scalar and aggregate units in the given expression node.
    """


class Replace(RewriteBase):
    """
    Replaces the given expression with a recombined copy.
    """


class RewriteQuery(Rewrite):

    adapt(QueryExpr)

    def __call__(self):
        # Initialize the rewriting state.
        self.state.set_root(RootFlow(None, self.expression.binding))
        # Rewrite the segment, if present.
        segment = None
        if self.expression.segment is not None:
            segment = self.expression.segment
            # Rewrite: simplify expressions matching certain patterns.
            segment = self.state.rewrite(segment)
            # Unmask: eliminate redundant non-axial flow operations.
            segment = self.state.unmask(segment)
            # Collect: gather scalar and aggregate units.
            self.state.collect(segment)
            # Recombine: attach compilation hints to the collected units.
            self.state.recombine()
            # Replace: replace units with recombined copies.
            segment = self.state.replace(segment)
        # Clear the state.
        self.state.flush()
        # Clone the query node with a rewritten segment.
        return self.expression.clone(segment=segment)


class RewriteSegment(Rewrite):

    adapt(SegmentCode)

    def __call__(self):
        # Rewrite the output flow and output record.
        root = self.state.rewrite(self.expression.root)
        flow = self.state.rewrite(self.expression.flow)
        code = self.state.rewrite(self.expression.code)
        return self.expression.clone(root=root, flow=flow, code=code)


class UnmaskSegment(Unmask):

    adapt(SegmentCode)

    def __call__(self):
        # Unmask the output record against the output flow.
        code = self.state.unmask(self.expression.code,
                                 mask=self.expression.flow)
        # Unmask the flow itself.
        flow = self.state.unmask(self.expression.flow,
                                 mask=self.expression.root)
        root = self.state.unmask(self.expression.root)
        # Produce a clone of the segment with new flow and output columns.
        return self.expression.clone(root=root, flow=flow, code=code)


class CollectSegment(Collect):

    adapt(SegmentCode)

    def __call__(self):
        pass


class ReplaceSegment(Replace):

    adapt(SegmentCode)

    def __call__(self):
        # Recombine the content of the segment against a blank state.
        substate = self.state.spawn()
        substate.collect(self.expression.root)
        substate.collect(self.expression.flow)
        substate.collect(self.expression.code)
        substate.recombine()
        root = substate.replace(self.expression.root)
        flow = substate.replace(self.expression.flow)
        code = substate.replace(self.expression.code)
        return self.expression.clone(root=root, flow=flow, code=code)


class RewriteFlow(Rewrite):

    adapt(Flow)

    def __init__(self, flow, state):
        # Overriden to replace the attribute.
        super(RewriteFlow, self).__init__(flow, state)
        self.flow = flow

    def __call__(self):
        # No-op for the root flow.
        if self.flow.base is None:
            return self.flow
        # Otherwise, apply the adapter to the parent flow.
        base = self.state.rewrite(self.flow.base)
        return self.flow.clone(base=base)


class UnmaskFlow(Unmask):

    adapt(Flow)

    def __init__(self, flow, state):
        # Overriden to rename the attribute.
        super(UnmaskFlow, self).__init__(flow, state)
        self.flow = flow

    def __call__(self):
        # No-op for the root flow.
        if self.flow.base is None:
            return self.flow
        # Apply the adapter to the parent flow.
        base = self.state.unmask(self.flow.base)
        return self.flow.clone(base=base)


class CollectFlow(Collect):

    adapt(Flow)

    def __init__(self, flow, state):
        # Overriden to rename the attribute.
        super(CollectFlow, self).__init__(flow, state)
        self.flow = flow

    def __call__(self):
        # No-op for the root flow.
        if self.flow.base is None:
            return
        # Apply the adapter to the parent flow.
        self.state.collect(self.flow.base)


class ReplaceFlow(Replace):

    adapt(Flow)

    def __init__(self, flow, state):
        # Overriden to rename the attribute.
        super(ReplaceFlow, self).__init__(flow, state)
        self.flow = flow

    def __call__(self):
        # No-op for the root flow.
        if self.flow.base is None:
            return self.flow
        # Otherwise, replace the parent flow.
        base = self.state.replace(self.flow.base)
        return self.flow.clone(base=base)


class RewriteQuotient(RewriteFlow):

    adapt(QuotientFlow)

    def __call__(self):
        # Apply the adapter to all sub-nodes.
        base = self.state.rewrite(self.flow.base)
        seed = self.state.rewrite(self.flow.family.seed)
        kernels = [self.state.rewrite(code)
                   for code in self.flow.family.kernels]
        return self.flow.clone(base=base, seed=seed, kernels=kernels)


class UnmaskQuotient(UnmaskFlow):

    adapt(QuotientFlow)

    def __call__(self):
        # Unmask the kernel against the seed flow.
        kernels = [self.state.unmask(code, mask=self.flow.family.seed)
                   for code in self.flow.family.kernels]
        # Verify that the kernel is not scalar.  We can't do it earlier
        # because since unmasking may remove fantom units.
        if all(not code.units for code in kernels):
            raise EncodeError("an empty or constant kernel is not allowed",
                              self.flow.mark)
        # Unmask the seed against the quotient parent flow.
        seed = self.state.unmask(self.flow.family.seed, mask=self.flow.base)
        # Unmask the parent flow against the current mask.
        base = self.state.unmask(self.flow.base)
        return self.flow.clone(base=base, seed=seed, kernels=kernels)


class ReplaceQuotient(ReplaceFlow):

    adapt(QuotientFlow)

    def __call__(self):
        # Replace the parent flow.
        base = self.state.replace(self.flow.base)
        # Create a new empty state.
        substate = self.state.spawn()
        # Collect/recombine/replace units in the seed and kernel expressions
        # against a fresh state.
        substate.collect(self.flow.seed)
        for code in self.flow.kernels:
            substate.collect(code)
        substate.recombine()
        seed = substate.replace(self.flow.seed)
        kernels = [substate.replace(code)
                   for code in self.flow.kernels]
        # Produce a recombined node.
        return self.flow.clone(base=base, seed=seed, kernels=kernels)


class RewriteMoniker(RewriteFlow):

    adapt_many(MonikerFlow,
               ClippedFlow)

    def __call__(self):
        # Apply the adapter to all child nodes.
        base = self.state.rewrite(self.flow.base)
        seed = self.state.rewrite(self.flow.seed)
        return self.flow.clone(base=base, seed=seed)


class UnmaskMoniker(UnmaskFlow):

    adapt_many(MonikerFlow,
               ClippedFlow)

    def __call__(self):
        # Unmask the seed flow against the parent flow.
        seed = self.state.unmask(self.flow.seed, mask=self.flow.base)
        # Unmask the parent flow against the current mask.
        base = self.state.unmask(self.flow.base)
        return self.flow.clone(base=base, seed=seed)


class ReplaceMoniker(Replace):

    adapt_many(MonikerFlow,
               ClippedFlow)

    def __call__(self):
        # Replace the parent flow.
        base = self.state.replace(self.flow.base)
        # Recombine the seed flow against a fresh state.
        substate = self.state.spawn()
        substate.collect(self.flow.seed)
        substate.recombine()
        seed = substate.replace(self.flow.seed)
        # Produce a recombined flow node.
        return self.flow.clone(base=base, seed=seed)


class RewriteLocator(RewriteFlow):

    adapt(LocatorFlow)

    def __call__(self):
        #if self.flow.base.dominates(self.flow.seed):
        #    flow = FilteredFlow(self.flow.seed, self.flow.filter,
        #                        self.flow.binding)
        #    return self.state.rewrite(flow)
        base = self.state.rewrite(self.flow.base)
        seed = self.state.rewrite(self.flow.seed)
        filter = self.state.rewrite(self.flow.filter)
        return self.flow.clone(base=base, seed=seed, filter=filter)


class UnmaskLocator(UnmaskFlow):

    adapt(LocatorFlow)

    def __call__(self):
        # Unmask the seed flow against the parent flow.
        seed = self.state.unmask(self.flow.seed, mask=self.flow.base)
        # Unmask the parent flow against the current mask.
        base = self.state.unmask(self.flow.base)
        filter = self.state.unmask(self.flow.filter, mask=self.flow.seed)
        return self.flow.clone(base=base, seed=seed, filter=filter)


class ReplaceLocator(Replace):

    adapt(LocatorFlow)

    def __call__(self):
        base = self.state.replace(self.flow.base)
        substate = self.state.spawn()
        substate.collect(self.flow.seed)
        substate.collect(self.flow.filter)
        substate.recombine()
        seed = substate.replace(self.flow.seed)
        filter = substate.replace(self.flow.filter)
        return self.flow.clone(base=base, seed=seed, filter=filter)


class RewriteForked(RewriteFlow):

    adapt(ForkedFlow)

    def __call__(self):
        # Apply the adapter to all child nodes.
        base = self.state.rewrite(self.flow.base)
        seed = self.state.rewrite(self.flow.seed)
        kernels = [self.state.rewrite(code)
                   for code in self.flow.kernels]
        return self.flow.clone(base=base, seed=seed, kernels=kernels)


class UnmaskForked(UnmaskFlow):

    adapt(ForkedFlow)

    def __call__(self):
        # Prune all but trailing non-axial operations from the seed flow.
        seed = self.state.unmask(self.flow.seed, mask=self.flow.ground)
        # Unmask the kernel against the parent flow.
        kernels = [self.state.unmask(code, mask=self.flow.base)
                   for code in self.flow.kernels]
        # Unmask the parent flow.
        base = self.state.unmask(self.flow.base)
        return self.flow.clone(base=base, seed=seed, kernels=kernels)


class CollectForked(Collect):

    adapt(ForkedFlow)

    def __call__(self):
        # Collect units in the parent flow.
        self.state.collect(self.flow.base)
        # Ignore the seed flow as it is a duplicate of the parent flow,
        # but process through the kernel.
        # FIXME: do we need to process the kernel?
        for code in self.flow.kernels:
            self.state.collect(code)


class ReplaceForked(Replace):

    adapt(ForkedFlow)

    def __call__(self):
        # Replace the parent flow.
        base = self.state.replace(self.flow.base)
        # Replace the kernel.
        # FIXME: where to replace the kernel?  Perhaps store two copies
        # of the kernel?
        kernels = [self.state.replace(code) for code in self.flow.kernels]
        # Recombine the seed flow against a fresh state.
        substate = self.state.spawn()
        substate.collect(self.flow.seed)
        substate.recombine()
        seed = substate.replace(self.flow.seed)
        # Produce a recombined node.
        return self.flow.clone(base=base, seed=seed, kernels=kernels)


class RewriteLinked(RewriteFlow):

    adapt(LinkedFlow)

    def __call__(self):
        # Rewrite the child nodes.
        base = self.state.rewrite(self.flow.base)
        seed = self.state.rewrite(self.flow.seed)
        images = [(self.state.rewrite(lcode), self.state.rewrite(rcode))
                  for lcode, rcode in self.flow.images]
        return self.flow.clone(base=base, seed=seed, images=images)


class UnmaskLinked(UnmaskFlow):

    adapt(LinkedFlow)

    def __call__(self):
        # Unmask the parent flow.
        base = self.state.unmask(self.flow.base)
        # Unmask the seed flow against the parent.
        seed = self.state.unmask(self.flow.seed, mask=self.flow.base)
        # Unmask the parent image against the parent flow and the seed
        # image against the seed flow.
        images = [(self.state.unmask(lcode, mask=self.flow.base),
                   self.state.unmask(rcode, mask=self.flow.seed))
                  for lcode, rcode in self.flow.images]
        return self.flow.clone(base=base, seed=seed, images=images)


class CollectLinked(Collect):

    adapt(LinkedFlow)

    def __call__(self):
        # Gather units in the parent flow and the parent images.
        self.state.collect(self.flow.base)
        for lcode, rcode in self.flow.images:
            self.state.collect(lcode)


class ReplaceLinked(Replace):

    adapt(LinkedFlow)

    def __call__(self):
        # Replace the parent flow and parental images.
        base = self.state.replace(self.flow.base)
        images = [(self.state.replace(lcode), rcode)
                  for lcode, rcode in self.flow.images]
        # Recombine the seed flow and seed images against a fresh state.
        substate = self.state.spawn()
        substate.collect(self.flow.seed)
        for lcode, rcode in images:
            substate.collect(rcode)
        substate.recombine()
        seed = substate.replace(self.flow.seed)
        images = [(lcode, substate.replace(rcode))
                  for lcode, rcode in images]
        return self.flow.clone(base=base, seed=seed, images=images)


class RewriteFiltered(RewriteFlow):

    adapt(FilteredFlow)

    def __call__(self):
        # Rewrite the parent flow and the filter expression.
        base = self.state.rewrite(self.flow.base)
        filter = self.state.rewrite(self.flow.filter)
        # Eliminate a `?true` filter.
        if (isinstance(filter, LiteralCode) and
            isinstance(filter.domain, BooleanDomain) and
            filter.value is True):
            return base
        return self.flow.clone(base=base, filter=filter)


class UnmaskFiltered(UnmaskFlow):

    adapt(FilteredFlow)

    def __call__(self):
        # If the filter is already enforced by the mask,
        # remove the filter, return an unmasked parent flow.
        if (self.flow.prune(self.state.mask)
                == self.flow.base.prune(self.state.mask)):
            return self.state.unmask(self.flow.base)
        # Choose the mask to use for unmasking the filter.  Use the parent
        # flow unless it dominates the current mask (that is, the mask
        # contains all non-axial operations of the parent flow),
        if self.flow.base.dominates(self.state.mask):
            filter = self.state.unmask(self.flow.filter)
        else:
            filter = self.state.unmask(self.flow.filter,
                                       mask=self.flow.base)
        # Unmask the parent flow against the current mask.
        base = self.state.unmask(self.flow.base)
        return self.flow.clone(base=base, filter=filter)


class CollectFiltered(Collect):

    adapt(FilteredFlow)

    def __call__(self):
        # Collect units in all child nodes.
        self.state.collect(self.flow.base)
        self.state.collect(self.flow.filter)


class ReplaceFiltered(Replace):

    adapt(FilteredFlow)

    def __call__(self):
        # Replace all child nodes.
        base = self.state.replace(self.flow.base)
        filter = self.state.replace(self.flow.filter)
        return self.flow.clone(base=base, filter=filter)


class RewriteOrdered(RewriteFlow):

    adapt(OrderedFlow)

    def __call__(self):
        # Rewrite child nodes.
        base = self.state.rewrite(self.flow.base)
        order = [(self.state.rewrite(code), direction)
                 for code, direction in self.flow.order]
        return self.flow.clone(base=base, order=order)


class UnmaskOrdered(UnmaskFlow):

    adapt(OrderedFlow)

    def __call__(self):
        # If the ordering operation is already enforced by the mask,
        # return the parent flow.
        if (self.flow.prune(self.state.mask)
                == self.flow.base.prune(self.state.mask)):
            return self.state.unmask(self.flow.base)
        # Choose a mask for order expressions; use the parent flow
        # unless it dominates the current mask, in which case use
        # the current mask.
        if self.flow.base.dominates(self.state.mask):
            order = [(self.state.unmask(code), direction)
                     for code, direction in self.flow.order]
        else:
            order = [(self.state.unmask(code, mask=self.flow.base),
                      direction)
                     for code, direction in self.flow.order]
        # Unmask the parent flow, but only if `limit` and `offset` are
        # not specified.
        if self.flow.is_expanding:
            base = self.state.unmask(self.flow.base)
        else:
            base = self.state.unmask(self.flow.base, mask=self.state.root)
        return self.flow.clone(base=base, order=order)


class CollectOrdered(Collect):

    adapt(OrderedFlow)

    def __call__(self):
        # Collect units in all child nodes.
        self.state.collect(self.flow.base)
        for code, direction in self.flow.order:
            self.state.collect(code)


class ReplaceOrdered(Replace):

    adapt(OrderedFlow)

    def __call__(self):
        # Replace units in all child nodes.
        base = self.state.replace(self.flow.base)
        order = [(self.state.replace(code), direction)
                 for code, direction in self.flow.order]
        return self.flow.clone(base=base, order=order)


class RewriteCode(Rewrite):

    adapt(Code)

    def __init__(self, code, state):
        # Override to change the attribute name.
        super(RewriteCode, self).__init__(code, state)
        self.code = code

    def __call__(self):
        # The default implementation is no-op; override in subclasses
        # if necessary.
        return self.code


class UnmaskCode(Unmask):

    adapt(Code)

    def __init__(self, code, state):
        # Override to change the attribute name.
        super(UnmaskCode, self).__init__(code, state)
        self.code = code

    def __call__(self):
        # The default implementation is no-op; override in subclasses
        # if necessary.
        return self.code


class CollectCode(Collect):

    adapt(Code)

    def __init__(self, code, state):
        # Override to change the attribute name.
        super(CollectCode, self).__init__(code, state)
        self.code = code

    def __call__(self):
        # Collect all units in the node.
        for unit in self.code.units:
            self.state.collect(unit)


class ReplaceCode(Replace):

    adapt(Code)

    def __init__(self, code, state):
        # Override to change the attribute name.
        super(ReplaceCode, self).__init__(code, state)
        self.code = code

    def __call__(self):
        # The default implementation is no-op; should be changed
        # in subclasses.
        return self.code


class RewriteCast(RewriteCode):

    adapt(CastCode)

    def __call__(self):
        # Rewrite the operand of the cast.
        base = self.state.rewrite(self.code.base)
        return self.code.clone(base=base)


class UnmaskCast(UnmaskCode):

    adapt(CastCode)

    def __call__(self):
        # Unmask the operand of the cast.
        base = self.state.unmask(self.code.base)
        return self.code.clone(base=base)


class ReplaceCast(ReplaceCode):

    adapt(CastCode)

    def __call__(self):
        # Replace units in the operand of the cast.
        base = self.state.replace(self.code.base)
        return self.code.clone(base=base)


class RewriteFormula(RewriteCode):

    adapt(FormulaCode)

    def __call__(self):
        # Delegate to an auxiliary adapter dispatched by the formula signature.
        return RewriteBySignature.__invoke__(self.code, self.state)


class UnmaskFormula(UnmaskCode):

    adapt(FormulaCode)

    def __call__(self):
        # Unmask formula arguments.
        arguments = self.code.arguments.map(self.state.unmask)
        return FormulaCode(self.code.signature, self.code.domain,
                           self.code.binding, **arguments)


class ReplaceFormula(ReplaceCode):

    adapt(FormulaCode)

    def __call__(self):
        # Replace units in the formula arguments.
        arguments = self.code.arguments.map(self.state.replace)
        return FormulaCode(self.code.signature, self.code.domain,
                           self.code.binding, **arguments)


class RewriteBySignature(Adapter):
    """
    Rewrites a formula node.

    This is an auxiliary interface used by :class:`Rewrite` adapter.
    It is polymorphic on the signature of the formula.

    `code` (:class:`htsql.core.tr.flow.FormulaCode`)
        The formula node to rewrite.

    `state` (:class:`RewritingState`)
        The current state of rewrite process.
    """

    adapt(Signature)

    @classmethod
    def __dispatch__(interface, code, *args, **kwds):
        # Extract the dispatch key from the arguments.
        assert isinstance(code, FormulaCode)
        return (type(code.signature),)

    def __init__(self, code, state):
        assert isinstance(code, FormulaCode)
        assert isinstance(state, RewritingState)
        self.code = code
        self.state = state
        # Extract commonly used attributes of the formula node.
        self.signature = code.signature
        self.domain = code.domain
        self.arguments = code.arguments

    def __call__(self):
        # The default implementation rewrites the arguments of the formula.
        # Override in subclasses to provide specific optimizations.
        arguments = self.arguments.map(self.state.rewrite)
        return FormulaCode(self.signature, self.domain,
                           self.code.binding, **arguments)


class RewriteRecord(Rewrite):

    adapt_many(RecordCode,
               IdentityCode)

    def __call__(self):
        fields = [self.state.rewrite(field)
                  for field in self.code.fields]
        return self.code.clone(fields=fields)


class UnmaskRecord(Unmask):

    adapt_many(RecordCode,
               IdentityCode)

    def __call__(self):
        fields = [self.state.unmask(field)
                  for field in self.code.fields]
        return self.code.clone(fields=fields)


class CollectRecord(Collect):

    adapt_many(RecordCode,
               IdentityCode)

    def __call__(self):
        for field in self.code.fields:
            self.state.collect(field)


class ReplaceRecord(Replace):

    adapt_many(RecordCode,
               IdentityCode)

    def __call__(self):
        fields = [self.state.replace(field)
                  for field in self.code.fields]
        return self.code.clone(fields=fields)


class RewriteAnnihilator(Rewrite):

    adapt(AnnihilatorCode)

    def __call__(self):
        code = self.state.rewrite(self.code.code)
        indicator = self.state.rewrite(self.code.indicator)
        return self.code.clone(code=code, indicator=indicator)


class UnmaskAnnihilator(Unmask):

    adapt(AnnihilatorCode)

    def __call__(self):
        code = self.state.unmask(self.code.code)
        indicator = self.state.unmask(self.code.indicator)
        if not isinstance(indicator, Unit):
            return code
        return self.code.clone(code=code, indicator=indicator)


class CollectAnnihilator(Collect):

    adapt(AnnihilatorCode)

    def __call__(self):
        self.state.collect(self.code.code)
        self.state.collect(self.code.indicator)


class ReplaceAnnihilator(Replace):

    adapt(AnnihilatorCode)

    def __call__(self):
        code = self.state.replace(self.code.code)
        indicator = self.state.replace(self.code.indicator)
        return self.code.clone(code=code, indicator=indicator)


class RewriteUnit(RewriteCode):

    adapt(Unit)

    def __init__(self, unit, state):
        # Overriden to rename the attribute.
        super(RewriteUnit, self).__init__(unit, state)
        self.unit = unit

    def __call__(self):
        # Apply the adapter to child nodes.
        flow = self.state.rewrite(self.unit.flow)
        return self.unit.clone(flow=flow)


class UnmaskUnit(UnmaskCode):

    adapt(Unit)

    def __init__(self, unit, state):
        # Overriden to rename the attribute.
        super(UnmaskUnit, self).__init__(unit, state)
        self.unit = unit

    def __call__(self):
        # Apply the adapter to child nodes.
        flow = self.state.unmask(self.unit.flow)
        return self.unit.clone(flow=flow)


class CollectUnit(CollectCode):

    adapt(Unit)

    def __init__(self, unit, state):
        # Overriden to rename the attribute.
        super(CollectUnit, self).__init__(unit, state)
        self.unit = unit

    def __call__(self):
        # Add the unit to the collection.  Note that we do not
        # go to the child nodes of the unit, it is done against
        # a blank rewriting state in the `Replace` implementation.
        self.state.collection.append(self.unit)


class ReplaceUnit(ReplaceCode):

    adapt(Unit)

    def __init__(self, unit, state):
        # Overriden to rename the attribute.
        super(ReplaceUnit, self).__init__(unit, state)
        self.unit = unit

    def __call__(self):
        # Recombine the content of the unit node against a blank state.
        substate = self.state.spawn()
        substate.collect(self.unit.flow)
        substate.recombine()
        flow = substate.replace(self.unit.flow)
        return self.unit.clone(flow=flow)


class UnmaskColumn(UnmaskUnit):

    adapt(ColumnUnit)

    def __call__(self):
        flow = self.state.unmask(self.unit.flow)
        column = self.unit.column
        while (isinstance(flow, FiberTableFlow) and flow.join.is_direct and
               flow.is_expanding and flow.is_contracting):
            for origin_column, target_column in zip(flow.join.origin_columns,
                                                    flow.join.target_columns):
                if column is target_column:
                    flow = flow.base
                    column = origin_column
                    break
            else:
                break
        return self.unit.clone(flow=flow, column=column)


class RewriteCompound(RewriteUnit):

    adapt(CompoundUnit)

    def __call__(self):
        # Rewrite the content of the node.
        code = self.state.rewrite(self.unit.code)
        flow = self.state.rewrite(self.unit.flow)
        return self.unit.clone(code=code, flow=flow)


class ReplaceCompound(ReplaceUnit):

    adapt(CompoundUnit)

    def __call__(self):
        # Recombine the content of the unit node against a blank state.
        substate = self.state.spawn()
        substate.collect(self.unit.code)
        substate.collect(self.unit.flow)
        substate.recombine()
        code = substate.replace(self.unit.code)
        flow = substate.replace(self.unit.flow)
        return self.unit.clone(code=code, flow=flow)


class UnmaskScalar(UnmaskUnit):

    adapt(ScalarUnit)

    def __call__(self):
        # The unit is redundant if the mask is dominated by the unit flow.
        if self.unit.flow.dominates(self.state.mask):
            code = self.state.unmask(self.unit.code)
            return code
        # It is also redundant if the operand is a unit under the same
        # or a dominated flow.
        if (isinstance(self.unit.code, Unit) and
                self.unit.flow.dominates(self.unit.code.flow)):
            code = self.state.unmask(self.unit.code)
            return code
        # Unmask the unit expression against the unit flow.
        code = self.state.unmask(self.unit.code, mask=self.unit.flow)
        # Unmask the unit flow against the current mask.
        flow = self.state.unmask(self.unit.flow)
        return self.unit.clone(code=code, flow=flow)


class RewriteAggregate(RewriteUnit):

    adapt(AggregateUnitBase)

    def __call__(self):
        # Rewrite the content of the node.
        code = self.state.rewrite(self.unit.code)
        plural_flow = self.state.rewrite(self.unit.plural_flow)
        flow = self.state.rewrite(self.unit.flow)
        return self.unit.clone(code=code, plural_flow=plural_flow, flow=flow)


class UnmaskAggregate(UnmaskUnit):

    adapt(AggregateUnitBase)

    def __call__(self):
        # Unmask the argument against the plural flow.
        code = self.state.unmask(self.unit.code, mask=self.unit.plural_flow)
        # Unmask the plural flow against the unit flow unless it dominates
        # the current mask.
        if self.unit.flow.dominates(self.state.mask):
            plural_flow = self.state.unmask(self.unit.plural_flow)
        else:
            plural_flow = self.state.unmask(self.unit.plural_flow,
                                            mask=self.unit.flow)
        # Unmask the unit flow against the current mask.
        flow = self.state.unmask(self.unit.flow)
        return self.unit.clone(code=code, plural_flow=plural_flow, flow=flow)


class ReplaceAggregate(ReplaceUnit):

    adapt(AggregateUnitBase)

    def __call__(self):
        # Recombine the content of the unit node against a blank state.
        substate = self.state.spawn()
        substate.collect(self.unit.code)
        substate.collect(self.unit.plural_flow)
        substate.collect(self.unit.flow)
        substate.recombine()
        code = substate.replace(self.unit.code)
        plural_flow = substate.replace(self.unit.plural_flow)
        flow = substate.replace(self.unit.flow)
        return self.unit.clone(code=code, plural_flow=plural_flow, flow=flow)


class RewriteKernel(RewriteUnit):

    adapt(KernelUnit)

    def __call__(self):
        # At this stage, the kernel code is an element of the family kernel.
        assert self.unit.code in self.unit.flow.family.kernels
        index = self.unit.flow.family.kernels.index(self.unit.code)
        # Rewrite the quotient flow.
        flow = self.state.rewrite(self.unit.flow)
        # Get the new kernel code.
        code = flow.family.kernels[index]
        return self.unit.clone(code=code, flow=flow)


class UnmaskKernel(UnmaskUnit):

    adapt(KernelUnit)

    def __call__(self):
        # At this stage, the kernel code is an element of the family kernel.
        assert self.unit.code in self.unit.flow.family.kernels
        index = self.unit.flow.family.kernels.index(self.unit.code)
        # Unmask the quotient flow.
        flow = self.state.unmask(self.unit.flow)
        # Get the new kernel code.
        code = flow.family.kernels[index]
        return self.unit.clone(code=code, flow=flow)


class ReplaceKernel(ReplaceUnit):

    adapt(KernelUnit)

    def __call__(self):
        # At this stage, the kernel code is an element of the family kernel.
        assert self.unit.code in self.unit.flow.family.kernels
        index = self.unit.flow.family.kernels.index(self.unit.code)
        # Recombine the quotient flow.
        substate = self.state.spawn()
        substate.collect(self.unit.flow)
        substate.recombine()
        flow = substate.replace(self.unit.flow)
        # Get the new kernel code.
        code = flow.family.kernels[index]
        return self.unit.clone(code=code, flow=flow)


class UnmaskCovering(UnmaskUnit):
    # FIXME: not used?

    adapt(CoveringUnit)

    def __call__(self):
        # The unit expression is evaluated against the seed flow
        # of the unit flow, so use the seed as the mask.
        code = self.state.unmask(self.unit.code,
                                 mask=self.unit.flow.seed)
        # Unmask the unit flow.
        flow = self.state.unmask(self.unit.flow)
        return self.unit.clone(code=code, flow=flow)


def rewrite(expression, state=None):
    """
    Rewrites the given expression node.

    Returns a clone of the given node optimized for compilation.

    `expression` (:class:`htsql.core.tr.flow.Expression`)
        The expression node to rewrite.

    `state` (:class:`RewritingState` or ``None``)
        The rewriting state to use.  If not set, a new rewriting state
        is created.
    """
    # If a state is not provided, create a new one.
    if state is None:
        state = RewritingState()
    # Apply the `Rewrite` adapter.
    return Rewrite.__invoke__(expression, state)


