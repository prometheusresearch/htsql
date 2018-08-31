#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ..adapter import Utility, Adapter, adapt, adapt_many
from ..domain import BooleanDomain
from ..error import Error, translate_guard
from .coerce import coerce
from .space import (Expression, SegmentExpr, Space, RootSpace, FiberTableSpace,
        QuotientSpace, ComplementSpace, MonikerSpace, ForkedSpace, AttachSpace,
        ClippedSpace, LocatorSpace, FilteredSpace, OrderedSpace, Code,
        LiteralCode, CastCode, FormulaCode, Unit, ColumnUnit, CompoundUnit,
        ScalarUnit, AggregateUnitBase, AggregateUnit, KernelUnit, CoveringUnit)
from .signature import Signature, OrSig, AndSig, IsEqualSig, isformula
# FIXME: move `IfSig` and `SwitchSig` to `htsql.core.tr.signature`.
from .fn.signature import IfSig


class RewritingState:
    """
    Encapsulates the state of the rewriting process.

    State attributes:

    `root` (:class:`htsql.core.tr.space.RootSpace`)
        The root data space.

    `mask` (:class:`htsql.core.tr.space.Space`)
        The dominant space; used to prune dependent spaces on
        the *unmasking* phase.

    `collection` (list of :class:`htsql.core.tr.space.Unit`)
        A list of units accumulated on the *collecting* phase.
    """

    def __init__(self, root):
        # The root space.
        self.root = root
        # The current mask space.
        self.mask = root
        # Stack of saved previous mask spaces.
        self.mask_stack = []
        # List of collected units.
        self.collection = []
        # Dictionaries caching the results of `rewrite`, `unmask` and `replace`
        # phases.
        self.rewrite_cache = {}
        self.unmask_cache = {}
        self.replace_cache = {}

    def spawn(self):
        """
        Creates an empty copy of the state.
        """
        return RewritingState(self.root)

    def push_mask(self, mask):
        """
        Sets a new mask space.

        `mask` (:class:`htsql.core.tr.space.Space`)
            A new mask space.
        """
        assert isinstance(mask, Space)
        self.mask_stack.append(self.mask)
        self.mask = mask

    def pop_mask(self):
        """
        Restores the previous mask space.
        """
        self.mask = self.mask_stack.pop()

    def memorize(self, expression, replacement):
        """
        Memorizes a replacement node for the given expression node.

        `expression` (:class:`htsql.core.tr.space.Expression`)
            The expression node to replace.

        `replacement` (:class:`htsql.core.tr.space.Expression`)
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

        `expression` (:class:`htsql.core.tr.space.Expression`)
            The expression to rewrite.
        """
        # Check if the expression was already rewritten
        if expression in self.rewrite_cache:
            return self.rewrite_cache[expression]
        # Apply `Rewrite` adapter.
        with translate_guard(expression):
            replacement = Rewrite.__prepare__(expression, self)()
        # Cache the output.
        self.rewrite_cache[expression] = replacement
        return replacement

    def unmask(self, expression, mask=None):
        """
        Unmasks the given expression node.

        Unmasking prunes non-axial space operations that are already
        enforced by the mask space.

        `expression` (:class:`htsql.core.tr.space.Expression`)
            The expression to unmask.

        `mask` (:class:`htsql.core.tr.space.Space` or ``None``)
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
            with translate_guard(expression):
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

        `expression` (:class:`htsql.core.tr.space.Expression`)
            The expression to collect units from.
        """
        with translate_guard(expression):
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

        `expression` (:class:`htsql.core.tr.space.Expression`)
            The expression to replace.
        """
        # Check if the expression is in the cache.
        if expression in self.replace_cache:
            return self.replace_cache[expression]
        # If not, apply the `Replace` adapter.
        with translate_guard(expression):
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
        #assert isinstance(state, RewritingState)
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
        # List of unique spaces of the units.
        spaces = []
        # A mapping: space -> units with this space.
        space_to_units = {}

        # Iterate over all collected units.
        for unit in self.state.collection:
            # We are only interested in scalar units.
            if not isinstance(unit, ScalarUnit):
                continue
            # Skip duplicates.
            if unit in duplicates:
                continue
            duplicates.add(unit)
            # If the unit space is new, add it to the list of unique spaces.
            space = unit.space
            if space not in space_to_units:
                spaces.append(space)
                space_to_units[space] = []
            # Store the unit.
            space_to_units[space].append(unit)

        # Iterate over all unique unit spaces.
        for space in spaces:
            # Take all units with this space.
            units = space_to_units[space]
            # Recombine the units.
            self.recombine_scalar_batch(space, units)

    def recombine_aggregates(self):
        # Recombine aggregate units in the collection.

        # Duplicate unit nodes.
        duplicates = set()
        # Unique pairs of `(plural_space, space)` taken from aggregate units.
        space_pairs = []
        # A mapping: (plural_space, space) -> associated aggregate units.
        space_pair_to_units = {}
        # Note that we strip top filtering operations from the plural space;
        # that's because aggregates which plural spaces differ only by
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
            # The base space of the unit.
            space = unit.space
            # The space of the unit argument.
            plural_space = unit.plural_space
            # Strip top filtering operations from the plural space.
            while isinstance(plural_space, FilteredSpace):
                plural_space = plural_space.base
            # The space pair associated with the unit.
            pair = (plural_space, space)
            # Check if the space pair is new.
            if pair not in space_pair_to_units:
                space_pairs.append(pair)
                space_pair_to_units[pair] = []
            # Store the unit.
            space_pair_to_units[pair].append(unit)

        # Iterate over all unique space pairs.
        for pair in space_pairs:
            plural_space, space = pair
            # Aggregates associated with the pair.
            units = space_pair_to_units[pair]
            # Recombine the aggregates.
            self.recombine_aggregate_batch(plural_space, space, units)

    def recombine_scalar_batch(self, space, units):
        # Recombines a batch of scalar units sharing the same unit space.

        # Nothing to recombine if there are less than 2 units.
        if len(units) <= 1:
            return

        # Expressions associated with the units.
        codes = [unit.code for unit in units]
        # Recombine the unit space and unit expressions against a blank state.
        substate = self.state.spawn()
        substate.collect(space)
        for code in codes:
            substate.collect(code)
        substate.recombine()
        space = substate.replace(space)
        codes = [substate.replace(code) for code in codes]

        # Iterate over the units, generating a replacement for each.
        for idx, unit in enumerate(units):
            # New unit expression.
            code = codes[idx]
            # Expressions for companion units to be injected together with
            # the selected unit.
            companions = codes[:idx]+codes[idx+1:]
            # Generate and memorize the replacement.
            batch = unit.clone(code=code, space=space,
                               companions=companions)
            self.state.memorize(unit, batch)

    def recombine_aggregate_batch(self, plural_space, space, units):
        # Recombines a batch of aggregate units sharing the same
        # unit and operand spaces.

        # This flag indicates that the units belong to a quotient
        # space and the unit operands belong to the complement to
        # the quotient.  In this case, the aggregates could reuse
        # the frame that generates quotient space.
        is_quotient = (isinstance(space, QuotientSpace) and
                       isinstance(plural_space, ComplementSpace) and
                       plural_space.base == space)

        # Nothing to recombine if we don't have at least two units.
        # However, continue in case when the aggregate could be
        # embedded into a quotient frame.
        if len(units) <= 1 and not is_quotient:
            return

        # The common base space of all units.
        base_space = space

        # Plural spaces of the units may differ from each other
        # since they may have extra filters attached to the common parent.
        # Here we find the longest common ancestor of all plural spaces.

        # Candidate common ancestors, longest last.
        candidate_spaces = []
        candidate_space = units[0].plural_space
        candidate_spaces.append(candidate_space)
        while isinstance(candidate_space, FilteredSpace):
            candidate_space = candidate_space.base
            candidate_spaces.append(candidate_space)
        candidate_spaces.reverse()
        # Iterate over the units reducing the number of common ancestors.
        for unit in units[1:]:
            # Ancestors of the selected unit, longest first.
            alternate_spaces = []
            alternate_space = unit.plural_space
            alternate_spaces.append(alternate_space)
            while isinstance(alternate_space, FilteredSpace):
                alternate_space = alternate_space.base
                alternate_spaces.append(alternate_space)
            alternate_spaces.reverse()
            # Find the common prefix of `candidate_spaces` and
            # `alternate_spaces`.
            if len(alternate_spaces) < len(candidate_spaces):
                candidate_spaces = candidate_spaces[:len(alternate_spaces)]
            for idx in range(len(candidate_spaces)):
                if candidate_spaces[idx] != alternate_spaces[idx]:
                    assert idx > 0
                    candidate_spaces = candidate_spaces[:idx]
                    break
        # Take the longest of the common ancestors.
        shared_space = candidate_spaces[-1]
        # But when the aggregate is over a complement, ignore any shared
        # filter and take the axis space instead; that's because in this case,
        # applying filters does not provide any performance benefits, but
        # prevents the units from being embedded into the quotient frame.
        if isinstance(plural_space, ComplementSpace):
            shared_space = plural_space

        # Move non-shared filters from the operand space to the operand, i.e.,
        #   unit(plural_space{op}?filter) => unit(plural_space{if(filter,op)})

        # Rewritten operands.
        codes = []
        # Non-shared filters, to be `OR`-ed and applied to the shared space.
        filters = []
        # Iterate over the given aggregates.
        for unit in units:
            # The original operand of the aggregate.
            code = unit.code
            # A list of all non-shared filters on the unit operand space.
            code_filters = []
            unit_space = unit.plural_space
            while unit_space != shared_space:
                code_filters.append(unit_space.filter)
                unit_space = unit_space.base
            # If there are any filters, we need to rewrite the operand.
            if code_filters:
                # Merge all filters using `AND`.
                if len(code_filters) > 1:
                    code_filter = FormulaCode(AndSig(),
                                              coerce(BooleanDomain()),
                                              unit.space.flow,
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
                op = FormulaCode(IfSig(), op.domain, op.flow,
                                 predicates=[code_filter],
                                 consequents=[op],
                                 alternative=None)
                code = code.clone(op=op)
            # Add the (possibly) rewritten operand to the list.
            codes.append(code)

        # Check if we can apply the non-shared filters to the shared
        # space.  Technically, it is not necessary, but may improve
        # performance in some cases.  So we can do it only if every
        # aggregate has some filter applied on top of the shared space.
        # Also, we don't apply the filters on top of a complement space
        # as it cannot improve the performace.
        if (not isinstance(shared_space, ComplementSpace) and
                    all(unit.plural_space != shared_space for unit in units)):
            if len(filters) > 1:
                filter = FormulaCode(OrSig(), coerce(BooleanDomain()),
                                     shared_space.flow, ops=filters)
            else:
                [filter] = filters
            shared_space = FilteredSpace(shared_space, filter,
                                       shared_space.flow)

        # Now that the content of new units is generated, recombine
        # it against a blank state.
        substate = self.state.spawn()
        substate.collect(base_space)
        substate.collect(shared_space)
        for code in codes:
            substate.collect(code)
        substate.recombine()
        base_space = substate.replace(base_space)
        shared_space = substate.replace(shared_space)
        codes = [substate.replace(code) for code in codes]

        # Iterate over original units generating replacements.
        for idx, unit in enumerate(units):
            # The new unit expression and companions.
            code = codes[idx]
            # Generate and memorize the replacement.
            companions = codes[:idx]+codes[idx+1:]
            batch = unit.clone(code=code, plural_space=shared_space,
                               space=base_space, companions=companions)
            self.state.memorize(unit, batch)

        # The case when the aggregates could be embedded into the quotient
        # frame.
        if is_quotient:
            base_space = base_space.clone(companions=codes)
            self.state.memorize(space, base_space)


class RewriteBase(Adapter):
    """
    Applies the rewriting process to the given node.

    This is a base class for all rewriting adapters, it encapsulates
    common attributes and methods shared by all its subclasses.

    Most rewriting adapters have the following signature:

        Rewrite: (Expression, RewritingState) -> Expression

    The adapters are polymorphic on the first argument.

    `expression` (:class:`htsql.core.tr.space.Expression`)
        The expression node to rewrite.

    `state` (:class:`RewritingState`)
        The current state of rewriting process.
    """

    adapt(Expression)

    def __init__(self, expression, state):
        #assert isinstance(expression, Expression)
        #assert isinstance(state, RewritingState)
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

    Unmasking prunes non-axial space nodes that are already enforced
    by the current mask space.
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

    #adapt(QueryExpr)

    def __call__(self):
        # Initialize the rewriting state.
        self.state.set_root(RootSpace(None, self.expression.flow))
        # Rewrite the segment, if present.
        segment = None
        if self.expression.segment is not None:
            segment = self.expression.segment
            # Rewrite: simplify expressions matching certain patterns.
            segment = self.state.rewrite(segment)
            # Unmask: eliminate redundant non-axial space operations.
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

    adapt(SegmentExpr)

    def __call__(self):
        # Rewrite the output space and output record.
        root = self.state.rewrite(self.expression.root)
        space = self.state.rewrite(self.expression.space)
        codes = [self.state.rewrite(code)
                 for code in self.expression.codes]
        dependents = [self.state.rewrite(dependent)
                      for dependent in self.expression.dependents]
        return self.expression.clone(root=root, space=space, codes=codes,
                                     dependents=dependents)


class UnmaskSegment(Unmask):

    adapt(SegmentExpr)

    def __call__(self):
        # Unmask the output record against the output space.
        codes = [self.state.unmask(code, self.expression.space)
                 for code in self.expression.codes]
        dependents = [self.state.unmask(dependent, self.expression.space)
                      for dependent in self.expression.dependents]
        # Unmask the space itself.
        space = self.state.unmask(self.expression.space,
                                 mask=self.expression.root)
        root = self.state.unmask(self.expression.root)
        # Produce a clone of the segment with new space and output columns.
        return self.expression.clone(root=root, space=space, codes=codes,
                                     dependents=dependents)


class CollectSegment(Collect):

    adapt(SegmentExpr)

    def __call__(self):
        pass


class ReplaceSegment(Replace):

    adapt(SegmentExpr)

    def __call__(self):
        # Recombine the content of the segment against a blank state.
        substate = self.state.spawn()
        substate.collect(self.expression.root)
        substate.collect(self.expression.space)
        for code in self.expression.codes:
            substate.collect(code)
        for dependent in self.expression.dependents:
            substate.collect(dependent)
        substate.recombine()
        root = substate.replace(self.expression.root)
        space = substate.replace(self.expression.space)
        codes = [substate.replace(code)
                 for code in self.expression.codes]
        dependents = [substate.replace(dependent)
                      for dependent in self.expression.dependents]
        return self.expression.clone(root=root, space=space, codes=codes,
                                     dependents=dependents)


class RewriteSpace(Rewrite):

    adapt(Space)

    def __init__(self, space, state):
        # Overriden to replace the attribute.
        super(RewriteSpace, self).__init__(space, state)
        self.space = space

    def __call__(self):
        # No-op for the root space.
        if self.space.base is None:
            return self.space
        # Otherwise, apply the adapter to the parent space.
        base = self.state.rewrite(self.space.base)
        return self.space.clone(base=base)


class UnmaskSpace(Unmask):

    adapt(Space)

    def __init__(self, space, state):
        # Overriden to rename the attribute.
        super(UnmaskSpace, self).__init__(space, state)
        self.space = space

    def __call__(self):
        # No-op for the root space.
        if self.space.base is None:
            return self.space
        # Apply the adapter to the parent space.
        base = self.state.unmask(self.space.base)
        return self.space.clone(base=base)


class CollectSpace(Collect):

    adapt(Space)

    def __init__(self, space, state):
        # Overriden to rename the attribute.
        super(CollectSpace, self).__init__(space, state)
        self.space = space

    def __call__(self):
        # No-op for the root space.
        if self.space.base is None:
            return
        # Apply the adapter to the parent space.
        self.state.collect(self.space.base)


class ReplaceSpace(Replace):

    adapt(Space)

    def __init__(self, space, state):
        # Overriden to rename the attribute.
        super(ReplaceSpace, self).__init__(space, state)
        self.space = space

    def __call__(self):
        # No-op for the root space.
        if self.space.base is None:
            return self.space
        # Otherwise, replace the parent space.
        base = self.state.replace(self.space.base)
        return self.space.clone(base=base)


class RewriteQuotient(RewriteSpace):

    adapt(QuotientSpace)

    def __call__(self):
        # Apply the adapter to all sub-nodes.
        base = self.state.rewrite(self.space.base)
        seed = self.state.rewrite(self.space.family.seed)
        kernels = [self.state.rewrite(code)
                   for code in self.space.family.kernels]
        return self.space.clone(base=base, seed=seed, kernels=kernels)


class UnmaskQuotient(UnmaskSpace):

    adapt(QuotientSpace)

    def __call__(self):
        # Unmask the kernel against the seed space.
        kernels = [self.state.unmask(code, mask=self.space.family.seed)
                   for code in self.space.family.kernels]
        # Verify that the kernel is not scalar.  We can't do it earlier
        # because since unmasking may remove fantom units.
        if all(not code.units for code in kernels):
            raise Error("Found an empty or constant kernel")
        # Unmask the seed against the quotient parent space.
        seed = self.state.unmask(self.space.family.seed, mask=self.space.base)
        # Unmask the parent space against the current mask.
        base = self.state.unmask(self.space.base)
        return self.space.clone(base=base, seed=seed, kernels=kernels)


class ReplaceQuotient(ReplaceSpace):

    adapt(QuotientSpace)

    def __call__(self):
        # Replace the parent space.
        base = self.state.replace(self.space.base)
        # Create a new empty state.
        substate = self.state.spawn()
        # Collect/recombine/replace units in the seed and kernel expressions
        # against a fresh state.
        substate.collect(self.space.seed)
        for code in self.space.kernels:
            substate.collect(code)
        substate.recombine()
        seed = substate.replace(self.space.seed)
        kernels = [substate.replace(code)
                   for code in self.space.kernels]
        # Produce a recombined node.
        return self.space.clone(base=base, seed=seed, kernels=kernels)


class RewriteMoniker(RewriteSpace):

    adapt_many(MonikerSpace,
               ClippedSpace)

    def __call__(self):
        # Apply the adapter to all child nodes.
        base = self.state.rewrite(self.space.base)
        seed = self.state.rewrite(self.space.seed)
        return self.space.clone(base=base, seed=seed)


class UnmaskMoniker(UnmaskSpace):

    adapt_many(MonikerSpace,
               ClippedSpace)

    def __call__(self):
        # Unmask the seed space against the parent space.
        seed = self.state.unmask(self.space.seed, mask=self.space.base)
        # Unmask the parent space against the current mask.
        base = self.state.unmask(self.space.base)
        return self.space.clone(base=base, seed=seed)


class ReplaceMoniker(Replace):

    adapt_many(MonikerSpace,
               ClippedSpace)

    def __call__(self):
        # Replace the parent space.
        base = self.state.replace(self.space.base)
        # Recombine the seed space against a fresh state.
        substate = self.state.spawn()
        substate.collect(self.space.seed)
        substate.recombine()
        seed = substate.replace(self.space.seed)
        # Produce a recombined space node.
        return self.space.clone(base=base, seed=seed)


class RewriteForked(RewriteSpace):

    adapt(ForkedSpace)

    def __call__(self):
        # Apply the adapter to all child nodes.
        base = self.state.rewrite(self.space.base)
        seed = self.state.rewrite(self.space.seed)
        kernels = [self.state.rewrite(code)
                   for code in self.space.kernels]
        return self.space.clone(base=base, seed=seed, kernels=kernels)


class UnmaskForked(UnmaskSpace):

    adapt(ForkedSpace)

    def __call__(self):
        # Prune all but trailing non-axial operations from the seed space.
        seed = self.state.unmask(self.space.seed, mask=self.space.ground)
        # Unmask the kernel against the parent space.
        kernels = [self.state.unmask(code, mask=self.space.base)
                   for code in self.space.kernels]
        # Unmask the parent space.
        base = self.state.unmask(self.space.base)
        return self.space.clone(base=base, seed=seed, kernels=kernels)


class CollectForked(Collect):

    adapt(ForkedSpace)

    def __call__(self):
        # Collect units in the parent space.
        self.state.collect(self.space.base)
        # Ignore the seed space as it is a duplicate of the parent space,
        # but process through the kernel.
        # FIXME: do we need to process the kernel?
        for code in self.space.kernels:
            self.state.collect(code)


class ReplaceForked(Replace):

    adapt(ForkedSpace)

    def __call__(self):
        # Replace the parent space.
        base = self.state.replace(self.space.base)
        # Replace the kernel.
        # FIXME: where to replace the kernel?  Perhaps store two copies
        # of the kernel?
        kernels = [self.state.replace(code) for code in self.space.kernels]
        # Recombine the seed space against a fresh state.
        substate = self.state.spawn()
        substate.collect(self.space.seed)
        substate.recombine()
        seed = substate.replace(self.space.seed)
        # Produce a recombined node.
        return self.space.clone(base=base, seed=seed, kernels=kernels)


class RewriteAttach(RewriteSpace):

    adapt(AttachSpace)

    def __call__(self):
        # Rewrite the child nodes.
        base = self.state.rewrite(self.space.base)
        seed = self.state.rewrite(self.space.seed)
        images = [(self.state.rewrite(lcode), self.state.rewrite(rcode))
                  for lcode, rcode in self.space.images]
        filter = self.space.filter
        if filter is not None:
            filter = self.state.rewrite(filter)
            if (isinstance(filter, LiteralCode) and
                isinstance(filter.domain, BooleanDomain) and
                filter.value is True):
                filter = None
        predicates = []
        all_images = images
        images = []
        for lcode, rcode in all_images:
            if not lcode.units:
                code = FormulaCode(IsEqualSig(+1), BooleanDomain(),
                                   self.space.flow, lop=rcode, rop=lcode)
                predicates.append(code)
            else:
                images.append((lcode, rcode))
        if filter is not None:
            if isformula(filter, AndSig):
                ops = filter.ops
            else:
                ops = [filter]
            for op in ops:
                if (isformula(op, IsEqualSig) and
                        op.signature.polarity == +1):
                    if (op.lop.units and
                            all(self.space.base.spans(unit.space)
                                for unit in op.lop.units) and
                            any(not self.space.base.spans(unit.space)
                                for unit in op.rop.units)):
                        images.append((op.lop, op.rop))
                        continue
                    if (op.rop.units and
                            all(self.space.base.spans(unit.space)
                                for unit in op.rop.units) and
                            any(not self.space.base.spans(unit.space)
                                for unit in op.lop.units)):
                        images.append((op.rop, op.lop))
                        continue
                predicates.append(op)
        if len(predicates) == 0:
            filter = None
        elif len(predicates) == 1:
            [filter] = predicates
        else:
            filter = FormulaCode(AndSig(), BooleanDomain(),
                                 self.space.flow, ops=predicates)
        return self.space.clone(base=base, seed=seed, images=images,
                               filter=filter)


class UnmaskAttach(UnmaskSpace):

    adapt(AttachSpace)

    def __call__(self):
        # Unmask the parent space.
        base = self.state.unmask(self.space.base)
        # Unmask the seed space against the parent.
        seed = self.state.unmask(self.space.seed, mask=self.space.base)
        # Unmask the parent image against the parent space and the seed
        # image against the seed space.
        images = [(self.state.unmask(lcode, mask=self.space.base),
                   self.state.unmask(rcode, mask=self.space.seed))
                  for lcode, rcode in self.space.images]
        filter = None
        if self.space.filter is not None:
            filter = self.state.unmask(self.space.filter, mask=self.space.seed)
        return self.space.clone(base=base, seed=seed, images=images,
                               filter=filter)


class CollectAttach(Collect):

    adapt(AttachSpace)

    def __call__(self):
        # Gather units in the parent space and the parent images.
        self.state.collect(self.space.base)
        for lcode, rcode in self.space.images:
            self.state.collect(lcode)


class ReplaceAttach(Replace):

    adapt(AttachSpace)

    def __call__(self):
        # Replace the parent space and parental images.
        base = self.state.replace(self.space.base)
        images = [(self.state.replace(lcode), rcode)
                  for lcode, rcode in self.space.images]
        # Recombine the seed space and seed images against a fresh state.
        substate = self.state.spawn()
        substate.collect(self.space.seed)
        for lcode, rcode in images:
            substate.collect(rcode)
        substate.recombine()
        seed = substate.replace(self.space.seed)
        images = [(lcode, substate.replace(rcode))
                  for lcode, rcode in images]
        return self.space.clone(base=base, seed=seed, images=images)


class RewriteFiltered(RewriteSpace):

    adapt(FilteredSpace)

    def __call__(self):
        # Rewrite the parent space and the filter expression.
        base = self.state.rewrite(self.space.base)
        filter = self.state.rewrite(self.space.filter)
        # Eliminate a `?true` filter.
        if (isinstance(filter, LiteralCode) and
            isinstance(filter.domain, BooleanDomain) and
            filter.value is True):
            return base
        return self.space.clone(base=base, filter=filter)


class UnmaskFiltered(UnmaskSpace):

    adapt(FilteredSpace)

    def __call__(self):
        # If the filter is already enforced by the mask,
        # remove the filter, return an unmasked parent space.
        if (self.space.prune(self.state.mask)
                == self.space.base.prune(self.state.mask)):
            return self.state.unmask(self.space.base)
        # Choose the mask to use for unmasking the filter.  Use the parent
        # space unless it dominates the current mask (that is, the mask
        # contains all non-axial operations of the parent space),
        if self.space.base.dominates(self.state.mask):
            filter = self.state.unmask(self.space.filter)
        else:
            filter = self.state.unmask(self.space.filter,
                                       mask=self.space.base)
        # Unmask the parent space against the current mask.
        base = self.state.unmask(self.space.base)
        return self.space.clone(base=base, filter=filter)


class CollectFiltered(Collect):

    adapt(FilteredSpace)

    def __call__(self):
        # Collect units in all child nodes.
        self.state.collect(self.space.base)
        self.state.collect(self.space.filter)


class ReplaceFiltered(Replace):

    adapt(FilteredSpace)

    def __call__(self):
        # Replace all child nodes.
        base = self.state.replace(self.space.base)
        filter = self.state.replace(self.space.filter)
        return self.space.clone(base=base, filter=filter)


class RewriteOrdered(RewriteSpace):

    adapt(OrderedSpace)

    def __call__(self):
        # Rewrite child nodes.
        base = self.state.rewrite(self.space.base)
        order = [(self.state.rewrite(code), direction)
                 for code, direction in self.space.order]
        return self.space.clone(base=base, order=order)


class UnmaskOrdered(UnmaskSpace):

    adapt(OrderedSpace)

    def __call__(self):
        # If the ordering operation is already enforced by the mask,
        # return the parent space.
        if (self.space.prune(self.state.mask)
                == self.space.base.prune(self.state.mask)):
            return self.state.unmask(self.space.base)
        # Choose a mask for order expressions; use the parent space
        # unless it dominates the current mask, in which case use
        # the current mask.
        if self.space.base.dominates(self.state.mask):
            order = [(self.state.unmask(code), direction)
                     for code, direction in self.space.order]
        else:
            order = [(self.state.unmask(code, mask=self.space.base),
                      direction)
                     for code, direction in self.space.order]
        # Unmask the parent space, but only if `limit` and `offset` are
        # not specified.
        if self.space.is_expanding:
            base = self.state.unmask(self.space.base)
        else:
            base = self.state.unmask(self.space.base, mask=self.state.root)
        return self.space.clone(base=base, order=order)


class CollectOrdered(Collect):

    adapt(OrderedSpace)

    def __call__(self):
        # Collect units in all child nodes.
        self.state.collect(self.space.base)
        for code, direction in self.space.order:
            self.state.collect(code)


class ReplaceOrdered(Replace):

    adapt(OrderedSpace)

    def __call__(self):
        # Replace units in all child nodes.
        base = self.state.replace(self.space.base)
        order = [(self.state.replace(code), direction)
                 for code, direction in self.space.order]
        return self.space.clone(base=base, order=order)


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
                           self.code.flow, **arguments)


class ReplaceFormula(ReplaceCode):

    adapt(FormulaCode)

    def __call__(self):
        # Replace units in the formula arguments.
        arguments = self.code.arguments.map(self.state.replace)
        return FormulaCode(self.code.signature, self.code.domain,
                           self.code.flow, **arguments)


class RewriteBySignature(Adapter):
    """
    Rewrites a formula node.

    This is an auxiliary interface used by :class:`Rewrite` adapter.
    It is polymorphic on the signature of the formula.

    `code` (:class:`htsql.core.tr.space.FormulaCode`)
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
        #assert isinstance(code, FormulaCode)
        #assert isinstance(state, RewritingState)
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
                           self.code.flow, **arguments)


class RewriteUnit(RewriteCode):

    adapt(Unit)

    def __init__(self, unit, state):
        # Overriden to rename the attribute.
        super(RewriteUnit, self).__init__(unit, state)
        self.unit = unit

    def __call__(self):
        # Apply the adapter to child nodes.
        space = self.state.rewrite(self.unit.space)
        return self.unit.clone(space=space)


class UnmaskUnit(UnmaskCode):

    adapt(Unit)

    def __init__(self, unit, state):
        # Overriden to rename the attribute.
        super(UnmaskUnit, self).__init__(unit, state)
        self.unit = unit

    def __call__(self):
        # Apply the adapter to child nodes.
        space = self.state.unmask(self.unit.space)
        return self.unit.clone(space=space)


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
        substate.collect(self.unit.space)
        substate.recombine()
        space = substate.replace(self.unit.space)
        return self.unit.clone(space=space)


class UnmaskColumn(UnmaskUnit):

    adapt(ColumnUnit)

    def __call__(self):
        space = self.state.unmask(self.unit.space)
        column = self.unit.column
        while (isinstance(space, FiberTableSpace) and space.join.is_direct and
               space.is_expanding and space.is_contracting):
            for origin_column, target_column in zip(space.join.origin_columns,
                                                    space.join.target_columns):
                if column is target_column:
                    space = space.base
                    column = origin_column
                    break
            else:
                break
        return self.unit.clone(space=space, column=column)


class RewriteCompound(RewriteUnit):

    adapt(CompoundUnit)

    def __call__(self):
        # Rewrite the content of the node.
        code = self.state.rewrite(self.unit.code)
        space = self.state.rewrite(self.unit.space)
        return self.unit.clone(code=code, space=space)


class ReplaceCompound(ReplaceUnit):

    adapt(CompoundUnit)

    def __call__(self):
        # Recombine the content of the unit node against a blank state.
        substate = self.state.spawn()
        substate.collect(self.unit.code)
        substate.collect(self.unit.space)
        substate.recombine()
        code = substate.replace(self.unit.code)
        space = substate.replace(self.unit.space)
        return self.unit.clone(code=code, space=space)


class UnmaskScalar(UnmaskUnit):

    adapt(ScalarUnit)

    def __call__(self):
        # The unit is redundant if the mask is dominated by the unit space.
        if self.unit.space.dominates(self.state.mask):
            code = self.state.unmask(self.unit.code)
            return code
        # It is also redundant if the operand is a unit under the same
        # or a dominated space.
        if (isinstance(self.unit.code, Unit) and
                self.unit.space.dominates(self.unit.code.space)):
            code = self.state.unmask(self.unit.code)
            return code
        # Unmask the unit expression against the unit space.
        code = self.state.unmask(self.unit.code, mask=self.unit.space)
        # Unmask the unit space against the current mask.
        space = self.state.unmask(self.unit.space)
        return self.unit.clone(code=code, space=space)


class RewriteAggregate(RewriteUnit):

    adapt(AggregateUnitBase)

    def __call__(self):
        # Rewrite the content of the node.
        code = self.state.rewrite(self.unit.code)
        plural_space = self.state.rewrite(self.unit.plural_space)
        space = self.state.rewrite(self.unit.space)
        return self.unit.clone(code=code, plural_space=plural_space, space=space)


class UnmaskAggregate(UnmaskUnit):

    adapt(AggregateUnitBase)

    def __call__(self):
        # Unmask the argument against the plural space.
        code = self.state.unmask(self.unit.code, mask=self.unit.plural_space)
        # Unmask the plural space against the unit space unless it dominates
        # the current mask.
        if self.unit.space.dominates(self.state.mask):
            plural_space = self.state.unmask(self.unit.plural_space)
        else:
            plural_space = self.state.unmask(self.unit.plural_space,
                                            mask=self.unit.space)
        # Unmask the unit space against the current mask.
        space = self.state.unmask(self.unit.space)
        return self.unit.clone(code=code, plural_space=plural_space, space=space)


class ReplaceAggregate(ReplaceUnit):

    adapt(AggregateUnitBase)

    def __call__(self):
        # Recombine the content of the unit node against a blank state.
        substate = self.state.spawn()
        substate.collect(self.unit.code)
        substate.collect(self.unit.plural_space)
        substate.collect(self.unit.space)
        substate.recombine()
        code = substate.replace(self.unit.code)
        plural_space = substate.replace(self.unit.plural_space)
        space = substate.replace(self.unit.space)
        return self.unit.clone(code=code, plural_space=plural_space, space=space)


class RewriteKernel(RewriteUnit):

    adapt(KernelUnit)

    def __call__(self):
        # At this stage, the kernel code is an element of the family kernel.
        assert self.unit.code in self.unit.space.family.kernels
        index = self.unit.space.family.kernels.index(self.unit.code)
        # Rewrite the quotient space.
        space = self.state.rewrite(self.unit.space)
        # Get the new kernel code.
        code = space.family.kernels[index]
        return self.unit.clone(code=code, space=space)


class UnmaskKernel(UnmaskUnit):

    adapt(KernelUnit)

    def __call__(self):
        # At this stage, the kernel code is an element of the family kernel.
        assert self.unit.code in self.unit.space.family.kernels
        index = self.unit.space.family.kernels.index(self.unit.code)
        # Unmask the quotient space.
        space = self.state.unmask(self.unit.space)
        # Get the new kernel code.
        code = space.family.kernels[index]
        return self.unit.clone(code=code, space=space)


class ReplaceKernel(ReplaceUnit):

    adapt(KernelUnit)

    def __call__(self):
        # At this stage, the kernel code is an element of the family kernel.
        assert self.unit.code in self.unit.space.family.kernels
        index = self.unit.space.family.kernels.index(self.unit.code)
        # Recombine the quotient space.
        substate = self.state.spawn()
        substate.collect(self.unit.space)
        substate.recombine()
        space = substate.replace(self.unit.space)
        # Get the new kernel code.
        code = space.family.kernels[index]
        return self.unit.clone(code=code, space=space)


class UnmaskCovering(UnmaskUnit):
    # FIXME: not used?

    adapt(CoveringUnit)

    def __call__(self):
        # The unit expression is evaluated against the seed space
        # of the unit space, so use the seed as the mask.
        code = self.state.unmask(self.unit.code,
                                 mask=self.unit.space.seed)
        # Unmask the unit space.
        space = self.state.unmask(self.unit.space)
        return self.unit.clone(code=code, space=space)


def rewrite(segment):
    root = RootSpace(None, segment.flow)
    state = RewritingState(root)
    with translate_guard(segment):
        segment = state.rewrite(segment)
        segment = state.unmask(segment)
        state.collect(segment)
        state.recombine()
        segment = state.replace(segment)
    return segment

