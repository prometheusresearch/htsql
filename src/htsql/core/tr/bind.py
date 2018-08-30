#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


"""
:mod:`htsql.core.tr.bind`
=========================

This module implements the binding process.
"""


from ..util import maybe, listof, tupleof, similar
from ..adapter import Adapter, Protocol, adapt, adapt_many
from ..domain import (Domain, BooleanDomain, IntegerDomain, DecimalDomain,
        FloatDomain, UntypedDomain, EntityDomain, RecordDomain, ListDomain,
        IdentityDomain, VoidDomain)
from ..classify import normalize
from ..error import Error, translate_guard, choices_guard, point
from ..syn.syntax import (Syntax, CollectSyntax, SelectSyntax, ApplySyntax,
        FunctionSyntax, PipeSyntax, OperatorSyntax, PrefixSyntax,
        ProjectSyntax, FilterSyntax, LinkSyntax, DetachSyntax, AttachSyntax,
        AssignSyntax, ComposeSyntax, LocateSyntax, IdentitySyntax, GroupSyntax,
        IdentifierSyntax, UnpackSyntax, ReferenceSyntax, LiftSyntax,
        StringSyntax, LabelSyntax, NumberSyntax, RecordSyntax, DirectSyntax)
from .binding import (Binding, WrappingBinding, CollectBinding, RootBinding,
        HomeBinding, TableBinding, ChainBinding, ColumnBinding,
        QuotientBinding, KernelBinding, ComplementBinding, LocateBinding,
        SieveBinding, AttachBinding, SortBinding, CastBinding, IdentityBinding,
        ImplicitCastBinding, RescopingBinding, AssignmentBinding,
        DefineBinding, DefineReferenceBinding, DefineCollectionBinding,
        DefineLiftBinding, SelectionBinding, WildSelectionBinding,
        DirectionBinding, TitleBinding, RerouteBinding,
        ReferenceRerouteBinding, AliasBinding, LiteralBinding, FormulaBinding,
        VoidBinding, Recipe, LiteralRecipe, SelectionRecipe, FreeTableRecipe,
        AttachedTableRecipe, ColumnRecipe, KernelRecipe, ComplementRecipe,
        IdentityRecipe, ChainRecipe, SubstitutionRecipe, BindingRecipe,
        ClosedRecipe, PinnedRecipe, AmbiguousRecipe)
from .lookup import (lookup_attribute, lookup_reference, lookup_complement,
        lookup_attribute_set, lookup_reference_set, expand, direct, guess_tag,
        identify, unwrap)
from .signature import IsEqualSig, AndSig
from .coerce import coerce
from .decorate import decorate


class BindingState(object):

    def __init__(self, root, environment=None):
        assert isinstance(root, RootBinding)
        # The root lookup scope.
        self.root = root
        # The current lookup scope.
        self.scope = root
        # The stack of previous lookup scopes.
        self.scope_stack = []
        # References in the root scope.
        self.environment = environment
        if self.environment is not None:
            collection = {}
            for name, recipe in self.environment:
                name = normalize(name)
                collection[name] = recipe
            if collection:
                self.scope = DefineCollectionBinding(
                        self.scope, collection, True, self.scope.syntax)

    def push_scope(self, scope):
        """
        Sets the new lookup scope.

        This function stores the current scope in the stack and makes
        the given binding the new lookup scope.  Use the :attr:`scope`
        attribute to get the current scope; :meth:`pop_scope` to restore
        the previous scope.

        `scope` (:class:`htsql.core.tr.binding.Binding`)
            The new lookup scope.
        """
        # Sanity check on the argument.
        assert isinstance(scope, Binding)
        # Ensure that the root scope was set.
        assert self.root is not None
        # Save the current lookup scope.
        self.scope_stack.append(self.scope)
        # Assign the new lookup scope.
        self.scope = scope

    def pop_scope(self):
        """
        Restores the previous lookup scope.

        This functions restores the previous lookup scope from the stack.
        Use the :attr:`scope` attribute to get the current scope;
        :meth:`push_scope` to change the current scope.
        """
        # Restore the prevous lookup scope from the stack.
        self.scope = self.scope_stack.pop()

    def bind(self, syntax, scope=None):
        """
        Binds the given syntax node using the current binding state.

        Returns a binding node.

        `syntax` (:class:`htsql.core.tr.syntax.Syntax`)
            The syntax node to bind.

        `scope` (:class:`htsql.core.tr.binding.Binding` or ``None``)
            If set, the lookup scope is set to `scope` when
            binding the syntax node.
        """
        with translate_guard(syntax):
            if scope is not None:
                self.push_scope(scope)
            binding = Bind.__prepare__(syntax, self)()
            if scope is not None:
                self.pop_scope()
            return binding

    def use(self, recipe, syntax, scope=None):
        """
        Applies a recipe to produce a binding node.

        Returns a binding node.

        `recipe` (:class:`htsql.core.tr.binding.Recipe`)
            The recipe to apply.

        `syntax` (:class:`htsql.core.tr.syntax.Syntax`)
            The syntax node associated with the recipe.

        `scope` (:class:`htsql.core.tr.binding.Binding` or ``None``)
            If set, the lookup scope is set to `scope` when
            binding the syntax node.
        """
        # If passed, set the new lookup scope.
        if scope is not None:
            self.push_scope(scope)
        # Realize and apply `BindByRecipe` adapter.
        with translate_guard(syntax):
            binding = BindByRecipe.__invoke__(recipe, syntax, self)
        # Restore the old lookup scope.
        if scope is not None:
            self.pop_scope()
        # Return the generated binding node.
        return binding

    def call(self, syntax, scope=None):
        """
        Binds a global function or a global identifier.

        Returns a binding node.

        `syntax` (:class:`htsql.core.tr.syntax.Syntax`)
            The syntax node to bind.

        `scope` (:class:`htsql.core.tr.binding.Binding` or ``None``)
            If set, the lookup context is set to `scope` when
            binding the syntax node.
        """
        # If passed, set the new lookup scope.
        if scope is not None:
            self.push_scope(scope)
        # Realize and apply `BindByName` protocol.
        with translate_guard(syntax):
            binding = BindByName.__invoke__(syntax, self)
        # Restore the old lookup scope.
        if scope is not None:
            self.pop_scope()
        # Return the generated binding node.
        return binding


class Bind(Adapter):
    """
    Translates a syntax node to a binding node.

    This is an interface adapter; see subclasses for implementations.

    The binding process resolves identifiers against database objects,
    resolves and validates operators and function calls, and determine
    types of all expression.

    The :class:`Bind` adapter has the following signature::

        Bind: (Syntax, BindingState) -> Binding

    The adapter is polymorphic on the `Syntax` argument.

    `syntax` (:class:`htsql.core.tr.syntax.Syntax`)
        The syntax node to bind.

    `state` (:class:`BindingState`)
        The current state of the binding process.
    """

    adapt(Syntax)

    def __init__(self, syntax, state):
        assert isinstance(syntax, Syntax)
        assert isinstance(state, BindingState)
        self.syntax = syntax
        self.state = state

    def __call__(self):
        # The default implementation raises an error.  It is actually
        # unreachable since we provide an implementation for all syntax nodes.
        raise Error("Unable to bind a node")


def hint_choices(choices):
    # Generate a hint from a list of choices.
    assert isinstance(choices, listof(str))
    if not choices:
        return None
    chunks = ["did you mean:"]
    if len(choices) == 1:
        chunks.append("'%s'" % choices[0].encode('utf-8'))
    else:
        chunks.append(", ".join("'%s'" % choice.encode('utf-8')
                                for choice in choices[:-1]))
        chunks.append("or")
        chunks.append("'%s'" % choices[-1].encode('utf-8'))
    return " ".join(chunks)


class BindCollect(Bind):

    adapt(CollectSyntax)

    def __call__(self):
        ## FIXME: an empty segment syntax should not be generated.
        #if self.syntax.arm is None:
        #    raise Error("output columns are not specified",
        #                self.syntax.mark)
        # Bind the segment expression.
        if self.syntax.arm is not None:
            seed = self.state.bind(self.syntax.arm)
            if isinstance(seed, AssignmentBinding):
                with translate_guard(seed):
                    if len(seed.terms) != 1:
                        raise Error("Qualified definition is not allowed"
                                    " for an in-segment assignment")
                    if seed.parameters is not None:
                        raise Error("Parameterized definition is not allowed"
                                    " for an in-segment assignment")
                name, is_reference = seed.terms[0]
                if is_reference:
                    recipe = BindingRecipe(self.state.bind(seed.body))
                else:
                    recipe = SubstitutionRecipe(self.state.scope, [],
                                                None, seed.body)
                recipe = ClosedRecipe(recipe)
                syntax = seed.syntax
                if isinstance(syntax, AssignSyntax):
                    syntax = syntax.larm
                seed = self.state.use(recipe, syntax)
        else:
            seed = self.state.scope
        seed = Select.__invoke__(seed, self.state)
        domain = ListDomain(seed.domain)
        return CollectBinding(self.state.scope, seed, domain,
                              self.syntax)


class Select(Adapter):

    adapt(Domain)

    @classmethod
    def __dispatch__(interface, binding, *args, **kwds):
        assert isinstance(binding, Binding)
        return (type(binding.domain),)

    def __init__(self, binding, state):
        self.binding = binding
        self.state = state

    def __call__(self):
        domain = coerce(self.binding.domain)
        if domain is None:
            # FIXME: separate implementation for VoidDomain with a better error
            # message.
            raise Error("Output column must be scalar")
        return ImplicitCastBinding(self.binding, domain, self.binding.syntax)


class SelectRecord(Select):

    adapt_many(EntityDomain,
               RecordDomain)

    def __call__(self):
        recipes = expand(self.binding, with_syntax=True, with_wild=True,
                         with_class=True)
        if recipes is None:
            return super(SelectRecord, self).__call__()
        elements = []
        for syntax, recipe in recipes:
            element = self.state.use(recipe, syntax, scope=self.binding)
            element = Select.__invoke__(element, self.state)
            elements.append(element)
        fields = [decorate(element) for element in elements]
        domain = RecordDomain(fields)
        binding = SelectionBinding(self.binding, elements, domain,
                                   self.binding.syntax)
        return binding


class SelectList(Select):

    adapt(ListDomain)

    def __call__(self):
        return self.binding


class SelectIdentity(Select):

    adapt(IdentityDomain)

    def __call__(self):
        return self.binding


class SelectUntyped(Select):

    adapt(UntypedDomain)

    def __call__(self):
        return self.binding


class BindSelect(Bind):

    adapt(SelectSyntax)

    def __call__(self):
        scope = self.state.bind(self.syntax.larm)
        return self.state.bind(self.syntax.rarm, scope=scope)


class BindRecord(Bind):

    adapt(RecordSyntax)

    def __call__(self):
        # Extract selector elements.
        elements = []
        scope = self.state.scope
        self.state.push_scope(scope)
        for arm in self.syntax.arms:
            binding = self.state.bind(arm)
            # Handle in-selector assignments.
            if isinstance(binding, AssignmentBinding):
                with translate_guard(binding):
                    if len(binding.terms) != 1:
                        raise Error("Qualified definition is not allowed"
                                    " for an in-selector assignment")
                    if binding.parameters is not None:
                        raise Error("Parameterized definition is not allowed"
                                    " for an in-selector assignment")
                name, is_reference = binding.terms[0]
                if is_reference:
                    recipe = BindingRecipe(self.state.bind(binding.body))
                else:
                    recipe = SubstitutionRecipe(scope, [],
                                                None, binding.body)
                recipe = ClosedRecipe(recipe)
                syntax = binding.syntax
                if isinstance(syntax, AssignSyntax):
                    syntax = syntax.larm.larms[0]
                binding = self.state.use(recipe, syntax)
                if is_reference:
                    scope = DefineReferenceBinding(scope, name,
                                                   recipe, scope.syntax)
                else:
                    scope = DefineBinding(scope, name, None,
                                          recipe, scope.syntax)
                self.state.pop_scope()
                self.state.push_scope(scope)
            # Extract nested selectors, if any.
            bindings = []
            recipes = expand(binding, with_wild=True)
            if recipes is not None:
                seed = binding
                for syntax, recipe in recipes:
                    binding = self.state.use(recipe, syntax)
                    binding = RescopingBinding(binding, seed, binding.syntax)
                    bindings.append(binding)
            else:
                bindings.append(binding)
            # Handle in-selector direction decorators.
            order = []
            for binding in bindings:
                direction = direct(binding)
                if direction is not None:
                    order.append(binding)
            if order:
                scope = SortBinding(scope, order, None, None, scope.syntax)
                self.state.pop_scope()
                self.state.push_scope(scope)
            elements.extend(bindings)
        self.state.pop_scope()
        # Generate a selection scope.
        fields = [decorate(element) for element in elements]
        domain = RecordDomain(fields)
        return SelectionBinding(scope, elements, domain, self.syntax)


class BindApply(Bind):

    adapt(ApplySyntax)

    def __call__(self):
        # Look for the parameterized attribute in the current local scope.
        recipe = lookup_attribute(self.state.scope,
                                  self.syntax.name, len(self.syntax.arguments))
        if recipe is not None:
            binding = self.state.use(recipe, self.syntax)
        # If not found, look for a global function.
        else:
            binding = self.state.call(self.syntax)
        return binding


class BindOperator(Bind):

    adapt(OperatorSyntax)

    def __call__(self):
        # Look for the operator in the global scope.  We skip the local scope
        # as there is no way to add an operator to a local scope.
        return self.state.call(self.syntax)


class BindProject(Bind):

    adapt(ProjectSyntax)

    def __call__(self):
        # Get the seed of the quotient.
        seed = self.state.bind(self.syntax.larm)
        # get the kernel expressions.
        elements = []
        binding = self.state.bind(self.syntax.rarm, scope=seed)
        recipes = expand(binding, with_syntax=True)
        if recipes is not None:
            for syntax, recipe in recipes:
                element = self.state.use(recipe, syntax, scope=binding)
                element = RescopingBinding(element, binding, element.syntax)
                elements.append(element)
        else:
            elements.append(binding)
        # Validate types of the kernel expressions.
        kernels = []
        for element in elements:
            domain = coerce(element.domain)
            with translate_guard(element):
                if domain is None:
                    raise Error("Expected a scalar column")
            kernel = ImplicitCastBinding(element, domain, element.syntax)
            kernels.append(kernel)
        # Generate the quotient scope.
        quotient = QuotientBinding(self.state.scope, seed, kernels,
                                   self.syntax)
        # Assign names to the kernel and the complement links when possible.
        binding = quotient
        name = guess_tag(seed)
        if name is not None:
            recipe = ComplementRecipe(quotient)
            recipe = ClosedRecipe(recipe)
            binding = DefineBinding(binding, name, None, recipe, self.syntax)
        for index, kernel in enumerate(kernels):
            name = guess_tag(kernel)
            if name is not None:
                recipe = KernelRecipe(quotient, index)
                recipe = ClosedRecipe(recipe)
                binding = DefineBinding(binding, name, None, recipe,
                                        self.syntax)
        return binding


class BindFilter(Bind):

    adapt(FilterSyntax)

    def __call__(self):
        # Get the sieve base.
        base = self.state.bind(self.syntax.larm)
        # Bind the filter and force the Boolean type on it.
        filter = self.state.bind(self.syntax.rarm, scope=base)
        filter = ImplicitCastBinding(filter, coerce(BooleanDomain()),
                                     filter.syntax)
        # Produce a sieve scope.
        return SieveBinding(base, filter, self.syntax)


class BindLink(Bind):

    adapt(LinkSyntax)

    def __call__(self):
        # Bind the origin images.
        origin_images = []
        binding = self.state.bind(self.syntax.larm)
        recipes = expand(binding, with_syntax=True)
        if recipes is not None:
            for syntax, recipe in recipes:
                element = self.state.use(recipe, syntax)
                element = RescopingBinding(element, binding, element.syntax)
                origin_images.append(element)
        else:
            origin_images.append(binding)
        # Bind the target scope.
        home = HomeBinding(self.state.scope, self.syntax)
        seed = self.state.bind(self.syntax.rarm, scope=home)
        # Bind the target images; if not provided, reuse the syntax node
        # of the origin images.
        binding = seed
        target_images = []
        recipes = expand(seed, with_syntax=True)
        if recipes is None:
            binding = self.state.bind(self.syntax.larm, scope=seed)
            recipes = expand(binding, with_syntax=True)
        if recipes is not None:
            for syntax, recipe in recipes:
                element = self.state.use(recipe, syntax, scope=seed)
                element = RescopingBinding(element, binding, element.syntax)
                target_images.append(element)
        else:
            target_images.append(binding)
        # Correlate origin and target images.
        if len(origin_images) != len(target_images):
            raise Error("Found unbalanced origin and target columns")
        images = []
        for origin_image, target_image in zip(origin_images, target_images):
            domain = coerce(origin_image.domain, target_image.domain)
            if domain is None:
                raise Error("Cannot coerce origin and target columns"
                            " to a common type")
            origin_image = ImplicitCastBinding(origin_image, domain,
                                               origin_image.syntax)
            target_image = ImplicitCastBinding(target_image, domain,
                                               target_image.syntax)
            images.append((origin_image, target_image))
        # Generate a link scope.
        return AttachBinding(self.state.scope, seed, images, None, self.syntax)


class BindAttach(Bind):

    adapt(AttachSyntax)

    def __call__(self):
        home = HomeBinding(self.state.scope, self.syntax)
        seed = self.state.bind(self.syntax.rarm, scope=home)
        recipe = BindingRecipe(seed)
        scope = self.state.scope
        scope = DefineLiftBinding(scope, recipe, self.syntax)
        name = guess_tag(seed)
        if name is not None:
            scope = DefineBinding(scope, name, None, recipe, self.syntax)
        condition = self.state.bind(self.syntax.larm, scope=scope)
        condition = ImplicitCastBinding(condition, coerce(BooleanDomain()),
                                        condition.syntax)
        return AttachBinding(self.state.scope, seed, [], condition, self.syntax)


class BindDetach(Bind):

    adapt(DetachSyntax)

    def __call__(self):
        # Make the home scope.
        home = HomeBinding(self.state.scope, self.syntax)
        # Bind the operand against the home scope.
        return self.state.bind(self.syntax.arm, scope=home)


class BindAssign(Bind):

    adapt(AssignSyntax)

    def __call__(self):
        # Parse the left side of the assignment.  It takes one of the forms:
        #   $reference := ...
        #   identifier := ...
        #   identifier(parameter,...) := ...
        #   parent. ... .identifier(parameter,...) := ...
        #   parent. ... .$identifier(parameter,...) := ...

        # The dot-separated names and reference indicators.
        terms = []
        parameters = None
        syntax = self.syntax.larm
        for idx, arm in enumerate(syntax.larms):
            if isinstance(arm, ReferenceSyntax):
                with translate_guard(arm):
                    if idx < len(syntax.larms)-1:
                        raise Error("Expected an identifier")
                terms.append((arm.identifier.name, True))
            else:
                terms.append((arm.name, False))
        if syntax.rarms is not None:
            parameters = []
            for arm in syntax.rarms:
                if isinstance(arm, ReferenceSyntax):
                    parameters.append((arm.identifier.name, True))
                else:
                    parameters.append((arm.name, False))
        # The right side of the assignment expression.
        body = self.syntax.rarm
        # Generate an assignment node.
        return AssignmentBinding(self.state.scope, terms, parameters, body,
                                 self.syntax)


class BindCompose(Bind):

    adapt(ComposeSyntax)

    def __call__(self):
        # Expression:
        #   parent . child
        # evaluates `child` in the scope of `parent`.
        scope = self.state.bind(self.syntax.larm)
        binding = self.state.bind(self.syntax.rarm, scope=scope)
        return binding


class BindLocate(Bind):

    adapt(LocateSyntax)

    def __call__(self):
        seed = self.state.bind(self.syntax.larm)
        recipe = identify(seed)
        with translate_guard(seed):
            if recipe is None:
                raise Error("Cannot determine identity")
        identity = self.state.use(recipe, self.syntax.rarm, scope=seed)
        location = self.state.bind(self.syntax.rarm, scope=seed)
        with translate_guard(self.syntax.rarm):
            if identity.domain.width != location.width:
                raise Error("Found ill-formed locator")
        def convert(identity, elements):
            assert isinstance(identity, IdentityBinding)
            images = []
            for field in identity.elements:
                if isinstance(field.domain, IdentityDomain):
                    total_width = 0
                    items = []
                    while total_width < field.domain.width:
                        assert elements
                        element = elements.pop(0)
                        if (total_width == 0 and
                                isinstance(element, IdentityBinding) and
                                element.width == field.domain.width):
                            items = element.elements[:]
                            total_width = element.width
                        elif isinstance(element, IdentityBinding):
                            items.append(element)
                            total_width += element.width
                        else:
                            items.append(element)
                            total_width += 1
                    with translate_guard(self.syntax.rarm):
                        if total_width > field.domain.width:
                            raise Error("Found ill-formed locator")
                    images.extend(convert(field, items))
                else:
                    assert elements
                    element = elements.pop(0)
                    with translate_guard(self.syntax.larm):
                        if isinstance(element, IdentityBinding):
                            raise Error("Found ill-formed locator")
                    item = ImplicitCastBinding(element, field.domain,
                                               element.syntax)
                    images.append((item, field))
            return images
        elements = location.elements[:]
        while len(elements) == 1 and isinstance(elements[0], IdentityBinding):
            elements = elements[0].elements[:]
        images = convert(identity, elements)
        return LocateBinding(self.state.scope, seed, images, None, self.syntax)


class BindIdentity(Bind):

    adapt(IdentitySyntax)

    def __call__(self):
        elements = []
        for arm in self.syntax.arms:
            element = self.state.bind(arm)
            identity = unwrap(element, IdentityBinding, is_deep=False)
            if identity is not None:
                element = identity
            elements.append(element)
        return IdentityBinding(self.state.scope, elements, self.syntax)


class BindGroup(Bind):

    adapt(GroupSyntax)

    def __call__(self):
        # Bind the expression in parenthesis, then wrap the result
        # to attach the original syntax node.
        binding = self.state.bind(self.syntax.arm)
        return WrappingBinding(binding, self.syntax)


class BindIdentifier(Bind):

    adapt(IdentifierSyntax)

    def __call__(self):
        # Look for the identifier in the current lookup scope.
        recipe = lookup_attribute(self.state.scope, self.syntax.name)
        if recipe is not None:
            binding = self.state.use(recipe, self.syntax)
        # If not found, try the global scope.
        else:
            binding = self.state.call(self.syntax)
        return binding


class BindUnpack(Bind):

    adapt(UnpackSyntax)

    def __call__(self):
        # Get all public columns in the current lookup scope.
        recipes = expand(self.state.scope, with_syntax=True, with_wild=True,
                         with_class=True, with_link=True)
        if recipes is None:
            raise Error("Cannot expand '*' since output columns"
                        " are not defined")
        # If a position is given, extract a specific element.
        if self.syntax.index is not None:
            index = self.syntax.index
            index -= 1
            if not (0 <= index < len(recipes)):
                raise Error("Expected value in range 1-%s" % len(recipes))
            syntax, recipe = recipes[index]
            syntax = point(syntax, self.syntax)
            return self.state.use(recipe, syntax)
        # Otherwise, generate a selection node.
        elements = []
        for syntax, recipe in recipes:
            syntax = point(syntax, self.syntax)
            element = self.state.use(recipe, syntax)
            elements.append(element)
        fields = [decorate(element) for element in elements]
        domain = RecordDomain(fields)
        return WildSelectionBinding(self.state.scope, elements, domain,
                                    self.syntax)


class BindDirect(Bind):

    adapt(DirectSyntax)

    def __call__(self):
        base = self.state.bind(self.syntax.arm)
        direction = {'+': +1, '-': -1}[self.syntax.symbol]
        return DirectionBinding(base, direction, self.syntax)


class BindReference(Bind):

    adapt(ReferenceSyntax)

    def __call__(self):
        # Look for a reference, complain if not found.
        recipe = lookup_reference(self.state.scope,
                                  self.syntax.identifier.name)
        if recipe is None:
            model = self.syntax.identifier.name.lower()
            names = lookup_reference_set(self.state.scope)
            choices = ["$"+name for name in sorted(names)
                                 if similar(model, name)]
            with choices_guard(choices):
                raise Error("Found unknown reference", self.syntax)
        return self.state.use(recipe, self.syntax)


class BindLift(Bind):

    adapt(LiftSyntax)

    def __call__(self):
        # Look for a complement, complain if not found.
        recipe = lookup_complement(self.state.scope)
        if recipe is None:
            raise Error("'^' could only be used in a quotient scope")
        return self.state.use(recipe, self.syntax)


class BindString(Bind):

    adapt_many(StringSyntax,
               LabelSyntax)

    def __call__(self):
        # Bind a quoted literal.  Note that a quoted literal not necessarily
        # represents a string value; its initial domain is untyped.
        binding = LiteralBinding(self.state.scope,
                                 self.syntax.text,
                                 UntypedDomain(),
                                 self.syntax)
        return binding


class BindNumber(Bind):

    adapt(NumberSyntax)

    def __call__(self):
        # Bind an unquoted (numeric) literal.

        # Create an untyped literal binding.
        binding = LiteralBinding(self.state.scope,
                                 self.syntax.text,
                                 UntypedDomain(),
                                 self.syntax)

        # Cast the binding to an appropriate numeric type.
        if self.syntax.is_float:
            domain = coerce(FloatDomain())
        elif self.syntax.is_decimal:
            domain = coerce(DecimalDomain())
        elif self.syntax.is_integer:
            domain = coerce(IntegerDomain())
        binding = ImplicitCastBinding(binding, domain, self.syntax)
        return binding


class BindByName(Protocol):
    """
    Binds a application node.

    This is an abstract protocol interface that provides a mechanism
    for name-based dispatch of application syntax nodes.

    The :class:`BindByName` interface has the following signature::

        BindByName: (ApplicationSyntax, BindingState) -> Binding
        BindByName: (IdentifierSyntax, BindingState) -> Binding

    The protocol is polymorphic on the name and the number of arguments
    of the syntax node.

    To add an implementation of the interface, define a subclass
    of :class:`BindByName` and specify its name and expected number
    of arguments using function :func:`call`.

    Class attributes:

    `names` (a list of names or pairs `(name, length)`)
        List of names the component matches.

        Here `name` is a non-empty string, `length` is an integer or
        ``None``, where ``-1`` indicates any number of arguments, ``None``
        means no arguments are accepted.
    """

    names = []

    @classmethod
    def __dominates__(component, other):
        # Determine if the component dominates another component
        # assuming that they match the same dispatch key.

        # A component implementing a protocol interface dominates
        # another component if one of the following two conditions
        # holds:

        # (1) The component is a subclass of the other component.
        if issubclass(component, other):
            return True

        # (2) The component and the other component match the
        # same name, but the former requires a fixed number of
        # arguments while the latter accepts a node with any
        # number of arguments.
        for name in component.__names__:
            arity = -1
            if isinstance(name, tuple):
                name, arity = name
            name = name.lower()
            for other_name in other.__names__:
                other_arity = -1
                if isinstance(other_name, tuple):
                    other_name, other_arity = other_name
                other_name = other_name.lower()
                if name == other_name:
                    if arity != -1 and other_arity == -1:
                        return True

        return False

    @classmethod
    def __matches__(component, dispatch_key):
        # Check if the component matches the given function name
        # and the number of arguments.
        assert isinstance(dispatch_key, tupleof(str, maybe(int)))

        # The name and the number of arguments of the call node.
        key_name, key_arity = dispatch_key
        # We want to compare names case insensitive.  Unfortunately,
        # we cannot use `normalize` from `htsql.core.tr.lookup` since it
        # mangles symbols.
        key_name = key_name.lower()

        # Check if any of the component names matches the given name.
        for name in component.__names__:
            # `name` could be either a string or a pair of a string
            # and an integer.  The former assumes that the component
            # accepts call nodes with any number of arguments.
            arity = -1
            if isinstance(name, tuple):
                name, arity = name
            name = name.lower()
            # Check if the component name matches the node name.
            if name == key_name:
                if ((arity == key_arity) or
                        (arity == -1 and key_arity is not None)):
                    return True

        # None of the names matched the dispatch key.
        return False

    @classmethod
    def __dispatch__(interface, syntax, *args, **kwds):
        assert isinstance(syntax, (ApplySyntax, IdentifierSyntax))
        # We override `dispatch` since, as opposed to regular protocol
        # interfaces, we also want to take into account not only the
        # function name, but also the number of arguments.
        if isinstance(syntax, ApplySyntax):
            name = syntax.name
            arity = len(syntax.arguments)
        elif isinstance(syntax, IdentifierSyntax):
            name = syntax.name
            arity = None
        return (name, arity)

    def __init__(self, syntax, state):
        assert isinstance(syntax, (ApplySyntax, IdentifierSyntax))
        assert isinstance(state, BindingState)
        self.syntax = syntax
        self.state = state
        # Extract commonly accessed attributes of the call node.
        if isinstance(syntax, ApplySyntax):
            self.name = syntax.name
            self.arguments = syntax.arguments
        elif isinstance(syntax, IdentifierSyntax):
            self.name = syntax.name
            self.arguments = None

    def __call__(self):
        # The default implementation; override in subclasses.
        # Generate a hint with a list of alternative names.
        model = self.name.lower()
        arity = None
        if self.arguments is not None:
            arity = len(self.arguments)
        attributes = lookup_attribute_set(self.state.scope)
        global_attributes = set()
        for component_name in BindByName.__catalogue__():
            component_arity = -1
            if isinstance(component_name, tuple):
                component_name, component_arity = component_name
            if isinstance(component_name, str):
                component_name = component_name.decode('utf-8')
            component_name = component_name.lower()
            global_attributes.add((component_name, component_arity))
        all_attributes = sorted(attributes|global_attributes)
        choices = []
        if not choices and arity is None:
            names = lookup_reference_set(self.state.scope)
            if model in names:
                choices = ["a reference '$%s'" % model.encode('utf-8')]
        if not choices and arity is None:
            if any(model == sample
                   for sample, sample_arity in all_attributes
                   if sample_arity is not None):
                choices = ["a function '%s'" % model.encode('utf-8')]
        if not choices and arity is None:
            choices = [sample
                       for sample, sample_arity in all_attributes
                       if sample_arity is None and sample != model
                            and similar(model, sample)]
        if not choices and arity is not None \
                and not isinstance(self.syntax, OperatorSyntax):
            arities = [sample_arity
                       for sample, sample_arity in all_attributes
                       if sample == model and
                            sample_arity not in [None, -1, arity]]
            if arities:
                required_arity = []
                arities.sort()
                if len(arities) == 1:
                    required_arity.append(str(arities[0]))
                else:
                    required_arity.append(", ".join(str(sample_arity)
                                    for sample_arity in arities[:-1]))
                    required_arity.append("or")
                    required_arity.append(str(arities[-1]))
                if required_arity[-1] == "1":
                    required_arity.append("argument")
                else:
                    required_arity.append("arguments")
                required_arity = " ".join(required_arity)
                raise Error("Function '%s' requires %s; got %s"
                            % (self.syntax.identifier,
                               required_arity, arity))
        if not choices and arity is not None:
            if any(model == sample
                   for sample, sample_arity in all_attributes
                   if sample_arity is None):
                choices = ["an attribute '%s'" % model.encode('utf-8')]
        if not choices and arity is not None:
            choices = [sample
                       for sample, sample_arity in all_attributes
                       if sample_arity in [-1, arity] and sample != model
                            and similar(model, sample)]
        scope_name = guess_tag(self.state.scope)
        if scope_name is not None:
            scope_name = scope_name.encode('utf-8')
        with choices_guard(choices):
            if isinstance(self.syntax, (FunctionSyntax, PipeSyntax)):
                raise Error("Found unknown function",
                            self.syntax.identifier)
            if isinstance(self.syntax, OperatorSyntax):
                raise Error("Found unknown operator",
                            self.syntax.symbol)
            if isinstance(self.syntax, PrefixSyntax):
                raise Error("Found unknown unary operator",
                            self.syntax.symbol)
            if isinstance(self.syntax, IdentifierSyntax):
                raise Error("Found unknown attribute",
                            "%s.%s" % (scope_name, self.syntax)
                            if scope_name is not None else str(self.syntax))


class BindByRecipe(Adapter):
    """
    Applies a recipe to generate a binding node.

    This is an abstract adapter that generates new binding nodes
    from binding recipes.  The :class:`BindByRecipe` interface
    has the following signature::

        BindByRecipe: (Recipe, Syntax, BindingState) -> Binding

    The adapter is polymorphic by the first argument.

    `recipe` (:class:`htsql.core.tr.binding.Recipe`)
        A recipe to apply.

    `syntax` (:class:`htsql.core.tr.syntax.Syntax`)
        The syntax node associated with the recipe.

    `state` (:class:`BindingState`)
        The current binding state.
    """

    adapt(Recipe)

    def __init__(self, recipe, syntax, state):
        assert isinstance(recipe, Recipe)
        assert isinstance(syntax, Syntax)
        assert isinstance(state, BindingState)
        self.recipe = recipe
        self.syntax = syntax
        self.state = state

    def __call__(self):
        # The default implementation should not be reachable.
        raise Error("unable to bind a node")


class BindByLiteral(BindByRecipe):

    adapt(LiteralRecipe)

    def __call__(self):
        return LiteralBinding(self.state.scope,
                              self.recipe.value,
                              self.recipe.domain,
                              self.syntax)


class BindBySelection(BindByRecipe):

    adapt(SelectionRecipe)

    def __call__(self):
        elements = []
        for recipe in self.recipe.recipes:
            element = self.state.use(recipe, self.syntax)
            elements.append(element)
        fields = [decorate(element) for element in elements]
        domain = RecordDomain(fields)
        return SelectionBinding(self.state.scope, elements, domain, self.syntax)


class BindByFreeTable(BindByRecipe):

    adapt(FreeTableRecipe)

    def __call__(self):
        # Produce a free table scope.
        return TableBinding(self.state.scope,
                            self.recipe.table,
                            self.syntax)


class BindByAttachedTable(BindByRecipe):

    adapt(AttachedTableRecipe)

    def __call__(self):
        return ChainBinding(self.state.scope, self.recipe.joins, self.syntax)


class BindByColumn(BindByRecipe):

    adapt(ColumnRecipe)

    def __call__(self):
        # Generate a link associated with the column.
        link = None
        if self.recipe.link is not None:
            link = self.state.use(self.recipe.link, self.syntax)
        # Produce a column scope.
        return ColumnBinding(self.state.scope, self.recipe.column,
                             link, self.syntax)


class BindByKernel(BindByRecipe):

    adapt(KernelRecipe)

    def __call__(self):
        # Generate a kernel expression of a quotient scope.
        return KernelBinding(self.state.scope, self.recipe.quotient,
                             self.recipe.index, self.syntax)


class BindByComplement(BindByRecipe):

    adapt(ComplementRecipe)

    def __call__(self):
        # Generate a complement link to a quotient scope.
        return ComplementBinding(self.state.scope,
                                 self.recipe.quotient, self.syntax)


class BindByIdentity(BindByRecipe):

    adapt(IdentityRecipe)

    def __call__(self):
        elements = [self.state.use(recipe, self.syntax)
                    for recipe in self.recipe.elements]
        return IdentityBinding(self.state.scope, elements, self.syntax)


class BindBySubstitution(BindByRecipe):

    adapt(SubstitutionRecipe)

    def __call__(self):
        # Bind the given syntax node in place of an identifier
        # or a function call.

        # Check if the recipe has a qualifier.
        if self.recipe.terms:
            # Find the same identifier in the base scope.
            assert isinstance(self.syntax, IdentifierSyntax)
            name, is_reference = self.recipe.terms[0]
            arity = None
            if (len(self.recipe.terms) == 1 and
                    self.recipe.parameters is not None):
                arity = len(self.recipe.parameters)
            recipe = lookup_attribute(self.recipe.base, self.syntax.name)
            if recipe is None:
                raise Error("Found unknown attribute", self.syntax)
            binding = self.state.use(recipe, self.syntax)
            # Check if the term is a reference.
            if is_reference:
                # Must the the last term in the assignment.
                assert len(self.recipe.terms) == 1
                # Bind the reference against the scope where it is defined.
                body = self.state.bind(self.recipe.body, scope=binding)
                recipe = BindingRecipe(body)
            # Augment the scope with the tail of the recipe.
            else:
                recipe = SubstitutionRecipe(binding, self.recipe.terms[1:],
                                            self.recipe.parameters,
                                            self.recipe.body)
            recipe = ClosedRecipe(recipe)
            if is_reference:
                binding = DefineReferenceBinding(binding, name,
                                                 recipe, self.syntax)
            else:
                binding = DefineBinding(binding, name, arity,
                                        recipe, self.syntax)
            return binding

        # Otherwise, bind the syntax node associated with the recipe.
        # Bind against the current scope, but route all lookup requests
        # to the scope where the recipe was defined.
        scope = self.state.scope
        scope = RerouteBinding(scope, self.recipe.base, scope.syntax)
        # Bind the parameters.
        if self.recipe.parameters is not None:
            assert isinstance(self.syntax, ApplySyntax)
            assert len(self.syntax.arguments) == len(self.recipe.parameters)
            for (name, is_reference), syntax in zip(self.recipe.parameters,
                                                    self.syntax.arguments):
                binding = self.state.bind(syntax)
                recipe = BindingRecipe(binding)
                recipe = ClosedRecipe(recipe)
                if is_reference:
                    scope = DefineReferenceBinding(scope, name,
                                                   recipe, scope.syntax)
                else:
                    scope = DefineBinding(scope, name, None,
                                          recipe, scope.syntax)
        # Bind the syntax node associated with the recipe.
        binding = self.state.bind(self.recipe.body, scope=scope)
        # Hide all referenced defined there.
        binding = ReferenceRerouteBinding(binding, self.state.scope,
                                          binding.syntax)
        return binding


class BindByBinding(BindByRecipe):

    adapt(BindingRecipe)

    def __call__(self):
        return self.recipe.binding


class BindByClosed(BindByRecipe):

    adapt(ClosedRecipe)

    def __call__(self):
        # Generate a binding from the given recipe.
        binding = self.state.use(self.recipe.recipe, self.syntax)
        # Force the current syntax node to the binding.
        return AliasBinding(binding, self.syntax)


class BindByChain(BindByRecipe):

    adapt(ChainRecipe)

    def __call__(self):
        binding = self.state.scope
        for recipe in self.recipe.recipes:
            binding = self.state.use(recipe, self.syntax, scope=binding)
        return binding


class BindByPinned(BindByRecipe):

    adapt(PinnedRecipe)

    def __call__(self):
        # Bind the given recipe in the specified scope.
        binding = self.state.use(self.recipe.recipe, self.syntax,
                                 scope=self.recipe.scope)
        return binding


class BindByAmbiguous(BindByRecipe):

    adapt(AmbiguousRecipe)

    def __call__(self):
        syntax = self.syntax
        if isinstance(self.syntax, (FunctionSyntax, PipeSyntax)):
            syntax = self.syntax.identifier
        int = None
        choices = []
        if self.recipe.alternatives:
            choices = [str(alternative)
                       for alternative in self.recipe.alternatives]
        with choices_guard(choices):
            raise Error("Found ambiguous name", syntax)


def bind(syntax, environment=None):
    recipes = []
    if environment is not None:
        for name in sorted(environment):
            value = environment[name]
            if value.data is None:
                recipe = LiteralRecipe(value.data, value.domain)
            elif isinstance(value.domain, ListDomain):
                item_recipes = [LiteralRecipe(item,
                                              value.domain.item_domain)
                                for item in value.data]
                recipe = SelectionRecipe(item_recipes)
            elif isinstance(value.domain, RecordDomain):
                item_recipes = [LiteralRecipe(item, profile.domain)
                                for item, profile in
                                    zip(value.data, value.domain.fields)]
                recipe = SelectionRecipe(item_recipes)
            elif isinstance(value.domain, IdentityDomain):
                def convert(domain, data):
                    items = []
                    for element, item in zip(domain.labels, data):
                        if isinstance(element, IdentityDomain):
                            item = convert(element, item)
                        else:
                            item = LiteralRecipe(item, element)
                        items.append(item)
                    return IdentityRecipe(items)
                recipe = convert(value.domain, value.data)
            else:
                recipe = LiteralRecipe(value.data, value.domain)
            recipes.append((name, recipe))
    root = RootBinding(syntax)
    state = BindingState(root, recipes)
    if isinstance(syntax, AssignSyntax):
        specifier = syntax.larm
        with translate_guard(specifier):
            if specifier.identifier is None:
                raise Error("Expected an identifier")
        identifier = specifier.larms[0]
        binding = state.bind(syntax.rarm)
        binding = Select.__invoke__(binding, state)
        binding = TitleBinding(binding, identifier, binding.syntax)
    else:
        binding = state.bind(syntax)
        binding = Select.__invoke__(binding, state)
    return binding


