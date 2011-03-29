#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.bind`
====================

This module implements the binding process.
"""


from ..util import tupleof
from ..adapter import Adapter, Protocol, adapts
from ..domain import (BooleanDomain, IntegerDomain, DecimalDomain,
                      FloatDomain, UntypedDomain)
from .error import BindError
from .syntax import (Syntax, QuerySyntax, SegmentSyntax, SelectorSyntax,
                     SieveSyntax, CallSyntax, OperatorSyntax,
                     FunctionOperatorSyntax, FunctionCallSyntax, GroupSyntax,
                     SpecifierSyntax, IdentifierSyntax, WildcardSyntax,
                     ComplementSyntax, StringSyntax, NumberSyntax)
from .recipe import (Recipe, FreeTableRecipe, AttachedTableRecipe,
                     ColumnRecipe, ComplementRecipe, KernelRecipe,
                     SubstitutionRecipe, BindingRecipe, AmbiguousRecipe)
from .binding import (Binding, RootBinding, QueryBinding, SegmentBinding,
                      LiteralBinding, SieveBinding, CastBinding,
                      WrapperBinding, FreeTableBinding, AttachedTableBinding,
                      ColumnBinding, ComplementBinding, KernelBinding,
                      DefinitionBinding, RedirectBinding, AliasBinding)
from .lookup import lookup, itemize, get_kernel, get_complement, get_function
from .coerce import coerce


class BindingState(object):
    """
    Encapsulates the (mutable) state of the binding process.

    State attributes:

    `root` (:class:`htsql.tr.binding.RootBinding`)

    `base` (:class:`htsql.tr.binding.Binding`)
        The current lookup context.
    """

    def __init__(self):
        # The root lookup context.
        self.root = None
        # The current lookup context.
        self.base = None
        # The stack of previous lookup contexts.
        self.base_stack = []

    def set_root(self, root):
        """
        Sets the root lookup context.

        This function initializes the lookup context stack and must be
        called before any calls of :meth:`push_base` and :meth:`pop_base`.

        `root` (:class:`htsql.tr.binding.RootBinding`)
            The root lookup context.
        """
        # Check that the lookup stack is not initialized.
        assert self.root is None
        assert self.base is None
        assert isinstance(root, RootBinding)
        self.root = root
        self.base = root

    def flush(self):
        """
        Clears the lookup context.
        """
        # We expect the lookup context stack being empty and the current
        # context to coincide with the root context.
        assert self.root is not None
        assert not self.base_stack
        assert self.root is self.base
        self.root = None
        self.base = None

    def push_base(self, base):
        """
        Sets the new lookup context.

        This function stores the current context in the stack and makes
        the given binding `base` the new lookup context.  Use the
        :attr:`base` attribute to get the current context; :meth:`pop_base`
        to restore the previous context.

        `base` (:class:`htsql.tr.binding.Binding`)
            The new lookup context.
        """
        # Sanity check on the argument.
        assert isinstance(base, Binding)
        # Ensure that the root context was set.
        assert self.root is not None
        # Save the current lookup context.
        self.base_stack.append(self.base)
        # Assign the new lookup context.
        self.base = base

    def pop_base(self):
        """
        Restores the previous lookup context.

        This functions restores the previous lookup context from the stack.
        Use the :attr:`base` attribute to get the current context;
        :meth:`push_base` to change the current context.
        """
        # Restore the prevous lookup context from the stack.
        self.base = self.base_stack.pop()

    def bind_all(self, syntax, base=None):
        """
        Binds the given syntax node using the current binding state.

        Returns a list of generated binding nodes.

        `syntax` (:class:`htsql.tr.syntax.Syntax`)
            The syntax node to bind.

        `base` (:class:`htsql.tr.binding.Binding` or ``None``)
            If set, the lookup context is set to `base` when
            binding the syntax node.
        """
        return bind_all(syntax, self, base)

    def bind(self, syntax, base=None):
        """
        Binds the given syntax node using the current binding state.

        Returns a binding node.  This function raises an error if none
        or more than one node are produced.

        `syntax` (:class:`htsql.tr.syntax.Syntax`)
            The syntax node to bind.

        `base` (:class:`htsql.tr.binding.Binding` or ``None``)
            If set, the lookup context is set to `base` when
            binding the syntax node.
        """
        return bind(syntax, self, base)

    def call(self, syntax, base=None):
        """
        Binds the given function call node using the current binding state.

        Returns a list of binding nodes.

        `syntax` (:class:`htsql.tr.syntax.CallSyntax`)
            The syntax node to bind.

        `base` (:class:`htsql.tr.binding.Binding` or ``None``)
            If set, the lookup context is set to `base` when
            binding the syntax node.
        """
        # If passed, set the new lookup context.
        if base is not None:
            self.push_base(base)
        # First, try to find the function in the local context.
        recipe = get_function
        # Otherwise, look for the global function.
        # Realize and apply `BindByName` protocol.
        bind = BindByName(syntax, self)
        bindings = list(bind())
        # Restore the old lookup context.
        if base is not None:
            self.pop_base()
        # Return the generated binding nodes.
        return bindings


class Bind(Adapter):
    """
    Translates a syntax node to a sequence of binding nodes.

    This is an interface adapter; see subclasses for implementations.

    The binding process translates a syntax tree to a binding tree.  The
    primary purpose of binding is to resolve identifiers against database
    objects, resolve an validate function and operator calls and determine
    types of all expressions.

    The :class:`Bind` adapter has the following signature::

        Bind: (Syntax, BindingState) -> (Binding ...)

    The adapter is polymorphic on the `Syntax` argument.

    `syntax` (:class:`htsql.tr.syntax.Syntax`)
        The syntax node to bind.

    `state` (:class:`BindingState`)
        The current state of the binding process.
    """

    adapts(Syntax)

    def __init__(self, syntax, state):
        assert isinstance(syntax, Syntax)
        assert isinstance(state, BindingState)
        self.syntax = syntax
        self.state = state

    def __call__(self):
        # The default implementation raises an error.  It is actually
        # unreachable since we provide an implementation for all syntax nodes.
        raise BindError("unable to bind a node", self.syntax.mark)


class BindQuery(Bind):
    """
    Binds the top-level syntax node :class:`htsql.tr.syntax.QuerySyntax`.

    Produces a :class:`htsql.tr.binding.QueryBinding` node.
    """

    adapts(QuerySyntax)

    def __call__(self):
        # Initialize the lookup context stack with a root context, which
        # represents a scalar context with `lookup` implemented as table
        # lookup.
        root = RootBinding(self.syntax)
        self.state.set_root(root)
        # Bind the segment node if it is available.
        segment = None
        if self.syntax.segment is not None:
            segment = self.state.bind(self.syntax.segment)
        # Shut down the lookup context stack.
        self.state.flush()
        # Construct and return the top-level binding node.
        yield QueryBinding(root, segment, self.syntax)


class BindSegment(Bind):
    """
    Binds a :class:`htsql.tr.syntax.SegmentSyntax` node.

    Produces a :class:`htsql.tr.binding.SegmentBinding` node.
    """

    adapts(SegmentSyntax)

    def __call__(self):
        # To construct a segment binding, we determine its base and its
        # elements.  Note that the selector node has the form:
        #   /base{selector}?filter
        # (where the selector and the filter nodes are optional) or
        #   /{selector}
        # Typically the base and the filter nodes are bound to construct
        # the segment base and the selector is bound to construct the segment
        # elements.

        # If the syntax node has the form:
        #   /{selector}
        # we use the current lookup context as the segment base.
        base = self.state.base
        # Othewise, for queries `/base{selector}?filter` and `/base{selector}`
        # we bind the nodes `(base?filter)` and `base` respectively
        # to get the segment base.
        if self.syntax.base is not None:
            if self.syntax.filter is not None:
                base_syntax = SieveSyntax(self.syntax.base, None,
                                          self.syntax.filter, self.syntax.mark)
                base_syntax = GroupSyntax(base_syntax, self.syntax.mark)
                base = self.state.bind(base_syntax)
            else:
                base = self.state.bind(self.syntax.base)
        # Bind the selector against the base to get the segment elements.
        if self.syntax.selector is not None:
            bare_elements = self.state.bind_all(self.syntax.selector, base)
        else:
            # No selector means that the segment has the form:
            #   /base   or   /base?filter
            # This is a special case: depending on whether the base is
            # enumerable, it is interpreted either as
            #   /base{*}
            # or as
            #   /{base}
            bare_elements = []
            bare_group = itemize(base)
            if bare_group is None:
                bare_elements = [base]
                base = self.state.base
            else:
                self.state.push_base(base)
                for syntax, recipe in bare_group:
                    syntax = syntax.clone(mark=base.mark)
                    bind = BindByRecipe(recipe, syntax, self.state)
                    bare_elements.extend(bind())
                self.state.pop_base()
        # Validate and specialize the domains of the elements.
        elements = []
        for element in bare_elements:
            domain = coerce(element.domain)
            if domain is None:
                raise BindError("invalid element type", element.mark)
            element = CastBinding(element, domain, element.syntax)
            elements.append(element)
        # Currently we disallow an empty selector.
        if not elements:
            raise BindError("empty selector", self.syntax.mark)
        # Generate a segment binding.
        yield SegmentBinding(base, elements, self.syntax)


class BindSelector(Bind):
    """
    Binds a :class:`htsql.tr.syntax.SelectorSyntax` node.

    Produces a sequence (possibly empty) of binding nodes.
    """

    adapts(SelectorSyntax)

    def __call__(self):
        # The selector node has the form:
        #   {element,...}
        # We iterate over the elements to bind them.
        for element in self.syntax.elements:
            for binding in self.state.bind_all(element):
                yield binding


class BindSieve(Bind):
    """
    Bind a :class:`htsql.tr.syntax.SieveSyntax` node.

    Produces a sequence (possibly empty) of binding nodes.
    """

    adapts(SieveSyntax)

    def __call__(self):
        # A sieve node admits one of two forms.  The first form
        #   /base?filer
        # produces a sieve binding node.  The second form
        #   /base{selector}?filter
        # generates a sieve binding node and uses it as a lookup
        # context when binding the selector.  The binding nodes
        # produced by the selector are returned.

        if self.syntax.selector is None:
            # Handle the case `/base?filter`: generate and return
            # a sieve binding.
            base = self.state.bind(self.syntax.base)
            # Note: this condition is always satisfied.
            if self.syntax.filter is not None:
                filter = self.state.bind(self.syntax.filter, base)
                filter = CastBinding(filter, coerce(BooleanDomain()),
                                     filter.syntax)
                base = SieveBinding(base, filter, self.syntax)
            yield base

        else:
            # Handle the cases `/base{selector}` and `/base{selector}?filter`:
            # bind and return the selector using `base` or `base?filter` as
            # the lookup context.

            # Generate the new lookup context.
            if self.syntax.filter is not None:
                # Generate a new syntax node `(base?filter)` and bind it.
                base_syntax = SieveSyntax(self.syntax.base, None,
                                          self.syntax.filter, self.syntax.mark)
                base_syntax = GroupSyntax(base_syntax, self.syntax.mark)
                base = self.state.bind(base_syntax)
            else:
                base = self.state.bind(self.syntax.base)

            # Bind and return the selector.
            for binding in self.state.bind_all(self.syntax.selector, base):
                # Wrap the binding nodes to change the associated syntax node.
                # We replace `element` with `base{element}`.
                selector_syntax = SelectorSyntax([binding.syntax],
                                                 binding.mark)
                binding_syntax = SieveSyntax(self.syntax.base,
                                             selector_syntax,
                                             self.syntax.filter,
                                             binding.mark)
                binding = WrapperBinding(binding, binding_syntax)
                yield binding


class BindOperator(Bind):
    """
    Binds an :class:`htsql.tr.syntax.OperatorSyntax` node.
    """

    adapts(OperatorSyntax)

    def __call__(self):
        # The operator node has one of the forms:
        #   <lop><symbol><rop>, <lop><symbol>, <symbol><rop>.

        # Find and bind the operator.
        return self.state.call(self.syntax)


class BindFunctionOperator(Bind):
    """
    Binds a :class:`htsql.tr.syntax.FunctionOperatorSyntax` node.
    """

    adapts(FunctionOperatorSyntax)

    def __call__(self):
        # A function operator node has the form:
        #   <lop> <identifier> <rop>

        # Find and bind the function.
        recipe = get_function(self.state.base, self.syntax.identifier,
                              len(self.syntax.arguments))
        if recipe is not None:
            bind = BindByRecipe(recipe, self.syntax, self.state)
            bindings = list(bind())
        else:
            bindings = self.state.call(self.syntax)
        return bindings


class BindFunctionCall(Bind):
    """
    Binds a :class:`htsql.tr.syntax.FunctionCallSyntax` node.
    """

    adapts(FunctionCallSyntax)

    def __call__(self):
        # A function call has one of the forms:
        #   `identifier(argument,...)` or `base.identifier(argument,...)`.
        # When `base` is set, it is used as the lookup context when binding
        # the function and its arguments.

        # Get the lookup context of the function.
        base = self.state.base
        if self.syntax.base is not None:
            base = self.state.bind(self.syntax.base)
        # Find and bind the function.
        self.state.push_base(base)
        recipe = get_function(base, self.syntax.identifier,
                              len(self.syntax.arguments))
        if recipe is not None:
            bind = BindByRecipe(recipe, self.syntax, self.state)
            bindings = list(bind())
        else:
            bindings = self.state.call(self.syntax, base)
        self.state.pop_base()
        return bindings


class BindByName(Protocol):
    """
    Binds a call node.

    This is an abstract protocol interface that provides a mechanism
    for name-based dispatch of call syntax nodes.

    The :class:`BindByName` interface has the following signature::

        BindByName: (CallSyntax, BindingState) -> listof(Binding)

    The protocol is polymorphic on `name` and `len(arguments)`, where
    `name` and `arguments` are attributes of the call node.

    To add an implementation of the interface, define a subclass
    of :class:`BindByName` and specify its name and expected number
    of arguments using function :func:`named`.

    For more implementations of the interface, see :mod:`htsql.tr.fn.bind`.

    Class attributes:

    `names` (a list of names or pairs `(name, length)`)
        List of names the component matches.

        Here `name` is a non-empty string, `length` is an integer or
        ``None``.
    """

    names = []

    @classmethod
    def dominates(component, other):
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
        for name in component.names:
            arity = None
            if isinstance(name, tuple):
                name, arity = name
            name = name.lower()
            for other_name in other.names:
                other_arity = None
                if isinstance(other_name, tuple):
                    other_name, other_arity = other_name
                other_name = other_name.lower()
                if name == other_name:
                    if arity is not None and other_arity is None:
                        return True

        return False

    @classmethod
    def matches(component, dispatch_key):
        # Check if the component matches the given function name
        # and the number of arguments.
        assert isinstance(dispatch_key, tupleof(str, int))

        # The name and the number of arguments of the call node.
        key_name, key_arity = dispatch_key
        # We want to compare names case insensitive.  Unfortunately,
        # we cannot use `normalize` from `htsql.tr.lookup` since it
        # mangles symbols.
        key_name = key_name.lower()

        # Check if any of the component names matches the given name.
        for name in component.names:
            # `name` could be either a string or a pair of a string
            # and an integer.  The former assumes that the component
            # accepts call nodes with any number of arguments.
            arity = None
            if isinstance(name, tuple):
                name, arity = name
            name = name.lower()
            # Check if the component name matches the node name.
            if name == key_name:
                if arity is None or arity == key_arity:
                    return True

        # None of the names matched the dispatch key.
        return False

    @classmethod
    def dispatch(interface, syntax, *args, **kwds):
        assert isinstance(syntax, CallSyntax)
        # We override `dispatch` since, as opposed to regular protocol
        # interfaces, we also want to take into account not only the
        # function name, but also the number of arguments.
        return (syntax.name, len(syntax.arguments))

    def __init__(self, syntax, state):
        assert isinstance(syntax, CallSyntax)
        assert isinstance(state, BindingState)
        self.syntax = syntax
        self.state = state
        # Extract commonly accessed attributes of the call node.
        self.name = syntax.name
        self.arguments = syntax.arguments

    def __call__(self):
        # The default implementation; override in subclasses.
        raise BindError("unknown function %s" % self.name,
                        self.syntax.mark)


class BindGroup(Bind):
    """
    Binds a :class:`htsql.tr.syntax.GroupSyntax` node.
    """

    adapts(GroupSyntax)

    def __call__(self):
        # A group node has the form:
        #   ( expression )

        # Bind the expression and wrap the result to add parentheses
        # around the syntax node.
        for binding in self.state.bind_all(self.syntax.expression):
            binding_syntax = GroupSyntax(binding.syntax, binding.mark)
            binding = WrapperBinding(binding, binding_syntax)
            yield binding


class BindSpecifier(Bind):
    """
    Binds a :class:`htsql.tr.syntax.SpecifierSyntax` node.
    """

    adapts(SpecifierSyntax)

    def __call__(self):
        # A specifier node has the form:
        #   `base.identifier` or `base.*`

        # Bind `base` and use it as the lookup context when binding
        # the identifier.
        base = self.state.bind(self.syntax.base)
        for binding in self.state.bind_all(self.syntax.identifier, base):
            # Wrap the binding node to update the associated syntax node.
            # Note `identifier` is replaced with `base.identifier`.
            # FIXME: Will fail if `binding.syntax` is not an identifier
            # or a wildcard node.  Currently, `binding.syntax` is always
            # an identifier node, but that might change in the future.
            binding_syntax = binding.syntax
            if isinstance(binding_syntax, IdentifierSyntax):
                binding_syntax = SpecifierSyntax(base.syntax, binding_syntax,
                                                 binding.mark)
                binding = WrapperBinding(binding, binding_syntax)
            yield binding


class BindIdentifier(Bind):
    """
    Binds an :class:`htsql.tr.syntax.IdentifierSyntax` node.
    """

    adapts(IdentifierSyntax)

    def __call__(self):
        # Look for the identifier in the current lookup context.
        recipe = lookup(self.state.base, self.syntax)
        if recipe is None:
            raise BindError("unable to resolve an identifier",
                            self.syntax.mark)
        bind = BindByRecipe(recipe, self.syntax, self.state)
        return bind()


class BindWildcard(Bind):
    """
    Binds a :class:`htsql.tr.syntax.WildcardSyntax` node.
    """

    adapts(WildcardSyntax)

    def __call__(self):
        # Get all public descendants in the current lookup context.
        group = itemize(self.state.base)
        if group is None:
            raise BindError("unable to resolve a wildcard",
                            self.syntax.mark)
        for syntax, recipe in group:
            syntax = syntax.clone(mark=self.syntax.mark)
            bind = BindByRecipe(recipe, syntax, self.state)
            for binding in bind():
                yield binding


class BindComplement(Bind):
    """
    Bind a :class:`htsql.tr.syntax.ComplementSyntax` node.
    """

    adapts(ComplementSyntax)

    def __call__(self):
        recipe = get_complement(self.state.base)
        if recipe is None:
            raise BindError("expected a quotient context", self.syntax.mark)
        bind = BindByRecipe(recipe, self.syntax, self.state)
        return bind()


class BindString(Bind):
    """
    Binds a :class:`htsql.tr.syntax.StringSyntax` node.
    """

    adapts(StringSyntax)

    def __call__(self):
        # Bind a quoted literal.  Note that a quoted literal not necessarily
        # represents a string value; its initial domain is untyped.
        binding = LiteralBinding(self.syntax.value,
                                 UntypedDomain(),
                                 self.syntax)
        yield binding


class BindNumber(Bind):
    """
    Binds a :class:`htsql.tr.syntax.NumberSyntax` node.
    """

    adapts(NumberSyntax)

    def __call__(self):
        # Bind an unquoted (numeric) literal.

        # Create an untyped literal binding.
        binding = LiteralBinding(self.syntax.value,
                                 UntypedDomain(),
                                 self.syntax)

        # Cast the binding to an appropriate numeric type.
        value = self.syntax.value
        # If the literal uses the exponential notation, assume it's
        # a float number.
        if 'e' in value or 'E' in value:
            domain = coerce(FloatDomain())
        # If the literal uses the decimal notation, assume it's
        # a decimal number.
        elif '.' in value:
            domain = coerce(DecimalDomain())
        # Otherwise, it's an integer.
        else:
            domain = coerce(IntegerDomain())
        binding = CastBinding(binding, domain, self.syntax)
        yield binding


class BindByRecipe(Adapter):

    adapts(Recipe)

    def __init__(self, recipe, syntax, state):
        assert isinstance(recipe, Recipe)
        assert isinstance(syntax, Syntax)
        assert isinstance(state, BindingState)
        self.recipe = recipe
        self.syntax = syntax
        self.state = state

    def __call__(self):
        raise BindError("unable to bind a node", self.syntax.mark)


class BindByFreeTable(BindByRecipe):

    adapts(FreeTableRecipe)

    def __call__(self):
        yield FreeTableBinding(self.state.base, self.recipe.table,
                               self.syntax)


class BindByAttachedTable(BindByRecipe):

    adapts(AttachedTableRecipe)

    def __call__(self):
        binding = self.state.base
        for join in self.recipe.joins:
            binding = AttachedTableBinding(binding, join, self.syntax)
        yield binding


class BindByColumn(BindByRecipe):

    adapts(ColumnRecipe)

    def __call__(self):
        link = None
        if self.recipe.link is not None:
            bind = BindByRecipe(self.recipe.link, self.syntax, self.state)
            links = list(bind())
            if len(links) != 1:
                raise BindError("unexpected selector or wildcard expression",
                                syntax.mark)
            link = links[0]
        yield ColumnBinding(self.state.base, self.recipe.column,
                            link, self.syntax)


class BindByComplement(BindByRecipe):

    adapts(ComplementRecipe)

    def __call__(self):
        syntax = self.recipe.seed.syntax.clone(mark=self.syntax.mark)
        yield ComplementBinding(self.state.base, self.recipe.seed, syntax)


class BindByKernel(BindByRecipe):

    adapts(KernelRecipe)

    def __call__(self):
        binding = self.recipe.kernel[self.recipe.index]
        syntax = binding.syntax.clone(mark=self.syntax.mark)
        yield KernelBinding(self.state.base, self.recipe.index,
                            binding.domain, syntax)


class BindBySubstitution(BindByRecipe):

    adapts(SubstitutionRecipe)

    def __call__(self):
        if self.recipe.subnames:
            assert isinstance(self.syntax, IdentifierSyntax)
            recipe = lookup(self.recipe.base, self.syntax)
            if recipe is None:
                raise BindError("unable to resolve an identifier",
                                self.syntax.mark)
            bind = BindByRecipe(recipe, self.syntax, self.state)
            bindings = list(bind())
            if len(bindings) != 1:
                raise BindError("unexpected selector or wildcard expression",
                                syntax.mark)
            binding = bindings[0]
            binding = DefinitionBinding(binding, self.recipe.subnames[0],
                                        self.recipe.subnames[1:],
                                        self.recipe.arguments,
                                        self.recipe.body,
                                        binding.syntax)
            yield binding
            return
        base = RedirectBinding(self.state.base, self.recipe.base,
                               self.state.base.syntax)
        if self.recipe.arguments is not None:
            assert isinstance(self.syntax, CallSyntax)
            assert len(self.syntax.arguments) == len(self.recipe.arguments)
            for name, syntax in zip(self.recipe.arguments,
                                    self.syntax.arguments):
                binding = self.state.bind(syntax)
                base = AliasBinding(base, name, binding, base.syntax)
        for binding in self.state.bind_all(self.recipe.body, base=base):
            binding = WrapperBinding(binding, self.syntax)
            yield binding


class BindByBinding(BindByRecipe):

    adapts(BindingRecipe)

    def __call__(self):
        yield self.recipe.binding


class BindByAmbiguous(BindByRecipe):

    adapts(AmbiguousRecipe)

    def __call__(self):
        raise BindError("ambiguous name", self.syntax.mark)


def bind_all(syntax, state=None, base=None):
    """
    Binds the given syntax node.

    Returns a list of generated binding nodes.

    `syntax` (:class:`htsql.tr.syntax.Syntax`)
        The syntax node to bind.

    `state` (:class:`BindingState` or ``None``).
        The binding state to use.  If not set, a new binding state
        is created.

    `base` (:class:`htsql.tr.binding.Binding` or ``None``)
        If set, the lookup context is set to `base` when
        binding the node.
    """
    # Create a new binding state if necessary.
    if state is None:
        state = BindingState()
    # If passed, set the new lookup context.
    if base is not None:
        state.push_base(base)
    # Realize and apply the `Bind` adapter.
    bind = Bind(syntax, state)
    bindings = list(bind())
    # Restore the old lookup context.
    if base is not None:
        state.pop_base()
    # Return the binding nodes.
    return bindings


def bind(syntax, state=None, base=None):
    """
    Binds the given syntax node.

    Returns a binding node.  This function raises an error if no nodes
    or more than one node are produced.

    `syntax` (:class:`htsql.tr.syntax.Syntax`)
        The syntax node to bind.

    `state` (:class:`BindingState` or ``None``).
        The binding state to use.  If not set, a new binding state
        is created.

    `base` (:class:`htsql.tr.binding.Binding` or ``None``)
        If set, the lookup context is set to `base` when binding
        the node.
    """
    # Bind the syntax node.
    bindings = bind_all(syntax, state, base)
    # Ensure we got exactly one binding node back.
    if len(bindings) != 1:
        raise BindError("unexpected selector or wildcard expression",
                        syntax.mark)
    return bindings[0]


