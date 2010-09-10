#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.bind`
====================

This module implements the binding process.
"""


from ..adapter import Adapter, adapts
from ..context import context
from ..domain import (BooleanDomain, IntegerDomain, DecimalDomain,
                      FloatDomain, UntypedDomain)
from .error import BindError
from .syntax import (Syntax, QuerySyntax, SegmentSyntax, SelectorSyntax,
                     SieveSyntax, OperatorSyntax, FunctionOperatorSyntax,
                     FunctionCallSyntax, GroupSyntax, SpecifierSyntax,
                     IdentifierSyntax, WildcardSyntax, StringSyntax,
                     NumberSyntax)
from .binding import (Binding, RootBinding, QueryBinding, SegmentBinding,
                      LiteralBinding, SieveBinding, CastBinding,
                      WrapperBinding)
from .lookup import lookup, itemize
from .coerce import coerce
from .fn.function import call
import decimal


class BindingState(object):
    """
    Encapsulates the (mutable) state of the binding process.

    State attributes:

    `base` (:class:`htsql.tr.binding.Binding`)
        The current lookup context.
    """

    def __init__(self):
        # The current lookup context.
        self.base = None
        # The stack of previous lookup contexts.
        self.base_stack = []

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
        return call(syntax, self, base)


class Bind(Adapter):
    """
    Translates a syntax node to a sequence of binding nodes.

    This is an interface adapter; see subclasses for implementations.

    The binding process translates a syntax tree to a binding tree.  The
    primary purpose of binding is to resolve identifiers against database
    objects, resolve an validate function and operator calls and determine
    types of all expressions.

    The :class:`Bind` adapter has the following signature::

        Bind: (Syntax, State) -> (Binding ...)

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
        # Set the root lookup context: `RootBinding` represents a scalar
        # context with `lookup` implemented as table lookup.
        root = RootBinding(self.syntax)
        self.state.push_base(root)
        # Bind the segment node if it is available.
        segment = None
        if self.syntax.segment is not None:
            segment = self.state.bind(self.syntax.segment)
        # Restore the original lookup context.
        self.state.pop_base()
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
        # we take the current lookup context as the segment base.
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
            #   / base   or   / base ?filter
            # This is a special case: depending on whether the base is
            # enumerable, it is interpreted either as
            #   / base {*}
            # or as
            #   / {base}
            bare_elements = itemize(base, base.syntax)
            if bare_elements is None:
                bare_elements = [base]
                base = self.state.base
        # Validate and specialize the domains of the elements.
        elements = []
        for element in bare_elements:
            domain = coerce(element.domain)
            if domain is None:
                raise BindError("invalid element type", element.mark)
            element = CastBinding(element, domain, element.syntax)
            elements.append(element)
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
        return self.state.call(self.syntax)


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
        return self.state.call(self.syntax, base)


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
            binding_syntax = SpecifierSyntax(base.syntax, binding.syntax,
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
        binding = lookup(self.state.base, self.syntax)
        if binding is None:
            raise BindError("unable to resolve an identifier",
                            self.syntax.mark)
        yield binding


class BindWildcard(Bind):
    """
    Binds a :class:`htsql.tr.syntax.WildcardSyntax` node.
    """

    adapts(WildcardSyntax)

    def __call__(self):
        # Get all public descendants in the current lookup context.
        bindings = itemize(self.state.base, self.syntax)
        if bindings is None:
            raise BindError("unable to resolve a wildcard",
                            self.syntax.mark)
        for binding in bindings:
            yield binding


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


