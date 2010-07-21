#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.adapter`
====================

This module provides a mechanism for pluggable multiple dispatch.
"""


from .util import listof, subclassof, aresubclasses, toposort
from .context import context
import sys


class Adapter(object):
    """
    Implements a polymorphic class.

    Recall that a *polymorphic function* is a set of functions
    with the same name and the number of arguments.  When a polymorphic
    function is called, the implementation is chosen based on the
    types of the arguments.

    :class:`Adapter` implements the concept of polymorphic functions.

    A *signature* is a tuple which elements are Python classes or types.
    The length of the tuple is called the signature *arity*.

    Given two signatures of the same arity, we say that the first
    signature is more *specific* than the second signature if each
    type in the first signature is a subclass of the corresponding
    type in the second signature.  In this case, we may also say
    that the second signature is *compatible* with the first signature.
    
    Note that specificity establishes a partial order relation on
    the sets of signatures of the same arity.

    An *adapter* is a class with an associated signature.
    
    An adapter must be a subclass of :class:`Adapter`.  The adapter
    signature indicates the types of the constructor arguments.
    
    An utility is an adapter with an empty signature.  That is,
    the utility signature has the arity equal to `0`.
    
    An utility should be a subclass of :class:`Utility`.

    Suppose we defined a base adapter that declares some methods
    and a group of its subclasses implementing the methods.  In this
    case, the base adapter is called an *interface*, and its subclasses
    are called *implementations* of the interface.  All implementation
    signatures must have the same arity and the interface signature
    must be equal to or less specific that the implementation signatures.

    Note that the specificity relation on signatures induces
    a partial order relation on interfaces.  Given two implementations,
    we say that the first implementation dominates the second one if
    the signature of the first implementation is more specific than
    the signature of the second implementation.  However it is not
    forbidden to have two or more implementations with the same signature.
    In fact, all utilities have the same signature.  To handle the case
    when signatures cannot establish dominancy, we permit specifying
    dominancy relation explicitly.

    Now assume that given an interface and a list of arguments,
    we need to construct a class implementing the interface for the
    given arguments.  Such class is called a *realization* of
    the interface.  A realization is class that inherits from of all
    implementations which signatures are compatible with the arguments.
    The implementations are ordered according to dominancy relation.

    :class:`Adapter` is an abstract class; to add a new interface
    or an implementation, create a subclass of :class:`Adapter`
    and override the following class attributes:

    `signature` (a tuple of types)
        The adapter signature.

        You may also set the signature using the :func:`adapts` function.

    `weight` (an integer or a float number)
        The weight of the adapter.

        The adapter weight establishes dominancy between implementations
        with the same signature.  A "heavier" adapter dominates a "lighter"
        adapter.

        You may also set the weight using the :func:`weights` function.

    `dominated_adapters` (a list of :class:`Adapter` subclasses)
        The list of implementations dominated by the adapter.

        You may also set this parameter using the :func:`dominates` function.

    `dominating_adapters` (a list of :class:`Adapter` subclasses)
        The list of implementations dominating the adapter.

        You may also set this parameter using the :func:`dominated_by`
        function.
    """

    # Override in subclasses.
    signature = None
    weight = 1
    dominated_adapters = []
    dominating_adapters = []

    # If set, indicates that the class was generated as a realization.
    is_realized = False

    def __new__(cls, *args, **kwds):
        """
        Adapts the interface for the given arguments.
        """
        # Bypass realizations.
        if cls.is_realized:
            return super(Adapter, cls).__new__(cls)
        # Extract polymorphic parameters.
        assert cls.signature is not None and len(args) >= len(cls.signature)
        objects = args[:len(cls.signature)]
        # Specialize the interface for the given parameters.
        realization = cls.realize(*objects)
        # Create an instance of the realization.
        return super(Adapter, realization).__new__(realization)

    @classmethod
    def realize(cls, *objects):
        """
        Builds a realization for the given polymorphic parameters.
        """
        # Determine the signature.
        signature = tuple(type(obj) for obj in objects)
        # Get the active HTSQL application.
        app = context.app
        # Build the realization for the given signature.
        return app.adapter_registry.specialize(cls, signature)


class Utility(Adapter):
    """
    Implements an adapter with an empty signature.
    """

    signature = ()


class AdapterRegistry(object):
    """
    Contains all adapters used by an HTSQL application.

    `adapters` (a list of :class:`Adapter` subclasses)
        List of active adapters.
    """

    def __init__(self, adapters):
        # Sanity check on the argument.
        assert isinstance(adapters, listof(subclassof(Adapter)))
        # List of active adapters.
        self.adapters = adapters
        # A mapping: interface -> (signature -> realization).
        # It is populated lazily by `specialize()`.
        self.realizations = {}

    def specialize(self, interface, signature):
        """
        Produces a realization of an interface for the given signature.

        `interface` (a subclass of :class:`Adapter`)
            The interface to adapt.

        `signature` (a tuple of types)
            The types of the polymorphic parameters.
        """
        # Build the mapping signature -> realization for the given interface.
        if interface not in self.realizations:
            self.populate_interface(interface)
        realization_by_signature = self.realizations[interface]

        # Find the best realization for the given signature.
        if signature not in realization_by_signature:
            self.populate_signature(interface, signature)
        realization = realization_by_signature[signature]

        return realization

    def populate_interface(self, interface):
        # For the given interface, build the mapping:
        #   signature -> realization.

        # Sanity check on the interface.  Check if it is, indeed, an adapter.
        assert isinstance(interface, subclassof(Adapter))
        # Check if the interface is active in the current application.
        assert interface in self.adapters
        # Check that the adapter signature is defined.
        assert interface.signature is not None

        # Find implementations for the given interface among active adapters.
        implementations = [adapter for adapter in self.adapters
                                   if issubclass(adapter, interface)
                                   and adapter.signature is not None]

        # Sanity check on the implementations.
        for adapter in implementations:
            # Check if the signature is valid.
            assert adapter.signature is not None
            assert len(adapter.signature) == len(interface.signature)
            assert aresubclasses(adapter.signature, interface.signature)
            # Check if the dominated and dominating adapters are
            # specified correctly.
            assert all(dominated in implementations
                       for dominated in adapter.dominated_adapters)
            assert all(dominating in implementations
                       for dominating in adapter.dominating_adapters)

        # Build a mapping: master -> [slave, ...], where `master` dominates
        # its `slave` adapters.  `dominates` establishes a partial preorder
        # relation on the set of implementations.
        dominates = {}
        # A loop over potential masters.
        for master in implementations:
            # A list of adapters dominated by `master`.
            dominates[master] = []
            # The interface adapter never dominates its implementations.
            if master is interface:
                continue
            # A loop over potential slaves.
            for slave in implementations:
                if master is slave:
                    continue
                # Indicates if `master` dominates `slave`.
                is_dominated = False
                # A subclass always dominates its superclasses.  In particular,
                # the interface adapter is dominated by all implementations.
                if issubclass(master, slave):
                    is_dominated = True
                # Check if dominance is specified explicitly.
                if slave in master.dominated_adapters:
                    is_dominated = True
                if master in slave.dominating_adapters:
                    is_dominated = True
                # For implementations with identical signatures,
                # dominance is determined by `weight`.
                if master.signature == slave.signature:
                    if master.weight > slave.weight:
                        is_dominated = True
                # Otherwise, an adapter with a more specific signature
                # dominates an adapter with a less specific signature.
                else:
                    if aresubclasses(master.signature, slave.signature):
                        is_dominated = True
                # Update the list of adapters dominated by `master`.
                if is_dominated:
                    dominates[master].append(slave)

        # A list of signatures for which we pre-build realizations.
        signatures = []
        # The list consists of signatures of all adapters.
        for adapter in implementations:
            if adapter.signature not in signatures:
                signatures.append(adapter.signature)

        # A mapping: signature -> realization.
        realization_by_signature = {}
        # For each signature, build a realization.
        for signature in signatures:
            # The realization inherits from all implementations with
            # a compatible signature.
            bases = [adapter for adapter in implementations
                             if aresubclasses(signature, adapter.signature)]
            # Now we need to determine the correct order of the bases.
            # The order of the bases must conform the preorder established
            # by the dominance relation.  Moreover, when restricted to
            # the set of bases, the preorder relation must become total.
            # Apply topological sort to order the base according to
            # the dominance relation.  Note that `toposort` will complain
            # if the dominance is not a proper preorder (there are cycles).
            bases = toposort(bases, (lambda adapter: dominates[adapter]))
            bases = tuple(reversed(bases))
            # Check if the preorder is total.
            for idx in range(1, len(bases)):
                assert bases[idx] in dominates[bases[idx-1]]
            # Build and save the realization.
            name = bases[0].__name__
            realization = type(name, bases, {'is_realized': True})
            realization_by_signature[signature] = realization

        # Set realizations for the given interface.
        self.realizations[interface] = realization_by_signature

    def populate_signature(self, interface, signature):
        # Find a suitable realization for the given signature.

        # Sanity check on the arguments.
        assert len(interface.signature) == len(signature)
        assert aresubclasses(signature, interface.signature)

        # The mapping: signature -> realization.
        realization_by_signature = self.realizations[interface]
        # Contains compatible and most specific realization signatures.
        candidates = []
        # Loop over signatures of existing realizations.
        for candidate in realization_by_signature:
            # Skip incompatible signatures.
            if not aresubclasses(signature, candidate):
                continue
            # Skip signatures less specific than current candidates.
            if any(aresubclasses(other, candidate) for other in candidates):
                continue
            # We got a new candidate.  Now remove other candidates that
            # are less specific than the one we found.
            candidates = [other for other in candidates
                                if not aresubclasses(candidate, other)]
            # Add the candidate to the list.
            candidates.append(candidate)
        # Now `candidates` should contain the most specific realization
        # among all realizations compatible with the given signature.
        assert len(candidates) == 1
        best_candidate = candidates[0]

        # Remember the realization for the given signature.
        realization = realization_by_signature[best_candidate]
        realization_by_signature[signature] = realization


def adapts(*types):
    """
    Specifies the adapter signature.

    Use it in the namespace of the adapter, for example::

        class DoSmth(Adapter):

            adapts(T1, T2, ...)
    """
    assert isinstance(list(types), listof(type))
    frame = sys._getframe(1)
    frame.f_locals['signature'] = types


def adapts_none():
    """
    Indicates that the adapter has no signature.

    Use it in the namespace of the adapter, for example::

        class DoSmth(Adapter):

            adapts_none()
    """
    frame = sys._getframe(1)
    frame.f_locals['signature'] = None


def dominates(*adapters):
    """
    Specifies the implementations dominated by the adapter.

    Use it in the namespace of the adapter, for example::

        class DoSmth(Adapter):

            dominates(A1, A2, ...)
    """
    assert isinstance(list(adapters), listof(subclassof(Adapter)))
    frame = sys._getframe(1)
    frame.f_locals['dominated_adapters'] = adapters


def dominated_by(*adapters):
    """
    Specifies the implementations that dominate the adapter.

    Use it in the namespace of the adapter, for example::

        class DoSmth(Adapter):

            dominated_by(A1, A2, ...)
    """
    assert isinstance(list(adapters), listof(subclassof(Adapter)))
    frame = sys._getframe(1)
    frame.f_locals['dominating_adapters'] = adapters


def weights(value):
    """
    Speficies the adapter weight.

    Use it in the namespace of the adapter, for example::
    
        class DoSmth(Adapter):

            weights(1)
    """
    assert isinstance(value, (int, float))
    frame = sys._getframe(1)
    frame.f_locals['weight'] = value


def find_adapters():
    """
    Returns a list of adapters defined in the current namespace.
    """
    # We assume `frame_adapters` is called in the context of a module.
    frame = sys._getframe(1)
    # The module namespace.
    locals = frame.f_locals
    # The name of the current module.
    module_name = locals['__name__']
    # The list of adapters.
    adapters = []
    # Find subclasses of `Adapter` defined in the current module.
    for name in sorted(locals):
        obj = locals[name]
        if (isinstance(obj, type) and issubclass(obj, Adapter)
                and obj.__module__ == module_name):
            adapters.append(obj)
    return adapters


