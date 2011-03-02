#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.adapter`
====================

This module provides a mechanism for pluggable extensions.
"""


from .util import listof, aresubclasses, toposort
from .context import context
import sys


class Component(object):
    """
    A unit of extension in the HTSQL component architecture.

    *HTSQL component architecture* allows you to:

    - declare *interfaces* that provide various services;

    - define *components* implementing the interfaces;

    - given an interface and a *dispatch key*, produce a component which
      implements the interface for the given key.

    Three types of interfaces are supported: *utilities*, *adapters* and
    *protocols*; see :class:`Utility`, :class:`Adapter`, :class:`Protocol`
    respectively.
    """

    @staticmethod
    def components():
        """
        Produce a list of all components of the active application.
        """
        # Get the component registry of the active application.
        registry = context.app.component_registry
        # A shortcut: return cached components.
        if registry.components is not None:
            return registry.components
        # All modules exported by the addons of the active application.
        modules = set()
        for addon in context.app.addons:
            # An addon exports all modules defined in the same package.
            package = addon.__module__
            if '.' in package:
                package = package.rsplit('.', 1)[0]
            for module in sys.modules:
                if module == package or module.startswith(package+'.'):
                    modules.add(module)
        # A list of `Component` subclasses defined in the `modules`.
        components = [Component]
        idx = 0
        while idx < len(components):
            for subclass in components[idx].__subclasses__():
                # Skip realizations.
                if issubclass(subclass, Realization):
                    continue
                if subclass.__module__ in modules:
                    components.append(subclass)
            idx += 1
        # Cache and return the components.
        registry.components = components
        return components

    @classmethod
    def implementations(interface):
        """
        Produces a list of all components implementing the interface.
        """
        # Get the component registry of the active application.
        registry = context.app.component_registry
        # A shortcut: return cached implementations.
        try:
            return registry.implementations[interface]
        except KeyError:
            pass
        # Get all active components.
        components = interface.components()
        # Leave only components implementing the interface.
        implementations = [component
                           for component in components
                           if component.implements(interface)]
        # Cache and return the implementations.
        registry.implementations[interface] = implementations
        return implementations

    @classmethod
    def realize(interface, dispatch_key):
        """
        Produces a realization of the interface for the given dispatch key.
        """
        # Get the component registry of the active application.
        registry = context.app.component_registry

        # A shortcut: if the realization for the given interface and the
        # dispatch key is already built, return it.
        try:
            return registry.realizations[interface, dispatch_key]
        except KeyError:
            pass

        # Get the implementations of the interface.
        implementations = interface.implementations()
        # Leave only implementations matching the dispatch key.
        implementations = [implementation
                           for implementation in implementations
                           if implementation.matches(dispatch_key)]
        # Note: commented out since we force the interface component
        # to match any dispatch keys.
        ## Check that we have at least one matching implementation.
        #if not implementations:
        #    raise RuntimeError("when realizing interface %s.%s for key %r,"
        #                       " unable to find matching implementations"
        #                       % (interface.__module__, interface.__name__,
        #                          dispatch_key))

        # Generate a function:
        # order(implementation) -> [dominated implementations].
        order_graph = {}
        for dominating in implementations:
            order_graph[dominating] = []
            for dominated in implementations:
                if dominating is dominated:
                    continue
                if dominating.dominates(dominated):
                    order_graph[dominating].append(dominated)
        order = (lambda implementation: order_graph[implementation])

        # Now we need to order the implementations unambiguously.
        try:
            implementations = toposort(implementations, order, is_total=True)
        except RuntimeError, exc:
            # We intercept exceptions to provide a nicer error message.
            # `message` is an explanation we discard; `conflict` is a list
            # of implementations which either form a domination loop or
            # have no ordering relation between them.
            message, conflict = exc
            interface_name = "%s.%s" % (interface.__module__,
                                        interface.__name__)
            component_names = ", ".join("%s.%s" % (component.__module__,
                                                   component.__name__)
                                        for component in conflict)
            if conflict[0] is conflict[-1]:
                problem = "an ordering loop"
            else:
                problem = "ambiguous ordering"
            # Report a problem.
            raise RuntimeError("when realizing interface %s for key %r,"
                               " detected %s in components: %s"
                               % (interface_name, dispatch_key,
                                  problem, component_names))

        # We want the most specific implementations first.
        implementations.reverse()

        # Force the interface component to the list of implementations.
        if interface not in implementations:
            implementations.append(interface)

        # Generate the name of the realization of the form:
        #   interface[implementation1,implementation2,...]
        module = interface.__module__
        name = "%s[%s]" % (interface.__name__,
                           ",".join("%s.%s" % (component.__module__,
                                               component.__name__)
                                    for component in implementations
                                    if component is not interface))
        # Get the list of bases for the realization.
        bases = tuple([Realization] + implementations)
        # Class attributes for the realization.
        attributes = {
                '__module__': module,
                'interface': interface,
                'dispatch_key': dispatch_key,
        }
        # Generate the realization.
        realization = type(name, bases, attributes)

        # Cache and return the realization.
        registry.realizations[interface, dispatch_key] = realization
        return realization

    @classmethod
    def implements(component, interface):
        """
        Tests if the component implements the interface.
        """
        return issubclass(component, interface)

    @classmethod
    def dominates(component, other):
        """
        Tests if the component dominates another component.
        """
        # Refine in subclasses.
        return issubclass(component, other)

    @classmethod
    def matches(component, dispatch_key):
        """
        Tests if the component matches a dispatch key.
        """
        # Override in subclasses.
        return False

    @classmethod
    def dispatch(interface, *args, **kwds):
        """
        Extract the dispatch key from the constructor arguments.
        """
        # Override in subclasses.
        return None

    def __new__(interface, *args, **kwds):
        # Extract polymorphic parameters.
        dispatch_key = interface.dispatch(*args, **kwds)
        # Realize the interface.
        realization = interface.realize(dispatch_key)
        # Create an instance of the realization.
        return super(Component, realization).__new__(realization)


class Realization(Component):
    """
    A realization of an interface for some dispatch key.
    """

    interface = None
    dispatch_key = None

    def __new__(cls, *args, **kwds):
        # Bypass `Component.__new__`.
        return object.__new__(cls)


class Utility(Component):
    """
    Implements utility interfaces.

    An utility is an interface with a single realization.

    This is an abstract class; to declare an utility interface, create
    a subclass of :class:`Utility`.  To add an implementation of the
    interface, create a subclass of the interface class.

    The following example declared an interface ``SayHello`` and provide
    an implementation ``PrintHello`` that prints ``'Hello, World!`` to
    the standard output::

        class SayHello(Utility):
            def __call__(self):
                raise NotImplementedError("interface is not implemented")

        class PrintHello(SayHello):
            def __call__(self):
                print "Hello, World!"

        def hello():
            hello = SayHello()
            hello()

        >>> hello()
        Hello, World!
    """

    weight = 0.0

    @classmethod
    def dominates(component, other):
        if issubclass(component, other):
            return True
        if component.weight > other.weight:
            return True
        return False

    @classmethod
    def matches(component, dispatch_key):
        # For an utility, the dispatch key is always a 0-tuple.
        assert dispatch_key == ()
        return True

    @classmethod
    def dispatch(interface, *args, **kwds):
        # The dispatch key is always a 0-tuple.
        return ()


def weigh(value):
    assert isinstance(value, (int, float))
    frame = sys._getframe(1)
    frame.f_locals['weight'] = value


class Adapter(Component):
    """
    Implements adapter interfaces.

    An adapter interface provides mechanism for polymorphic dispatch
    based on the types of the arguments.

    This is an abstract class; to declare an adapter interface, create
    a subclass of :class:`Adapter` and indicate the most generic type
    signature of the polymorphic arguments using function :func:`adapts`.

    To add an implementation of an adapter interface, create a subclass
    of the interface class and indicate the matching type signatures
    using functions :func:`adapts`, :func:`adapts_many`, or
    :func:`adapts_none`.

    Class attributes:

    `types` (a list of type signatures)
        List of signatures that the component matches.
    
    `arity` (an integer)
        Number of polymorphic arguments.

    The following example declares an adapter interface ``Format``
    and implements it for several data types::

        class Format(Adapter):
            adapts(object)
            def __init__(self, value):
                self.value = value
            def __call__(self):
                # The default implementation.
                return str(self.value)

        class FormatString(Format):
            adapts(str)
            def __call__(self):
                # Display alphanumeric values unquoted, the others quoted.
                if self.value.isalnum():
                    return self.value
                else:
                    return repr(self.value)

        class FormatList(Format):
            adapts(list)
            def __call__(self):
                # Apply `format` to the list elements.
                return "[%s]" % ",".join(format(item) for item in self.value)

        def format(value):
            format = Format(value)
            return format()

        >>> print format(123)
        123
        >>> print format("ABC")
        ABC
        >>> print format("Hello, World!")
        'Hello, World!'
        >>> print format([123, "ABC", "Hello, World!"])
        [123, ABC, 'Hello, World!']
    """

    types = []
    arity = 0

    @classmethod
    def dominates(component, other):
        # A component implementing an adapter interface dominates
        # over another component implementing the same interface
        # if one of the following two conditions holds:
        
        # (1) The component is a subclass of the other component.
        if issubclass(component, other):
            return True

        # (2) The signature of the component is more specific than
        #     the signature of the other component.
        # Note: In case if the component has more than one signature,
        # we require that at least one of the signatures is more
        # specific than some signature of the other component.  This
        # rule does not guarantee anti-symmetricity, so ambiguously
        # defined implementations may make the ordering ill defined.
        # Validness of the ordering is verified in `Component.realize()`.
        for type_vector in component.types:
            for other_type_vector in other.types:
                if aresubclasses(type_vector, other_type_vector):
                    if type_vector != other_type_vector:
                        return True

        return False

    @classmethod
    def matches(component, dispatch_key):
        # For an adapter interface, the dispatch key is a signature.
        # A component matches the dispatch key the component signature
        # is equal or less specific than the dispatch key.
        # Note: if the component has more than one signature, it
        # matches the dispatch key if at least one of its signatures
        # is equal or less specific than the dispatch key.
        assert isinstance(list(dispatch_key), listof(type))
        return any(aresubclasses(dispatch_key, type_vector)
                   for type_vector in component.types)

    @classmethod
    def dispatch(interface, *args, **kwds):
        # The types of the leading arguments of the constructor
        # form a dispatch key.
        assert interface.arity <= len(args)
        type_vector = tuple(type(arg) for arg in args[:interface.arity])
        return type_vector


def adapts(*type_vector):
    """
    Specifies the adapter signature.

    The component matches the specified or any more specific
    signature.

    Use it in the namespace of the component, for example::

        class DoSmth(Adapter):

            adapts(T1, T2, ...)
    """
    assert isinstance(list(type_vector), listof(type))
    frame = sys._getframe(1)
    frame.f_locals['types'] = [type_vector]
    frame.f_locals['arity'] = len(type_vector)


def adapts_none():
    """
    Indicates that the adapter does not match any signatures.

    Use it in the namespace of the adapter, for example::

        class DoSmth(Adapter):

            adapts_none()
    """
    frame = sys._getframe(1)
    frame.f_locals['types'] = []


def adapts_many(*type_vectors):
    """
    Specifies signatures of the adapter.

    The component matches any of the specified signatures as well
    all more specific signatures.

    Use it in the namespace of the adapter, for example::

        class DoSmth(Adapter):

            adapts_many((T11, T12, ...),
                        (T21, T22, ...),
                        ...)
    """
    # Normalize the given type vectors.
    type_vectors = [type_vector if isinstance(type_vector, tuple)
                                else (type_vector,)
                  for type_vector in type_vectors]
    assert len(type_vectors) > 0
    arity = len(type_vectors[0])
    assert all(len(type_vector) == arity
               for type_vector in type_vectors)
    frame = sys._getframe(1)
    frame.f_locals['types'] = type_vectors
    frame.f_locals['arity'] = arity


class Protocol(Component):
    """
    Implements protocol interfaces.

    A protocol interface provides mechanism for name-based dispatch.

    This is an abstract class; to declare a protocol interface, create
    a subclass of :class:`Protocol`.

    To add an implementation of a protocol interface, create a subclass
    of the interface class and specify its name using function :func:`named`.

    Class attributes:

    `names` (a list of strings)
        List of names that the component matches.

    The following example declares a protocol interface ``Weigh``
    and adds several implementations::

        class Weigh(Protocol):
            def __init__(self, name):
                self.name = name
            def __call__(self):
                # The default implementation.
                return -1

        class WeighAlice(Weigh):
            named("Alice")
            def __call__(self):
                return 150

        class WeighBob(Weigh):
            named("Bob")
            def __call__(self):
                return 160

        def weigh(name):
            weigh = Weigh(name)
            return weigh()

        >>> weigh("Alice")
        150
        >>> weigh("Bob")
        160
        >>> weigh("Clark")
        -1
    """

    names = []

    @classmethod
    def dispatch(interface, name, *args, **kwds):
        # The first argument of the constructor is the protocol name.
        return name

    @classmethod
    def matches(component, dispatch_key):
        # The dispatch key is the protocol name.
        assert isinstance(dispatch_key, str)
        return (dispatch_key in component.names)


def named(*names):
    """
    Specifies the names of the protocol.

    Use it in the namespace of the protocol, for example::

        class DoSmth(Protocol):

            named("...")
    """
    frame = sys._getframe(1)
    frame.f_locals['names'] = list(names)


class ComponentRegistry(object):
    """
    Contains cached components and realizations.
    """

    def __init__(self):
        # List of active components (populated by `Component.component()`).
        self.components = None
        # A mapping: interface -> [components]  (populated by
        # `Component.implementations()`).
        self.implementations = {}
        # A mapping: (interface, dispatch_key) -> realization (populated by
        # `Component.realize()`).
        self.realizations = {}


