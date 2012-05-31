#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql.core.adapter`
=========================

This module provides a mechanism for pluggable extensions.
"""


from .util import listof, aresubclasses, toposort
from .context import context
import sys
import types


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

    # Augment method names with prefix `<name>.` to make the adapter
    # name visible in tracebacks.
    class __metaclass__(type):

        def __new__(mcls, name, bases, content):
            # Iterate over all values in the class namespace.
            for value in content.values():
                # Ignore non-function attributes.
                if not isinstance(value, types.FunctionType):
                    continue
                # Update the code name and regenerate the code object.
                code = value.func_code
                code_name = code.co_name
                if '.' in code_name:
                    continue
                code_name = '%s.%s' % (name, code_name)
                code = types.CodeType(code.co_argcount, code.co_nlocals,
                                      code.co_stacksize, code.co_flags,
                                      code.co_code, code.co_consts,
                                      code.co_names, code.co_varnames,
                                      code.co_filename, code_name,
                                      code.co_firstlineno, code.co_lnotab,
                                      code.co_freevars, code.co_cellvars)
                # Patch the function object.
                value.func_code = code
            # Create the class.
            return type.__new__(mcls, name, bases, content)

    @staticmethod
    def __components__():
        """
        Produce a list of all components of the active application.
        """
        # Get the component registry of the active application.
        registry = context.app.component_registry
        # A shortcut: return cached components.
        if registry.components is not None:
            return registry.components
        # A list of `Component` subclasses defined in modules exported by addons.
        components = [Component]
        idx = 0
        while idx < len(components):
            for subclass in components[idx].__subclasses__():
                # Skip realizations.
                if issubclass(subclass, Realization):
                    continue
                # Check if the component belongs to the current application.
                if subclass.__enabled__():
                    components.append(subclass)
            idx += 1
        # Cache and return the components.
        registry.components = components
        return components

    @classmethod
    def __implementations__(interface):
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
        components = interface.__components__()
        # Leave only components implementing the interface.
        implementations = [component
                           for component in components
                           if component.__implements__(interface)]
        # Cache and return the implementations.
        registry.implementations[interface] = implementations
        return implementations

    @classmethod
    def __realize__(interface, dispatch_key):
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
        implementations = interface.__implementations__()
        # Leave only implementations matching the dispatch key.
        implementations = [implementation
                           for implementation in implementations
                           if implementation.__matches__(dispatch_key)]
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
                if dominating.__dominates__(dominated):
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
                '__interface__': interface,
                '__dispatch_key__': dispatch_key,
        }
        # Generate the realization.
        realization = type(name, bases, attributes)

        # Cache and return the realization.
        registry.realizations[interface, dispatch_key] = realization
        return realization

    @classmethod
    def __enabled__(component):
        """
        Tests if the component is a part of the current application.
        """
        registry = context.app.component_registry
        return (component.__module__ in registry.modules)

    @classmethod
    def __implements__(component, interface):
        """
        Tests if the component implements the interface.
        """
        return issubclass(component, interface)

    @classmethod
    def __dominates__(component, other):
        """
        Tests if the component dominates another component.
        """
        # Refine in subclasses.
        return issubclass(component, other)

    @classmethod
    def __matches__(component, dispatch_key):
        """
        Tests if the component matches a dispatch key.
        """
        # Override in subclasses.
        return False

    @classmethod
    def __dispatch__(interface, *args, **kwds):
        """
        Extracts the dispatch key from the constructor arguments.
        """
        # Override in subclasses.
        return None

    @classmethod
    def __prepare__(interface, *args, **kwds):
        """
        Instantiates the interface to the given arguments.
        """
        # Extract polymorphic parameters.
        dispatch_key = interface.__dispatch__(*args, **kwds)
        # Realize the interface.
        realization = interface.__realize__(dispatch_key)
        # Instantiate and return the realization.
        return realization(*args, **kwds)

    @classmethod
    def __invoke__(interface, *args, **kwds):
        """
        Realizes and applies the interface to the given arguments.

        Use ``__prepare__()()`` instead when traversing a deeply nested tree.
        """
        # Extract polymorphic parameters.
        dispatch_key = interface.__dispatch__(*args, **kwds)
        # Realize the interface.
        realization = interface.__realize__(dispatch_key)
        # Instantiate and call the realization.
        instance = realization(*args, **kwds)
        return instance()

    def __new__(interface, *args, **kwds):
        # Only realizations are permitted to instantiate.
        assert False

    @classmethod
    def __call__(self):
        """
        Executes the implementation.
        """
        raise NotImplementedError("interface %s is not implemented for: %r"
                                  % (self.__interface__.__name__,
                                     self.__dispatch_key__))


class Realization(Component):
    """
    A realization of an interface for some dispatch key.
    """

    __interface__ = None
    __dispatch_key__ = None

    def __new__(cls, *args, **kwds):
        # Allow realizations to instantiate.
        return object.__new__(cls)


class Utility(Component):
    """
    Provides utility interfaces.

    An utility is an interface with a single realization.

    This is an abstract class; to declare an utility interface, create
    a subclass of :class:`Utility`.  To add an implementation of the
    interface, create a subclass of the interface class.

    Class attributes:

    `__rank__` (a number)
        The relative weight of the component relative to the other
        components implementing the same utility.

    The following example declared an interface ``SayHello`` and provide
    an implementation ``PrintHello`` that prints ``'Hello, World!`` to
    the standard output::

        class SayHello(Utility):
            def __call__(self):
                raise NotImplementedError("interface is not implemented")

        class PrintHello(SayHello):
            def __call__(self):
                print "Hello, World!"

        hello = SayHello.__invoke__

        >>> hello()
        Hello, World!
    """

    __rank__ = 0.0

    @classmethod
    def __dominates__(component, other):
        if issubclass(component, other):
            return True
        if component.__rank__ > other.__rank__:
            return True
        return False

    @classmethod
    def __matches__(component, dispatch_key):
        # For an utility, the dispatch key is always a 0-tuple.
        assert dispatch_key == ()
        return True

    @classmethod
    def __dispatch__(interface, *args, **kwds):
        # The dispatch key is always a 0-tuple.
        return ()


def rank(value):
    assert isinstance(value, (int, float))
    frame = sys._getframe(1)
    frame.f_locals['__rank__'] = value


class Adapter(Component):
    """
    Provides adapter interfaces.

    An adapter interface provides mechanism for polymorphic dispatch
    based on the types of the arguments.

    This is an abstract class; to declare an adapter interface, create
    a subclass of :class:`Adapter` and indicate the most generic type
    signature of the polymorphic arguments using function :func:`adapt`.

    To add an implementation of an adapter interface, create a subclass
    of the interface class and indicate the matching type signatures
    using functions :func:`adapt`, :func:`adapt_many`, or
    :func:`adapt_none`.

    Class attributes:

    `__types__` (a list of type signatures)
        List of signatures that the component matches.
    
    `__arity__` (an integer)
        Number of polymorphic arguments.

    The following example declares an adapter interface ``Format``
    and implements it for several data types::

        class Format(Adapter):
            adapt(object)
            def __init__(self, value):
                self.value = value
            def __call__(self):
                # The default implementation.
                return str(self.value)

        class FormatString(Format):
            adapt(str)
            def __call__(self):
                # Display alphanumeric values unquoted, the others quoted.
                if self.value.isalnum():
                    return self.value
                else:
                    return repr(self.value)

        class FormatList(Format):
            adapt(list)
            def __call__(self):
                # Apply `format` to the list elements.
                return "[%s]" % ",".join(format(item) for item in self.value)

        format = Format.__invoke__

        >>> print format(123)
        123
        >>> print format("ABC")
        ABC
        >>> print format("Hello, World!")
        'Hello, World!'
        >>> print format([123, "ABC", "Hello, World!"])
        [123, ABC, 'Hello, World!']
    """

    __types__ = []
    __arity__ = 0

    @classmethod
    def __dominates__(component, other):
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
        # Correctness of the ordering is verified in `Component.__realize()__`.
        for type_vector in component.__types__:
            for other_type_vector in other.__types__:
                if aresubclasses(type_vector, other_type_vector):
                    if type_vector != other_type_vector:
                        return True

        return False

    @classmethod
    def __matches__(component, dispatch_key):
        # For an adapter interface, the dispatch key is a signature.
        # A component matches the dispatch key the component signature
        # is equal or less specific than the dispatch key.
        # Note: if the component has more than one signature, it
        # matches the dispatch key if at least one of its signatures
        # is equal or less specific than the dispatch key.
        assert isinstance(list(dispatch_key), listof(type))
        return any(aresubclasses(dispatch_key, type_vector)
                   for type_vector in component.__types__)

    @classmethod
    def __dispatch__(interface, *args, **kwds):
        # The types of the leading arguments of the constructor
        # form a dispatch key.
        assert interface.__arity__ <= len(args)
        type_vector = tuple(type(arg) for arg in args[:interface.__arity__])
        return type_vector


def adapt(*type_vector):
    """
    Specifies the adapter signature.

    The component matches the specified or any more specific
    signature.

    Use it in the namespace of the component, for example::

        class DoSmth(Adapter):

            adapt(T1, T2, ...)
    """
    assert isinstance(list(type_vector), listof(type))
    frame = sys._getframe(1)
    frame.f_locals['__types__'] = [type_vector]
    frame.f_locals['__arity__'] = len(type_vector)


def adapt_none():
    """
    Indicates that the adapter does not match any signatures.

    Use it in the namespace of the adapter, for example::

        class DoSmth(Adapter):

            adapt_none()
    """
    frame = sys._getframe(1)
    frame.f_locals['__types__'] = []


def adapt_many(*type_vectors):
    """
    Specifies signatures of the adapter.

    The component matches any of the specified signatures as well
    all more specific signatures.

    Use it in the namespace of the adapter, for example::

        class DoSmth(Adapter):

            adapt_many((T11, T12, ...),
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
    frame.f_locals['__types__'] = type_vectors
    frame.f_locals['__arity__'] = arity


class Protocol(Component):
    """
    Implements protocol interfaces.

    A protocol interface provides mechanism for name-based dispatch.

    This is an abstract class; to declare a protocol interface, create
    a subclass of :class:`Protocol`.

    To add an implementation of a protocol interface, create a subclass
    of the interface class and specify its name using function :func:`call`.

    Class attributes:

    `__names__` (a list of strings)
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
            call("Alice")
            def __call__(self):
                return 150

        class WeighBob(Weigh):
            call("Bob")
            def __call__(self):
                return 160

        weigh = Weigh.__invoke__

        >>> weigh("Alice")
        150
        >>> weigh("Bob")
        160
        >>> weigh("Clark")
        -1
    """

    __names__ = []

    @classmethod
    def __dispatch__(interface, name, *args, **kwds):
        # The first argument of the constructor is the protocol name.
        return name

    @classmethod
    def __matches__(component, dispatch_key):
        # The dispatch key is the protocol name.
        assert isinstance(dispatch_key, str)
        return (dispatch_key in component.__names__)

    @classmethod
    def __catalogue__(interface):
        """
        Returns all names assigned to protocol implementations.
        """
        names = []
        duplicates = set()
        for component in interface.__implementations__():
            for name in component.__names__:
                if name not in duplicates:
                    names.append(name)
                    duplicates.add(name)
        names.sort()
        return names


def call(*names):
    """
    Specifies the names of the protocol.

    Use it in the namespace of the protocol, for example::

        class DoSmth(Protocol):

            call("...")
    """
    frame = sys._getframe(1)
    frame.f_locals['__names__'] = list(names)


class ComponentRegistry(object):
    """
    Contains cached components and realizations.
    """

    def __init__(self, addons):
        # Packages exported by addons.
        packages = set()
        for addon in addons:
            # In Python 2.6+:
            # root_package = sys.modules[addon.__module__].__package__
            root_package = addon.__module__
            if not hasattr(sys.modules[root_package], '__path__'):
                root_package = root_package.rsplit('.', 1)[0]
            # An addon exports packages defined in `packages` attribute.
            for package in addon.packages:
                # Resolve relative package names.
                if package == '.':
                    package = root_package
                elif package.startswith('.'):
                    package = root_package+package
                packages.add(package)
        # All modules exported by the addons.
        modules = set()
        for module in sorted(sys.modules):
            # In Python 2.6+:
            # package = sys.modules[module].__package__
            package = module
            if not hasattr(sys.modules[package], '__path__'):
                package = package.rsplit('.', 1)[0]
            if package in packages:
                modules.add(module)
        self.modules = modules
        # List of active components (populated by
        # `Component.__components__()`).
        self.components = None
        # A mapping: interface -> [components]  (populated by
        # `Component.__implementations__()`).
        self.implementations = {}
        # A mapping: (interface, dispatch_key) -> realization (populated by
        # `Component.__realize__()`).
        self.realizations = {}


