# cython: language_level=3


from types import FunctionType
from weakref import WeakValueDictionary
from htsql.core.context import context


cdef class Clonable(object):

    __slots__ = ()

    def __init__(self):
        # Must be overriden in subclasses.
        raise NotImplementedError("%s.__init__()" % self.__class__.__name__)

    def clone(self, **replacements):
        if not replacements:
            return self
        init_code = self.__init__.__func__.__code__
        names = list(init_code.co_varnames[1:init_code.co_argcount])
        assert not (init_code.co_flags & 0x04)  # CO_VARARGS
        if init_code.co_flags & 0x08:           # CO_VARKEYWORDS
            name = init_code.co_varnames[init_code.co_argcount]
            names += sorted(getattr(self, name))
        for key in sorted(replacements):
            assert key in names
        arguments = {}
        is_modified = False
        for name in names:
            value = getattr(self, name)
            if name in replacements and replacements[name] is not value:
                value = replacements[name]
                is_modified = True
            arguments[name] = value
        if not is_modified:
            return self
        clone = self.__class__(**arguments)
        return clone

    def clone_to(self, clone_type, **replacements):
        init_code = self.__init__.__func__.__code__
        names = list(init_code.co_varnames[1:init_code.co_argcount])
        assert not (init_code.co_flags & 0x04)  # CO_VARARGS
        if init_code.co_flags & 0x08:           # CO_VARKEYWORDS
            name = init_code.co_varnames[init_code.co_argcount]
            names += sorted(getattr(self, name))
        for key in sorted(replacements):
            assert key in names
        arguments = {}
        is_modified = False
        if self.__class__ is not clone_type:
            is_modified = True
        for name in names:
            value = getattr(self, name)
            if name in replacements and replacements[name] is not value:
                value = replacements[name]
                is_modified = True
            arguments[name] = value
        if not is_modified:
            return self
        clone = clone_type(**arguments)
        return clone


cdef class Hashable(object):

    cdef object __weakref__
    cdef readonly long _hash
    cdef readonly object _basis
    cdef readonly object _matches

    def __hash__(self):
        if self._hash == 0:
            self._rehash()
        return self._hash

    def __basis__(self):
        raise NotImplementedError()

    cpdef _rehash(self):
        _basis = self.__basis__()
        if isinstance(_basis, tuple):
            elements = []
            for element in _basis:
                if isinstance(element, Hashable):
                    element_class = element.__class__
                    other = <Hashable>element
                    if other._hash == 0:
                        other._rehash()
                    element_hash = other._hash
                    element_basis = other._basis
                    elements.append((element_class,
                                     element_hash,
                                     element_basis))
                else:
                    elements.append(element)
            _basis = tuple(elements)
        self._basis = _basis
        self._hash = hash(_basis)
        if self._hash == 0:
            self._hash = 1

    cpdef _matching(self, other):
        if self._matches is not None:
            try:
                if self._matches[id(other)] is other:
                    return True
            except KeyError:
                pass
        return False

    cpdef _match(self, other):
        if self._matches is None:
            self._matches = WeakValueDictionary()
        self._matches[id(other)] = other

    def __richcmp__(self, other, int op):
        if self._hash == 0:
            self._rehash()
        if isinstance(other, Hashable):
            other_hashable = <Hashable>other
            if other_hashable._hash == 0:
                other_hashable._rehash()
            if op == 0:
                return self._basis < other_hashable._basis
            elif op == 1:
                return self._basis < other_hashable._basis
            elif op == 2:
                if type(self) != type(other_hashable):
                    return False
                if self._matching(other_hashable):
                    return True
                if self._basis == other_hashable._basis:
                    self._match(other_hashable)
                    return True
                return False
            elif op == 3:
                if type(self) != type(other_hashable):
                    return True
                if self._matching(other_hashable):
                    return False
                if self._basis == other_hashable._basis:
                    self._match(other_hashable)
                    return False
                return True
            elif op == 4:
                return self._basis > other_hashable._basis
            elif op == 5:
                return self._basis > other_hashable._basis
        else:
            if op == 0:
                return self._basis < other
            elif op == 1:
                return self._basis < other
            elif op == 2:
                return False
            elif op == 3:
                return True
            elif op == 4:
                return self._basis > other
            elif op == 5:
                return self._basis >= other


cdef bint aresubclasses(subclasses, superclasses):
    if len(subclasses) != len(superclasses):
        return False
    for subclass, superclass in zip(subclasses, superclasses):
        if not issubclass(subclass, superclass):
            return False
    return True


class ComponentMeta(type):

    def __new__(mcls, name, bases, content):
        for value in list(content.values()):
            if not isinstance(value, FunctionType):
                continue
            code = value.__code__
            code_name = code.co_name
            if '.' in code_name:
                continue
            code_name = '%s.%s' % (name, code_name)
            code = code.replace(co_name=code_name)
            value.__code__ = code
        return type.__new__(mcls, name, bases, content)


class Component(metaclass=ComponentMeta):

    @staticmethod
    def __components__():
        registry = context.app.component_registry
        if registry.components is not None:
            return registry.components
        components = [Component]
        idx = 0
        while idx < len(components):
            for subclass in components[idx].__subclasses__():
                if issubclass(subclass, Realization):
                    continue
                if subclass.__enabled__():
                    components.append(subclass)
            idx += 1
        registry.components = components
        return components

    @classmethod
    def __implementations__(interface):
        registry = context.app.component_registry
        try:
            return registry.implementations[interface]
        except KeyError:
            pass
        components = interface.__components__()
        implementations = [component
                           for component in components
                           if component.__implements__(interface)]
        registry.implementations[interface] = implementations
        return implementations

    @classmethod
    def __realize__(interface, dispatch_key):
        registry = context.app.component_registry
        try:
            return registry.realizations[interface, dispatch_key]
        except KeyError:
            pass
        implementations = interface.__implementations__()
        implementations = [implementation
                           for implementation in implementations
                           if implementation.__matches__(dispatch_key)]
        order_graph = dict((implementation, [])
                           for implementation in implementations)
        for implementation in implementations:
            for challenger in implementations:
                if implementation is challenger:
                    continue
                if implementation.__dominates__(challenger):
                    order_graph[implementation].append(challenger)
                elif implementation.__follows__(challenger):
                    order_graph[challenger].append(implementation)
        order = (lambda implementation: order_graph[implementation])

        from htsql.core.util import toposort
        try:
            implementations = toposort(implementations, order, is_total=True)
        except RuntimeError, exc:
            message, conflict = exc
            interface_name = str(interface)
            component_names = ", ".join(str(component)
                                        for component in conflict)
            if conflict[0] is conflict[-1]:
                problem = "an ordering loop"
            else:
                problem = "ambiguous ordering"
            raise RuntimeError("when realizing interface %s for key %r,"
                               " detected %s in components: %s"
                               % (interface_name, dispatch_key,
                                  problem, component_names))

        implementations.reverse()
        if interface not in implementations:
            implementations.append(interface)

        module = interface.__module__
        name = "%s[%s]" % (interface.__name__,
                           ",".join(str(component)
                                    for component in implementations
                                    if component is not interface))
        bases = tuple([Realization] + implementations)
        attributes = {
                '__module__': module,
                '__interface__': interface,
                '__dispatch_key__': dispatch_key,
        }
        realization = type(name, bases, attributes)

        registry.realizations[interface, dispatch_key] = realization
        return realization

    @classmethod
    def __enabled__(component):
        module = component.__module__
        if module == 'htsql._htsql_speedups':
            return True
        registry = context.app.component_registry
        return (module in registry.modules)

    @classmethod
    def __implements__(component, interface):
        return issubclass(component, interface)

    @classmethod
    def __dominates__(component, other):
        return issubclass(component, other)

    @classmethod
    def __follows__(component, other):
        return False

    @classmethod
    def __matches__(component, dispatch_key):
        return False

    @classmethod
    def __dispatch__(interface, *args, **kwds):
        return None

    @classmethod
    def __prepare__(interface, *args, **kwds):
        realizations = context.app.component_registry.realizations
        dispatch_key = interface.__dispatch__(*args, **kwds)
        try:
            realization = realizations[interface, dispatch_key]
        except KeyError:
            realization = interface.__realize__(dispatch_key)
        return realization(*args, **kwds)

    @classmethod
    def __invoke__(interface, *args, **kwds):
        realizations = context.app.component_registry.realizations
        dispatch_key = interface.__dispatch__(*args, **kwds)
        try:
            realization = realizations[interface, dispatch_key]
        except KeyError:
            realization = interface.__realize__(dispatch_key)
        return realization(*args, **kwds)()

    def __call__(self):
        raise NotImplementedError("interface %s is not implemented for: %r"
                                  % (self.__interface__.__name__,
                                     self.__dispatch_key__))


class Realization(Component):

    __interface__ = None
    __dispatch_key__ = None


class Utility(Component):

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
        assert dispatch_key == ()
        return True

    @classmethod
    def __dispatch__(interface, *args, **kwds):
        return ()


class Adapter(Component):

    __types__ = []
    __arity__ = 0

    @classmethod
    def __dominates__(component, other):
        if issubclass(component, other):
            return True
        for type_vector in component.__types__:
            for other_type_vector in other.__types__:
                if aresubclasses(type_vector, other_type_vector):
                    if type_vector != other_type_vector:
                        return True

        return False

    @classmethod
    def __matches__(component, dispatch_key):
        return any(aresubclasses(dispatch_key, type_vector)
                   for type_vector in component.__types__)

    @classmethod
    def __dispatch__(interface, *args, **kwds):
        arity = interface.__arity__
        assert arity <= len(args)
        return tuple([type(arg) for arg in args[:arity]])


class Protocol(Component):

    __names__ = []

    @classmethod
    def __dispatch__(interface, name, *args, **kwds):
        return name

    @classmethod
    def __matches__(component, dispatch_key):
        assert isinstance(dispatch_key, str)
        return (dispatch_key in component.__names__)

    @classmethod
    def __catalogue__(interface):
        names = []
        seen = set()
        for component in interface.__implementations__():
            for name in component.__names__:
                if name not in seen:
                    names.append(name)
                    seen.add(name)
        names.sort(key=(lambda n: str(n)))
        return names


