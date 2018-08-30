#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


import re
import sys
import math
import decimal
import urllib.request, urllib.parse, urllib.error
import pkgutil
import datetime, time
import collections
import weakref
import unicodedata
import yaml


#
# Type checking helpers.
#


class maybe(object):
    """
    Checks if a value is either ``None`` or an instance of the specified type.

    Use with ``isinstance()`` as in::

        isinstance(X, maybe(T))
    """

    def __init__(self, value_type):
        self.value_type = value_type

    def __instancecheck__(self, value):
        return (value is None or isinstance(value, self.value_type))


class oneof(object):
    """
    Checks if a value is an instance of one of the specified types.

    Use with ``isinstance()`` as in::

        isinstance(X, oneof(T1, T2, ...))
    """

    def __init__(self, *value_types):
        self.value_types = value_types

    def __instancecheck__(self, value):
        for value_type in self.value_types:
            if isinstance(value, value_type):
                return True
        return False


class listof(object):
    """
    Checks if a value is a list containing elements of the specified type.

    Use with ``isinstance()`` as in::

        isinstance(X, listof(T))
    """

    def __init__(self, item_type):
        self.item_type = item_type

    def __instancecheck__(self, value):
        if not isinstance(value, list):
            return False
        item_type = self.item_type
        for item in value:
            if not isinstance(item, item_type):
                return False
        return True


class setof(object):
    """
    Checks if a value is a set containing elements of the specified type.

    Use with ``isinstance()`` as in::

        isinstance(X, setof(T))
    """

    def __init__(self, item_type):
        self.item_type = item_type

    def __instancecheck__(self, value):
        if not isinstance(value, set):
            return False
        item_type = self.item_type
        for item in value:
            if not isinstance(item, item_type):
                return False
        return True


class tupleof(object):
    """
    Checks if a value is a tuple with the fixed number of elements
    of the specified types.

    Use with ``isinstance()`` as in::

        isinstance(X, tupleof(T1, T2, ..., TN))
    """

    def __init__(self, *item_types):
        self.item_types = item_types

    def __instancecheck__(self, value):
        if not (isinstance(value, tuple) and
                len(value) == len(self.item_types)):
            return False
        for item, item_type in zip(value, self.item_types):
            if not isinstance(item, item_type):
                return False
        return True


class dictof(object):
    """
    Checks if a value is a dictionary with keys and elements of
    the specified types.

    Use with ``isinstance()`` as in::

        isinstance(X, dictof(T1, T2))
    """

    def __init__(self, key_type, item_type):
        self.key_type = key_type
        self.item_type = item_type

    def __instancecheck__(self, value):
        if not isinstance(value, dict):
            return False
        for key in value:
            if not isinstance(key, self.key_type):
                return False
            if not isinstance(value[key], self.item_type):
                return False
        return True


class omapof(object):
    """
    Checks if a value is an :class:`omap` object with elements of the specified
    type.

    Use with ``isinstance()`` as in::

        isinstance(X, omapof(T))
    """

    def __init__(self, item_type):
        self.item_type = item_type

    def __instancecheck__(self, value):
        if not isinstance(value, frozenomap):
            return False
        item_type = self.item_type
        for item in value:
            if not isinstance(item, item_type):
                return False
        return True


class subclassof(object):
    """
    Checks if a value is a subclass of the specified class.

    Use with ``isinstance()`` as in::

        isinstance(X, subclassof(T))
    """

    def __init__(self, class_type):
        self.class_type = class_type

    def __instancecheck__(self, value):
        return (isinstance(value, type) and issubclass(value, self.class_type))


class filelike(object):
    """
    Checks if a value is a file or a file-like object.

    Usage::

        isinstance(X, filelike())
    """

    def __instancecheck__(self, value):
        return (hasattr(value, 'read') or hasattr(value, 'write'))


def aresubclasses(subclasses, superclasses):
    """
    Takes two lists; checks if each element of the first list is
    a subclass of the corresponding element in the second list.

    `subclasses`: sequence of ``type``
        A list of potential subclasses.

    `superclasses`: sequence of ``type``
        A list of potential superclasses.

    *Returns*: ``bool``
        ``True`` if the check succeeds; ``False`` otherwise.
    """
    if len(subclasses) != len(superclasses):
        return False
    for subclass, superclass in zip(subclasses, superclasses):
        if not issubclass(subclass, superclass):
            return False
    return True


def isfinite(value):
    """
    Verifies that the given value is a finite number.
    """
    return (isinstance(value, int) or
            (isinstance(value, float) and not math.isinf(value)
                                      and not math.isnan(value)) or
            (isinstance(value, decimal.Decimal) and value.is_finite()))


#
# Text and formatting utilities.
#


def trim_doc(doc):
    """
    Strips indentation from a docstring; also removes leading and trailing
    blank lines.

    `doc`: ``str`` or ``None``
        A docstring.
    """
    assert isinstance(doc, maybe(oneof(str, str)))

    # Pass `None` through.
    if doc is None:
        return None

    # Convert to a list of lines and remove leading and trailing blank lines.
    lines = doc.splitlines()
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop(-1)

    # Find the smallest indentation for non-empty lines.
    indent = None
    for line in lines:
        short_line = line.lstrip()
        if short_line:
            line_indent = len(line)-len(short_line)
            if indent is None or line_indent < indent:
                indent = line_indent

    # Strip indentation whitespaces and return the result.
    if indent:
        lines = [line[indent:] for line in lines]
    return "\n".join(lines)


def to_name(text):
    """
    Converts a string to a valid HTSQL identifier.

    The given `text` value is transformed as follows:

    - translated to Unicode normal form C;
    - converted to lowercase;
    - has non-alphanumeric characters replaced with underscores;
    - preceded with an underscore if it starts with a digit;
    - an empty string is replaced with ``'_'``.
    """
    assert isinstance(text, str)
    if isinstance(text, str):
        text = text.decode('utf-8', 'replace')
    if not text:
        text = "_"
    text = unicodedata.normalize('NFC', text).lower()
    text = re.sub(r"(?u)^(?=\d)|\W", "_", text)
    return text


def urlquote(text, reserved=";/?:@&=+$,"):
    """
    Replaces non-printable and reserved characters with ``%XX`` sequences.
    """
    assert isinstance(text, str)
    text = re.sub(r"[\x00-\x1F%%\x7F%s]" % reserved,
                  (lambda m: "%%%02X" % ord(m.group())),
                  text)
    return text


def to_literal(text):
    """
    Converts the text value to a valid string literal.

    This function escapes all non-printable characters and
    wraps the text value in single quotes.
    """
    assert isinstance(text, str)
    text = "'%s'" % urlquote(text, "").replace("'", "''")
    return text


def similar(model, sample):
    """
    Checks if `model` is similar to `sample`.

    `model`: ``unicode``
        A model string.

    `sample`: ``unicode``
        A sample string.

    *Returns*: ``bool``
        ``True`` if `model` is not too much different from `sample`;
        ``False`` otherwise.

    Use for error reporting to suggest alternatives for an unknown `model`
    identifier.
    """
    assert isinstance(model, str)
    assert isinstance(sample, str)

    # Skip empty strings.
    if not model or not sample:
        return False

    # Confirm similarity if `model` is a prefix of `sample`, but not for
    # a one-character `model`.
    if len(model) > 1 and sample.startswith(model):
        return True

    # Find the edit distance between `model` and `sample`; confirm similarity
    # if the distance is not greater than `1 + 1/5 * len(model)`.
    M = len(model)
    N = len(sample)
    threshold = 1+M/5
    INF = threshold+1
    # Bail out early if the threshold is impossible to reach.
    if abs(M-N) > threshold:
        return False
    # The edit distance between `model[:i]` and `sample[:j]`.
    distance = {}
    # Boundary conditions.
    for i in range(min(M, threshold)+1):
        distance[i, 0] = i
    for j in range(min(N, threshold)+1):
        distance[0, j] = j
    # Apply dynamic programming with a recursive formula:
    #   distance[i,j] = min(distance[i-1, j-1] + 1 (REPLACE),
    #                       distance[i, j-1] + 1 (INSERT),
    #                       distance[i-1, j] + 1 (DELETE),
    #                       distance[i-1, j-1] if model[i] == sample[j])
    for i in range(1, M+1):
        for j in range(max(1, i-threshold), min(N, i+threshold)+1):
            k = distance.get((i-1, j-1), INF)
            if model[i-1] != sample[j-1]:
                k += 1
            if (i > 1 and j > 1 and model[i-2] == sample[j-1]
                                and model[i-1] == sample[j-2]):
                k = min(k, distance.get((i-2, j-2), INF)+1)
            k = min(k, distance.get((i-1, j), INF)+1,
                       distance.get((i, j-1), INF)+1)
            if k <= threshold:
                distance[i, j] = k

    # Check if the distance does not exceed the threshold.
    return ((M, N) in distance)


class TextBuffer(object):
    """
    Reads the input text in blocks matching some regular expressions.

    `text`: ``str`` or ``unicode``
        The input text.
    """

    # Characters to skip over.
    skip_regexp = re.compile(r"(?: \s+ | [#] [^\r\n]* )+", re.X|re.U)

    def __init__(self, text):
        assert isinstance(text, str)
        # The input text.
        self.text = text
        # The head of the buffer.
        self.index = 0
        # Advance over whitespace and comments.
        self.skip()

    def __bool__(self):
        return (self.index < len(self.text))

    def reset(self):
        """
        Rewinds to the beginning of the text.
        """
        self.index = 0
        self.skip()

    def peek(self, pattern):
        """
        Checks if the head of the buffer matches the given pattern.

        `pattern`: ``str`` or ``unicode``
            A regular expression pattern.

        *Returns*: ``bool``
            ``True`` if the buffer head matches the given pattern; ``False``
            otherwise.
        """
        # Match the given pattern against the buffer head.
        regexp = re.compile(pattern, re.X|re.U)
        match = regexp.match(self.text, self.index)
        return (match is not None)

    def pull(self, pattern):
        """
        Reads a text block matching the given pattern from the head of the
        buffer.

        `pattern`: ``str`` or ``unicode``
            A regular expression pattern.

        *Returns*: ``str`` or ``unicode`` or ``None``
            A text block; ``None`` if the buffer head does not match the
            pattern.

        :meth:`pull` skips whitespace characters and comments at the head of
        the buffer.
        """
        # The matching block of text.
        block = None
        # Match the given pattern against the buffer head.
        regexp = re.compile(pattern, re.X|re.U)
        match = regexp.match(self.text, self.index)
        if match is not None:
            # Extract the block that matched the pattern.
            block = match.group()
            # Move the buffer head.
            self.index = match.end()
            # Advance over whitespace characters and comments.
            self.skip()
        return block

    def skip(self):
        # Advance over whitespace characters and comments.
        if self.skip_regexp is not None:
            match = self.skip_regexp.match(self.text, self.index)
            if match is not None:
                self.index = match.end()

    def fail(self, message):
        """
        Generates an exception with a fragment of the buffer at the current
        position included in the error message.

        `message`: ``str``
            The error message.

        *Returns*: :exc:`RuntimeError` instance
        """
        # The buffer from which we extract the fragment.
        excerpt = self.text
        # The head position.
        index = self.index
        # Convert the buffer to unicode and adjust the position.
        if isinstance(excerpt, str):
            excerpt = excerpt.decode('utf-8', 'replace')
            index = len(excerpt[:index].decode('utf-8', 'replace'))
        # Extract the line around the head position.
        start = excerpt.rfind("\n", 0, index)+1
        end = excerpt.find("\n", start)
        if end == -1:
            end = len(self.text)
        excerpt = excerpt[start:end].encode('utf-8')
        # Make a pointer to the buffer head.
        indent = index-start
        pointer = ' '*indent + '^'
        # Generate an exception object.
        return RuntimeError("\n".join([message, excerpt, pointer]))


#
# Topological sorting.
#


def toposort(elements, order, is_total=False):
    """
    Sorts elements with respect to the given partial order.

    Takes a list of elements and a partial order relation.  Returns
    the elements reordered to satisfy the given order.

    `elements`
        A list of elements.

    `order`
        A function which represents the partial order relation.  ``order(x)``
        takes an element `x` and produces a list of elements that must
        preceed `x`.

    `is_total`: ``bool``
        If set, validates that the given partial order is, in fact, total.

    This function raises :exc:`RuntimeError` if `order` is not a valid
    partial order (contains loops) or when `is_total` is set and `order`
    is not a valid total order.
    """
    # For a description of the algorithm, see, for example,
    #   http://en.wikipedia.org/wiki/Topological_sorting
    # In short, we apply depth-first search to the DAG represented
    # by the partial order.  As soon as the search finishes exploring
    # some node, the node is added to the list.

    # The sorted list.
    ordered = []
    # The set of nodes which the DFS has already processed.
    visited = set()
    # The set of nodes currently being processed by the DFS.
    active = set()
    # The path to the current node.  Note that `set(path) == active`.
    path = []
    # The map from a node to the position of the node in the original list.
    positions = dict((element, index)
                     for index, element in enumerate(elements))

    # Implements the depth-first search.
    def dfs(node):
        # Check if the node has already been processed.
        if node in visited:
            return

        # Update the path; check for cycles.
        path.append(node)
        if node in active:
            raise RuntimeError("order is not valid: loop detected",
                               path[path.index(node):])
        active.add(node)

        # Get the list of adjacent nodes.
        adjacents = order(node)
        # Sort the adjacent elements according to their order in the
        # original list.  It helps to keep the original order when possible.
        adjacents = sorted(adjacents, key=(lambda i: positions[i]))

        # Visit the adjacent nodes.
        for adjacent in adjacents:
            dfs(adjacent)

        # If requested, check that the order is total.
        if is_total and ordered:
            if ordered[-1] not in adjacents:
                raise RuntimeError("order is not total",
                                   [ordered[-1], node])

        # Add the node to the sorted list.
        ordered.append(node)

        # Remove the node from the path; add it to the set of processed nodes.
        path.pop()
        active.remove(node)
        visited.add(node)

    # Apply the DFS to the whole DAG.
    for element in elements:
        dfs(element)

    # Break the cycle created by a recursive nested function.
    dfs = None

    return ordered


#
# Cached property decorator.
#


class cachedproperty(object):
    """
    Implements a cached property decorator.

    The decorator calls the `getter` function on the first access to the
    property and saves the result.  Any subsequent access to the property
    returns the saved value.

    Usage::

        class C(object):
            @cachedproperty
            def field(self):
                # Called once to calculate the field value.
                # [...]
                return value
    """

    def __init__(self, getter):
        self.getter = getter
        # Steal the name and the docstring.
        self.__name__ = getter.__name__
        self.__doc__ = getter.__doc__

    def __get__(self, obj, objtype=None):
        # Access as a class attribute.
        if obj is None:
            return self

        # Access as an instance attribute; invoke the getter.
        value = self.getter(obj)
        # Store the result in the instance dictionary.  Since for a non-data
        # descriptor (i.e., without `__set__()`) `__dict__` takes the
        # precedence, the descriptor will never be called again.
        obj.__dict__[self.__name__] = value
        return value


#
# Ordered mapping.
#


class frozenomap(collections.Mapping):
    """
    An ordered immutable mapping.

    This container behaves like an immutable ``dict`` object with one
    exception: iterating over the container produces *values* (rather than
    *keys*) in the order they were added to the container.
    """

    def __init__(self, iterable=None):
        # List of keys in the order of insertion.
        self._keys = []
        # key -> value dictionary.
        self._value_by_key = {}
        # Initialize the mapping with elements from `iterable`.
        if isinstance(iterable, collections.Mapping):
            iterable = iter(iterable.items())
        if iterable is not None:
            for key, value in iterable:
                if key not in self._value_by_key:
                    self._keys.append(key)
                self._value_by_key[key] = value

    def __repr__(self):
        # 'omap([(key, value), ...])'
        return "%s([%s])" % (self.__class__.__name__,
                             ", ".join(repr((key, self._value_by_key[key]))
                                       for key in self._keys))

    def __hash__(self):
        return hash(tuple((key, self._value_by_key[key])
                          for key in self._keys))

    def __eq__(self, other):
        # Respect both the mapping content and the order of insertion.
        if not isinstance(other, frozenomap):
            return NotImplemented
        return (self._keys == other._keys and
                self._value_by_key == other._value_by_key)

    # `__ne__` is defined in `collections.Mapping`

    # Implementation of `collections.Mapping` API.

    def __iter__(self):
        # Here we diverge from `dict` interface: `iter(omap)` yields *values*.
        for key in self._keys:
            yield self._value_by_key[key]

    def __len__(self):
        return len(self._keys)

    def __contains__(self, key):
        return (key in self._value_by_key)

    def __getitem__(self, key):
        return self._value_by_key[key]

    def iterkeys(self):
        return iter(self._keys)

    def itervalues(self):
        for key in self._keys:
            yield self._value_by_key[key]

    def iteritems(self):
        for key in self._keys:
            yield (key, self._value_by_key[key])

    def keys(self):
        return list(self._keys)

    def items(self):
        return [(key, self._value_by_key[key]) for key in self._keys]

    def values(self):
        return [self._value_by_key[key] for key in self._keys]


class omap(frozenomap, collections.MutableMapping):
    """
    An ordered mutable mapping.

    This container behaves like a ``dict`` object with one exception: iterating
    over the container produces *values* (rather than *keys*) in the order they
    were added to the container.

    Overriding an entry does not change its position; delete and insert the
    entry to move it to the end.
    """

    __hash__ = None

    # Implementation of `collections.MutableMapping` API.

    def __setitem__(self, key, value):
        if key not in self._value_by_key:
            self._keys.append(key)
        self._value_by_key[key] = value

    def __delitem__(self, key):
        # FIXME: O(N) behavior.
        del self._value_by_key[key]
        self._keys.remove(key)

    def popitem(self):
        key = self._keys.pop()
        value = self._value_by_key[key]
        del self._value_by_key[key]
        return value

    def clear(self):
        self._keys = []
        self._value_by_key = {}

    def update(self, iterable):
        if isinstance(iterable, collections.Mapping):
            iterable = iter(iterable.items())
        for key, value in iterable:
            if key not in self._value_by_key:
                self._keys.append(key)
            self._value_by_key[key] = value


#
# Object types with special behavior.
#


class Clonable(object):
    """
    A clonable object.

    Subclasses of :class:`Clonable` can use :meth:`clone` and :meth:`clone_to`
    methods to create a clone of the given object with a specified set of
    attributes replaced.

    Subclasses of :class:`Clonable` must follow the following conventions:

    (1) Clonable objects must be immutable.

    (2) Each subclass must reimplement the `__init__` constructor.  All
        arguments of ``__init__`` must be stored as instance attributes
        unchanged (or, if changed, must still be in the form acceptable by the
        constructor).

    (3) The constructor must not expect a ``*``-wildcard argument.

    (4) The constructor may take a ``**``-wildcard argument.  In this case,
        the argument itself and all its entries must be stored as instance
        attributes.
    """

    __slots__ = ()

    def __init__(self):
        # Must be overriden in subclasses.
        raise NotImplementedError("%s.__init__()" % self.__class__.__name__)

    def clone(self, **replacements):
        """
        Clones the object assigning new values to selected attributes.

        `replacements`
            New attribute values.

        *Returns*
            A new object of the same type that has the same attribute values
            except those for which new values are specified.
        """
        # A shortcut: if there are no replacements, we could reuse
        # the same object.
        if not replacements:
            return self
        # Otherwise, reuse a more general method.
        return self.clone_to(self.__class__, **replacements)

    def clone_to(self, clone_type, **replacements):
        """
        Clones the object changing its type and assigning new values to
        selected attributes.

        `clone_type`: ``type``
            The type of the new object.

        `replacements`
            New attribute values.

        *Returns*
            A new object of the specified type which has the same attribute
            values as the original object except those for which new values are
            provided.
        """
        # Get the list of constructor arguments.  We expect that for each
        # constructor argument, the object has an attribute with the same name.
        init_code = self.__init__.__func__.__code__
        # Fetch the names of regular arguments, but skip `self`.
        names = list(init_code.co_varnames[1:init_code.co_argcount])
        # Check for * and ** arguments.  We cannot properly support
        # * arguments, so just complain about it.
        assert not (init_code.co_flags & 0x04)  # CO_VARARGS
        # Check for ** arguments.  If present, they must adhere
        # the following protocol:
        # (1) The object must keep the ** dictionary as an attribute
        #     with the same name and content.
        # (2) The object must have an attribute for each entry in
        #     the ** dictionary.
        if init_code.co_flags & 0x08:           # CO_VARKEYWORDS
            name = init_code.co_varnames[init_code.co_argcount]
            names += sorted(getattr(self, name))
        # Check that all replacements are, indeed, constructor parameters.
        assert all(key in names for key in sorted(replacements))
        # Arguments of a constructor call to generate a clone.
        arguments = {}
        # Indicates if at least one argument has changed.
        is_modified = False
        # If the target type differs from the object type, we need to
        # generate a new object even when there are no modified attributes.
        if self.__class__ is not clone_type:
            is_modified = True
        # For each argument, either extract the current value, or
        # get a replacement.
        for name in names:
            value = getattr(self, name)
            if name in replacements and replacements[name] is not value:
                value = replacements[name]
                is_modified = True
            arguments[name] = value
        # Even though we may have some replacements, in fact they all coincide
        # with the object attributes, so we could reuse the same object.
        if not is_modified:
            return self
        # Call the constructor and return a new object.
        clone = clone_type(**arguments)
        return clone


try:
    from htsql.htsql_speedups import Clonable
except ImportError:
    pass


class Hashable(object):
    """
    An immutable object with by-value comparison semantics.

    A subclass of :class:`Hashable` should reimplement :meth:`__basis__`
    to produce a tuple of all object attributes which uniquely identify
    the object.

    Two :class:`Hashable` instances are considered equal if they are of
    the same type and their basis vectors are equal.
    """

    __slots__ = ('_basis', '_hash', '_matches', '__weakref__')

    def __hash__(self):
        try:
            return self._hash
        except AttributeError:
            self._rehash()
            return self._hash

    def __basis__(self):
        """
        Returns a vector of values uniquely identifying the object.
        """
        raise NotImplementedError()

    def _rehash(self):
        # Calculate the object hash and the basis vector.
        _basis = self.__basis__()
        # Flatten and return the vector.
        if isinstance(_basis, tuple):
            elements = []
            for element in _basis:
                if isinstance(element, Hashable):
                    element_class = element.__class__
                    try:
                        element_hash = element._hash
                        element_basis = element._basis
                    except AttributeError:
                        element._rehash()
                        element_hash = element._hash
                        element_basis = element._basis
                    elements.append((element_class,
                                     element_hash,
                                     element_basis))
                else:
                    elements.append(element)
            _basis = tuple(elements)
        self._basis = _basis
        self._hash = hash(_basis)

    def __eq__(self, other):
        # We could just compare object basis vectors, but
        # for performance, we start with faster checks.
        if self is other:
            return True
        if not (isinstance(other, Hashable) and
                self.__class__ is other.__class__):
            return False
        try:
            if self._matches[id(other)] is other:
                return True
        except (AttributeError, KeyError):
            pass
        try:
            _hash = self._hash
            _basis = self._basis
        except AttributeError:
            self._rehash()
            _hash = self._hash
            _basis = self._basis
        try:
            _other_hash = other._hash
            _other_basis = other._basis
        except AttributeError:
            other._rehash()
            _other_hash = other._hash
            _other_basis = other._basis
        if _hash == _other_hash and _basis == _other_basis:
            try:
                self._matches[id(other)] = other
            except AttributeError:
                self._matches = weakref.WeakValueDictionary()
                self._matches[id(other)] = other
            return True
        return False

    def __ne__(self, other):
        # Since we override `==`, we also need to override `!=`.
        if self is other:
            return False
        return not (self == other)


try:
    from htsql.htsql_speedups import Hashable
except ImportError:
    pass


class Printable(object):
    """
    An object with default string representation.

    A subclass of :class:`Printable` is expected to reimplement the
    :meth:`__unicode__` method.
    """

    __slots__ = ()

    def __unicode__(self):
        # Override in subclasses.
        return "-"

    def __str__(self):
        # Reuse implementation of `__unicode__`.
        return str(self).encode('utf-8')

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self)


class YAMLable(object):
    """
    An object with YAML representation.

    Subclasses of :class:`YAMLable` must override :meth:`__yaml__` to generate
    a list of ``(field, value)`` pairs.
    """

    def to_yaml(self):
        """
        Returns YAML representation of the object.
        """
        return yaml.dump(self, Dumper=YAMLableDumper)

    def __yaml__(self):
        # Override in subclasses.
        return []

    def __str__(self):
        return self.to_yaml()

    def __unicode__(self):
        return self.to_yaml().decode('utf-8')

    def __repr__(self):
        return "<%s>" % self.__class__.__name__


class YAMLableDumper(yaml.Dumper):
    # Serializer for `YAMLable` instances.

    def represent_str(self, data):
        # Represent both `str` and `unicode` objects as YAML strings.
        # Use block style for multiline strings.
        if isinstance(data, str):
            data = data.encode('utf-8')
        tag = None
        style = None
        if data.endswith('\n'):
            style = '|'
        try:
            data = data.decode('utf-8')
            tag = 'tag:yaml.org,2002:str'
        except UnicodeDecodeError:
            data = data.encode('base64')
            tag = 'tag:yaml.org,2002:binary'
            style = '|'
        return self.represent_scalar(tag, data, style=style)

    def represent_yamlable(self, data):
        # Represent `YAMLable` objects.
        tag = str('!'+data.__class__.__name__)
        mapping = list(data.__yaml__())
        # Use block style if any field value is a multiline string.
        flow_style = None
        if any(isinstance(item, str) and '\n' in item
                for key, item in mapping):
            flow_style = False
        return self.represent_mapping(tag, mapping, flow_style=flow_style)

    def generate_anchor(self, node):
        # Use the class name for anchor names.
        if not isinstance(self.last_anchor_id, dict):
            self.last_anchor_id = { '': 1 }
        if node.tag.startswith('!'):
            text = node.tag[1:]
        else:
            text = ''
        self.last_anchor_id.setdefault(text, 1)
        index = self.last_anchor_id[text]
        self.last_anchor_id[text] += 1
        if text:
            text += '-%s' % index
        else:
            text = str(index)
        return text


YAMLableDumper.add_representer(str, YAMLableDumper.represent_str)
YAMLableDumper.add_representer(str, YAMLableDumper.represent_str)
YAMLableDumper.add_multi_representer(YAMLable,
        YAMLableDumper.represent_yamlable)


def to_yaml(data):
    """
    Represents the value in YAML format.
    """
    return yaml.dump(data, Dumper=YAMLableDumper)


#
# Database connection parameters.
#


class DB(Clonable, Hashable, Printable):
    """
    Parameters of a database connection.

    `engine`: ``str``
        The type of the database server; e.g., ``'pgsql'`` or ``'sqlite'``.

    `database`: ``str``
        The name of the database; the path to the database file for SQLite.

    `username`: ``str`` or ``None``
        The user name used for authentication; ``None`` to use the default.

    `password`: ``str`` or ``None``
        The password used for authentication; ``None`` to authenticate
        without providing a password.

    `host`: ``str`` or ``None``
        The host address; ``None`` to use the default or when not applicable.

    `port`: ``int`` or ``None``
        The port number; ``None`` to use the default or when not applicable.

    `options`: ``dict`` or ``None``
        A dictionary containing extra connection parameters; currently unused.
    """

    # Regular expression for parsing a connection URI of the form:
    # 'engine://username:password@host:port/database?options'.
    key_chars = r'''[%0-9a-zA-Z_.-]+'''
    value_chars = r'''[%0-9a-zA-Z`~!#$^*()_+\\|\[\]{};'",.<>/-]+'''
    pattern = r'''
        ^
        (?P<engine> %(key_chars)s )
        :
        (?: //
            (?: (?P<username> %(key_chars)s )?
                (?: : (?P<password> %(value_chars)s )? )? @ )?
            (?: (?P<host> %(key_chars)s )?
                (?: : (?P<port> %(key_chars)s )? )? )?
            /
        )?
        (?P<database> %(value_chars)s )
        (?: \?
            (?P<options>
                %(key_chars)s = (?: %(value_chars)s )?
                (?: & %(key_chars)s = (?: %(value_chars)s )? )* )? )?
        $
    ''' % vars()
    regexp = re.compile(pattern, re.X|re.U)

    def __init__(self, engine, database, username=None, password=None,
                 host=None, port=None, options=None):
        assert isinstance(engine, str)
        assert isinstance(database, str)
        assert isinstance(username, maybe(str))
        assert isinstance(password, maybe(str))
        assert isinstance(host, maybe(str))
        assert isinstance(port, maybe(int))
        assert isinstance(options, maybe(dictof(str, str)))

        self.engine = engine
        self.database = database
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.options = options

    def __basis__(self):
        return (self.engine, self.database,
                self.username, self.password, self.host, self.port,
                tuple(sorted(self.options))
                    if self.options is not None else None)

    @classmethod
    def parse(cls, value):
        """
        Parses a connection URI and returns a corresponding :class:`DB`
        instance.

        `value`: ``str``, ``unicode``, ``dict`` or :class:`DB`

        *Returns*: :class:`DB`

        A connection URI is a string of the form::

            engine://username:password@host:port/database?options

        The `engine` and `database` fragments are mandatory; the others could
        be omitted.

        If a fragment contains separator characters which cannot be represented
        literally (such as ``:``, ``/``, ``@`` or ``?``), the characters should
        be escaped using ``%``-encoding.

        If the connection URI is not in a valid format, :exc:`ValueError`
        is raised.

        :meth:`parse` also accepts:

        - a dictionary with keys ``'engine'``, ``'database'``, ``'username'``,
          ``'password'``, ``'host'``, ``'port'``, ``'options'``;
        - an instance of :class:`DB`.
        """
        # `value` must be one of:
        #
        # - an instance of `DB`;
        # - a connection URI in the form
        #   'engine://username:password@host:port/database?options';
        # - a dictionary with the keys:
        #   'engine', 'database', 'username', 'password', 'host', 'port',
        #   'database', 'options'.
        if not isinstance(value, (cls, str, dict)):
            raise ValueError("a connection URI is expected; got %r" % value)

        # Instances of `DB` are returned as is.
        if isinstance(value, cls):
            return value

        # We expect a connection URI to be a regular string, but we allow
        # Unicode strings too.
        if isinstance(value, str):
            value = value.encode('utf-8')

        # If a string is given, assume it is a connection URI and parse it.
        if isinstance(value, str):
            match = cls.regexp.search(value)
            if match is None:
                raise ValueError("expected a connection URI of the form"
                                 " 'engine://username:password@host:port"
                                 "/database?options'; got %r" % value)
            engine = match.group('engine')
            username = match.group('username')
            password = match.group('password')
            host = match.group('host')
            port = match.group('port')
            database = match.group('database')
            options = match.group('options')

            # We assume that values are URI-quoted; unquote them here.
            # Also perform necessary type conversion.
            engine = urllib.parse.unquote(engine)
            if username is not None:
                username = urllib.parse.unquote(username)
            if password is not None:
                password = urllib.parse.unquote(password)
            if host is not None:
                host = urllib.parse.unquote(host)
            if port is not None:
                port = urllib.parse.unquote(port)
                try:
                    port = int(port)
                except ValueError:
                    raise ValueError("expected port to be an integer;"
                                     " got %r" % port)
            database = urllib.parse.unquote(database)
            if options is not None:
                options = dict(list(map(urllib.parse.unquote, item.split('=', 1)))
                               for item in options.split('&'))

        # If a dictionary is given, assume it is a dictionary with
        # the fixed set of keys.  Extract the values.
        if isinstance(value, dict):
            for key in sorted(value):
                if key not in ['engine', 'username', 'password',
                               'host', 'port', 'database', 'options']:
                    raise ValueError("unexpected key: %r" % key)
            if 'engine' not in value:
                raise ValueError("key 'engine' is not found in %r" % value)
            if 'database' not in value:
                raise ValueError("key 'database' is not found in %r" % value)
            engine = value['engine']
            database = value['database']
            username = value.get('username')
            password = value.get('password')
            host = value.get('host')
            port = value.get('port')
            options = value.get('options')

            # Sanity check on the values.
            if isinstance(engine, str):
                engine = engine.encode('utf-8')
            if not isinstance(engine, str):
                raise ValueError("engine must be a string; got %r" % engine)
            if isinstance(database, str):
                database = database.encode('utf-8')
            if not isinstance(database, str):
                raise ValueError("database must be a string; got %r"
                                 % database)
            if isinstance(username, str):
                username = username.encode('utf-8')
            if not isinstance(username, maybe(str)):
                raise ValueError("username must be a string; got %r" % username)
            if isinstance(password, str):
                password = password.encode('utf-8')
            if not isinstance(password, maybe(str)):
                raise ValueError("password must be a string; got %r" % password)
            if isinstance(host, str):
                host = host.encode('utf-8')
            if not isinstance(host, maybe(str)):
                raise ValueError("host must be a string; got %r" % host)
            if isinstance(port, str):
                try:
                    port = int(port)
                except ValueError:
                    pass
            if not isinstance(port, maybe(int)):
                raise ValueError("port must be an integer; got %r" % port)
            if not isinstance(options, maybe(dictof(str, str))):
                raise ValueError("options must be a dictionary with"
                                 " string keys and values; got %r" % options)

        # Permit capitalized engine name.
        engine = engine.lower()

        # We are done, produce an instance.
        return cls(engine, database, username, password, host, port, options)

    def __unicode__(self):
        """Generate a connection URI corresponding to the parameters."""
        # The generated URI should only contain ASCII characters because
        # we want it to translate to Unicode without decoding errors.
        chunks = []
        chunks.append(self.engine)
        chunks.append('://')
        if ((self.username is not None or self.password is not None) or
            (self.host is None and self.port is not None)):
            if self.username is not None:
                chunks.append(urllib.parse.quote(self.username, safe=''))
            if self.password is not None:
                chunks.append(':')
                chunks.append(urllib.parse.quote(self.password, safe=''))
            chunks.append('@')
        if self.host is not None:
            chunks.append(urllib.parse.quote(self.host, safe=''))
        if self.port is not None:
            chunks.append(':')
            chunks.append(str(self.port))
        chunks.append('/')
        chunks.append(urllib.parse.quote(self.database))
        if self.options:
            chunks.append('?')
            is_first = True
            for key in sorted(self.options):
                if is_first:
                    is_first = False
                else:
                    chunks.append('&')
                chunks.append(urllib.parse.quote(key, safe=''))
                chunks.append('=')
                chunks.append(urllib.parse.quote(self.options[key]))
        return ''.join(chunks)


#
# Auto-import utility.
#


def autoimport(name):
    """
    Imports all modules (including subpackages) in a package.

    `name`: ``str``
        The package name.
    """
    # Import the package itself.
    package = __import__(name, fromlist=['__name__'])
    # It must be the package we asked for.
    assert hasattr(package, '__name__') and package.__name__ == name
    # Make sure it is indeed a package (has `__path__`).
    assert hasattr(package, '__path__')
    # Get the list of modules in the package directory; prepend the module
    # names with the package name.  That also includes modules in subpackages.
    modules = pkgutil.walk_packages(package.__path__, name+'.')
    # Import all the modules.
    for importer, module_name, is_package in modules:
        __import__(module_name)


