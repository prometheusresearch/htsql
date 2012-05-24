#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql.core.util`
======================

This module provides various hard-to-categorize utilities.
"""


import re
import sys
import math
import decimal
import urllib
import pkgutil
import datetime, time
import keyword
import operator


#
# Database connection parameters.
#


class DB(object):
    """
    Represents parameters of a database connection.

    `engine`
        The type of the database server; currently supported are ``'pgsql'``
        and ``'sqlite'``.

    `username`
        The user name used to authenticate; ``None`` to use the default.

    `password`
        The password used to authenticate; ``None`` to authenticate
        without providing a password.

    `host`
        The host address; ``None`` to use the default.

    `port`
        The port number; ``None`` to use the default.

    `database`
        The database name.

        For SQLite, the path to the database file, or `:memory:`.

    `options`
        A dictionary containing extra connection parameters.

        Currently ignored by all engines.

    The parameters `username`, `password`, `host`, `port` are
    ignored by the SQLite engine.
    """

    # Regular expression for parsing a connection URI of the form:
    # 'engine://username:password@host:port/database?options'.
    key_chars = r'''[%0-9a-zA-Z_.-]+'''
    value_chars = r'''[%0-9a-zA-Z`~!#$^*()_+\\|\[\]{};'",.<>/-]+'''
    pattern = r'''(?x)
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
    regexp = re.compile(pattern)

    def __init__(self, engine, username, password, host, port, database,
                 options=None):
        # Sanity checking on the arguments.
        assert isinstance(engine, str)
        assert isinstance(username, maybe(str))
        assert isinstance(password, maybe(str))
        assert isinstance(host, maybe(str))
        assert isinstance(port, maybe(int))
        assert isinstance(database, str)
        assert isinstance(options, maybe(dictof(str, str)))

        self.engine = engine
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.options = options

    @classmethod
    def parse(cls, value):
        """
        Parses a connection URI and returns a corresponding
        :class:`DB` instance.

        A connection URI is a string of the form::

            engine://username:password@host:port/database?options

        `engine`
            The type of the database server; supported values are ``pgsql``
            and ``sqlite``.

        `username:password`
            Used for authentication.

        `host:port`
            The server address.

        `database`
            The name of the database.

            For SQLite, the path to the database file.

        `options`
            A string of the form ``key=value&...`` providing extra
            connection parameters.

        The parameters `engine` and `database` are required, all the other
        parameters are optional.

        If a parameter contains a character which cannot be represented
        literally (such as ``:``, ``/``, ``@`` or ``?``), it should be
        escaped using ``%``-encoding.

        If the connection URI is not in a valid format, :exc:`ValueError`
        is raised.

        Besides a connection URI, the function also accepts instances
        of :class:`DB` and dictionaries.  An instance of :class:`DB` is
        returned as is.  A dictionary is assumed to contain connection
        parameters.  The corresponding instance of :class:`DB` is returned.
        """
        # `value` must be one of:
        #
        # - an instance of `DB`;
        # - a connection URI in the form
        #   'engine://username:password@host:port/database?options';
        # - a dictionary with the keys:
        #   'engine', 'username', 'password', 'host', 'port',
        #   'database', 'options'.
        if not isinstance(value, (cls, str, unicode, dict)):
            raise ValueError("a connection URI is expected; got %r" % value)

        # Instances of `DB` are returned as is.
        if isinstance(value, cls):
            return value

        # We expect a connection URI to be a regular string, but we allow
        # Unicode strings too.
        if isinstance(value, unicode):
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
            engine = urllib.unquote(engine)
            if username is not None:
                username = urllib.unquote(username)
            if password is not None:
                password = urllib.unquote(password)
            if host is not None:
                host = urllib.unquote(host)
            if port is not None:
                port = urllib.unquote(port)
                try:
                    port = int(port)
                except ValueError:
                    raise ValueError("expected port to be an integer;"
                                     " got %r" % port)
            database = urllib.unquote(database)
            if options is not None:
                options = dict(map(urllib.unquote, item.split('=', 1))
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
            username = value.get('username')
            password = value.get('password')
            host = value.get('host')
            port = value.get('port')
            database = value['database']
            options = value.get('options')

            # Sanity check on the values.
            if isinstance(engine, unicode):
                engine = engine.encode('utf-8')
            if not isinstance(engine, str):
                raise ValueError("engine must be a string; got %r" % engine)
            if isinstance(username, unicode):
                username = username.encode('utf-8')
            if not isinstance(username, maybe(str)):
                raise ValueError("username must be a string; got %r" % username)
            if isinstance(password, unicode):
                password = password.encode('utf-8')
            if not isinstance(password, maybe(str)):
                raise ValueError("password must be a string; got %r" % password)
            if isinstance(host, unicode):
                host = host.encode('utf-8')
            if not isinstance(host, maybe(str)):
                raise ValueError("host must be a string; got %r" % host)
            if isinstance(port, (str, unicode)):
                try:
                    port = int(port)
                except ValueError:
                    pass
            if not isinstance(port, maybe(int)):
                raise ValueError("port must be an integer; got %r" % port)
            if isinstance(database, unicode):
                database = database.encode('utf-8')
            if not isinstance(database, str):
                raise ValueError("database must be a string; got %r"
                                 % database)
            if not isinstance(options, maybe(dictof(str, str))):
                raise ValueError("options must be a dictionary with"
                                 " string keys and values; got %r" % options)

        # We are done, produce an instance.
        return cls(engine, username, password, host, port, database, options)

    def __str__(self):
        """Generate a connection URI corresponding to the instance."""
        # The generated URI should only contain ASCII characters because
        # we want it to translate to Unicode without decoding errors.
        chunks = []
        chunks.append(self.engine)
        chunks.append('://')
        if ((self.username is not None or self.password is not None) or
            (self.host is None and self.port is not None)):
            if self.username is not None:
                chunks.append(urllib.quote(self.username, safe=''))
            if self.password is not None:
                chunks.append(':')
                chunks.append(urllib.quote(self.password, safe=''))
            chunks.append('@')
        if self.host is not None:
            chunks.append(urllib.quote(self.host, safe=''))
        if self.port is not None:
            chunks.append(':')
            chunks.append(str(self.port))
        chunks.append('/')
        chunks.append(urllib.quote(self.database))
        if self.options is not None:
            chunks.append('?')
            is_first = True
            for key in sorted(self.options):
                if is_first:
                    is_first = False
                else:
                    chunks.append('&')
                chunks.append(urllib.quote(key, safe=''))
                chunks.append('=')
                chunks.append(urllib.quote(self.options[key]))
        return ''.join(chunks)

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self)


#
# Type checking helpers.
#


class maybe(object):
    """
    Checks if a value is either ``None`` or an instance of the specified type.

    Usage::

        isinstance(value, maybe(T))
    """

    def __init__(self, value_type):
        self.value_type = value_type

    def __instancecheck__(self, value):
        return (value is None or isinstance(value, self.value_type))


class oneof(object):
    """
    Checks if a value is an instance of one of the specified types.

    Usage::

        isinstance(value, oneof(T1, T2, ...))
    """

    def __init__(self, *value_types):
        self.value_types = value_types

    def __instancecheck__(self, value):
        return any(isinstance(value, value_type)
                   for value_type in self.value_types)


class listof(object):
    """
    Checks if a value is a list containing elements of the specified type.

    Usage::
    
        isinstance(value, listof(T))
    """

    def __init__(self, item_type):
        self.item_type = item_type

    def __instancecheck__(self, value):
        return (isinstance(value, list) and
                all(isinstance(item, self.item_type) for item in value))


class setof(object):
    """
    Checks if a value is a set containing elements of the specified type.

    Usage::
    
        isinstance(value, setof(T))
    """

    def __init__(self, item_type):
        self.item_type = item_type

    def __instancecheck__(self, value):
        return (isinstance(value, set) and
                all(isinstance(item, self.item_type) for item in value))


class tupleof(object):
    """
    Checks if a value is a tuple with the fixed number of elements
    of the specified types.

    Usage::

        isinstance(value, tupleof(T1, T2, ..., TN))
    """

    def __init__(self, *item_types):
        self.item_types = item_types

    def __instancecheck__(self, value):
        return (isinstance(value, tuple) and
                len(value) == len(self.item_types) and
                all(isinstance(item, item_type)
                    for item, item_type in zip(value, self.item_types)))


class dictof(object):
    """
    Checks if a value is a dictionary with keys and elements of
    the specified types.

    Usage::
    
        isinstance(value, dictof(T1, T2))
    """

    def __init__(self, key_type, item_type):
        self.key_type = key_type
        self.item_type = item_type

    def __instancecheck__(self, value):
        return (isinstance(value, dict) and
                all(isinstance(key, self.key_type) and
                    isinstance(value[key], self.item_type)
                    for key in value))


class subclassof(object):
    """
    Check if a value is a subclass of the specified class.

    Usage::

        isinstance(value, subclassof(T))
    """

    def __init__(self, class_type):
        self.class_type = class_type

    def __instancecheck__(self, value):
        return (isinstance(value, type) and issubclass(value, self.class_type))


class filelike(object):
    """
    Checks if a value is a file or a file-like object.

    Usage::
    
        isinstance(value, filelike())
    """

    def __instancecheck__(self, value):
        return (hasattr(value, 'read') or hasattr(value, 'write'))


def aresubclasses(subclasses, superclasses):
    """
    Takes two lists; checks if each element of the first list is
    a subclass of the corresponding element in the second list.

    `subclasses` (a sequence of types)
        A list of potential subclasses.

    `superclasses` (a sequence of types)
        A list of potential superclasses.

    Returns ``True`` if the check succeeds; ``False`` otherwise.
    """
    return (len(subclasses) == len(superclasses) and
            all(issubclass(subclass, superclass)
                for subclass, superclass in zip(subclasses, superclasses)))


def isfinite(value):
    """
    Verifies that the given value is a finite number.
    """
    return (isinstance(value, (int, long)) or
            (isinstance(value, float) and not math.isinf(value)
                                      and not math.isnan(value)) or
            (isinstance(value, decimal) and value.is_finite()))


#
# Text formatting.
#


def trim_doc(doc):
    """
    Unindent and remove leading and trailing blank lines.

    Useful for stripping indentation from docstrings.
    """
    assert isinstance(doc, maybe(str))
    if doc is None:
        return None
    lines = doc.splitlines()
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop(-1)
    indent = None
    for line in lines:
        short_line = line.lstrip()
        if short_line:
            line_indent = len(line)-len(short_line)
            if indent is None or line_indent < indent:
                indent = line_indent
    if indent:
        lines = [line[indent:] for line in lines]
    return "\n".join(lines)


#
# Topological sorting.
#


def toposort(elements, order, is_total=False):
    """
    Implements topological sort.

    Takes a list of elements and a partial order relation.  Returns
    the elements reordered to satisfy the given order.

    A (finite) order relation is an acyclic directed graph.

    `elements` (a list)
        A list of elements.

    `order` (a callable)
        A function ``order(element) -> [list of elements]`` representing
        the partial order relation.  For an element `x`, ``order(x)`` must
        produce a list of elements less than `x`.

    `is_total` (Boolean)
        Ensures that the given partial order is, in fact, a total order.

    This function raises :exc:`RuntimeError` if `order` is not a valid
    partial order (contains loops) or if `is_total` is set and `order`
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
    # The mapping: node -> position of the node in the original list.
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

    # Break the cycle created by recursive nested function.
    dfs = None

    return ordered


#
# Cached property decorator.
#


class cachedproperty(object):

    def __init__(self, fget):
        self.fget = fget

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self
        value = self.fget(obj)
        # It is a non-data descriptor (`__set__` is not defined),
        # so `__dict__` takes the precedence.
        obj.__dict__[self.fget.__name__] = value
        return value


#
# Node types with special behavior.
#


class Record(tuple):

    __slots__ = ()
    __fields__ = ()
    __dump__ = None

    @classmethod
    def make(cls, name, fields, dump=None):
        assert isinstance(name, maybe(oneof(str, unicode)))
        assert isinstance(fields, listof(maybe(oneof(str, unicode))))
        if isinstance(name, unicode):
            name = name.encode('utf-8')
        if name is not None and not re.match(r'^(?!\d)\w+$', name):
            name = None
        if name is not None and keyword.iskeyword(name):
            name = name+'_'
        if name is None:
            name = cls.__name__
        duplicates = set()
        for idx, field in enumerate(fields):
            if field is None:
                continue
            if isinstance(field, unicode):
                field = field.encode('utf-8')
            if not re.match(r'^(?!\d)\w+$', field):
                field = None
            elif field.startswith('__'):
                field = None
            else:
                if keyword.iskeyword(field):
                    field = field+'_'
                if field in duplicates:
                    field = None
                fields[idx] = field
                duplicates.add(field)
        bases = (cls,)
        attributes = {}
        attributes['__slots__'] = ()
        attributes['__fields__'] = tuple(fields)
        for idx, field in enumerate(fields):
            if field is None:
                continue
            attributes[field] = property(operator.itemgetter(idx))
        if dump is not None:
            attributes['__dump__'] = dump
        return type(name, bases, attributes)

    def __new__(cls, *args, **kwds):
        if kwds:
            args_tail = []
            for field in cls.__fields__[len(args):]:
                if field not in kwds:
                    if field is not None:
                        raise TypeError("Missing argument %r" % field)
                    else:
                        raise TypeError("Missing argument #%d"
                                        % (len(args)+len(args_tail)+1))
                args_tail.append(kwds.pop(field))
            if kwds:
                field = sorted(kwds)[0]
                if field in cls.__fields__:
                    raise TypeError("Duplicate argument %r" % field)
                else:
                    raise TypeError("Unknown argument %r" % field)
            args = args+tuple(args_tail)
        if len(args) != len(cls.__fields__):
            raise TypeError("Expected %d arguments, got %d"
                            % (len(cls.__fields__), len(args)))
        return tuple.__new__(cls, args)

    def __getnewargs__(self):
        return tuple(self)

    def __str__(self):
        if self.__class__.__dump__ is None:
            return repr(self)
        return self.__class__.__dump__(self)

    def __repr__(self):
        return ("%s(%s)"
                % (self.__class__.__name__,
                   ", ".join("%s=%r" % (name or '[%s]' % idx, value)
                             for idx, (name, value)
                                in enumerate(zip(self.__fields__, self)))))


class Printable(object):
    """
    Implements default string representation.
    """

    def __str__(self):
        # Default implementation; override in subclasses.
        return "[%s]" % id(self)

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self)


class Clonable(object):
    """
    Implements an immutable clonable object.
    """

    def __init__(self):
        # Subclasses must define the `__init__` method.  Moreover, the
        # arguments of `__init__` must be assigned to respective object
        # attributes unchanged (or if changed, must still be in the form
        # acceptable by the constructor).
        raise NotImplementedError()

    def clone(self, **replacements):
        """
        Clones the node assigning new values to selected attributes.

        Returns a new object of the same type keeping original attributes
        except those for which new values are specified.

        `replacements` (a dictionary)
            A mapping: attribute -> value containing new attribute values.
        """
        # A shortcut: if there are no replacements, we could reuse
        # the same object.
        if not replacements:
            return self
        # Otherwise, reuse a more general method.
        return self.clone_to(self.__class__, **replacements)

    def clone_to(self, clone_type, **replacements):
        """
        Clones the node changing its type and assigning new values to
        selected attributes.

        Returns a new object of the specified type which keeps the
        attributes of the original objects except those for which new
        values are specified.

        `clone_type` (a type)
            The type of the cloned node.

        `replacements` (a dictionary)
            A mapping: attribute -> value containing new attribute values.
        """
        # Get the list of constructor arguments.  Note that we assume
        # for each constructor argument, a node object has an attribute
        # with the same name.
        init_code = self.__init__.im_func.func_code
        # Fetch the names of regular arguments, but skip `self`.
        names = list(init_code.co_varnames[1:init_code.co_argcount])
        # Check for * and ** arguments.  We cannot properly support
        # * arguments, so just complain about it.
        assert not (init_code.co_flags & 0x04)  # CO_VARARGS
        # Check for ** arguments.  If present, they must adhere
        # the following protocol:
        # (1) The node must keep the ** dictionary as an attribute
        #     with the same name and content.
        # (2) The node must have an attribute for each entry in
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
        # If the target type differs from the node type, we need to
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
        # Call the node constructor and return a new object.
        clone = clone_type(**arguments)
        return clone


class Comparable(object):
    """
    Implements an object with by-value comparison semantics.

    A subclass of :class:`Comparable` should reimplement :meth:`__basis__`
    to produce a tuple of all object attributes which uniquely identify
    the object.

    Two :class:`Comparable` instances are considered equal if they are of
    the same type and their basis vectors are equal.

    Other attributes:

    `_hash` (an integer)
        The cached value of the object hash; computed lazily from the
        basis.

    `_basis` (a tuple)
        The cached value of the object basis.
    """


    def __hash__(self):
        return self._hash

    def __basis__(self):
        # By default, objects are compared by identity.  Reimplement
        # for by-value comparison.
        return id(self)

    @cachedproperty
    def _hash(self):
        # Calculate and cache the object hash.
        return hash(self._basis)

    @cachedproperty
    def _basis(self):
        # Get the object basis vector.
        _basis = self.__basis__()
        # Flatten and return the vector.
        if isinstance(_basis, tuple):
            elements = []
            for element in _basis:
                if isinstance(element, Comparable):
                    elements.append((element.__class__,
                                     element._hash,
                                     element._basis))
                else:
                    elements.append(element)
            _basis = tuple(elements)
        return _basis

    def __eq__(self, other):
        # We could just compare object basis vectors, but
        # for performance, we start with faster checks.
        return ((self is other) or
                (isinstance(other, Comparable) and
                 self.__class__ is other.__class__ and
                 self._hash == other._hash and
                 self._basis == other._basis))

    def __ne__(self, other):
        # Since we override `==`, we also need to override `!=`.
        return ((self is not other) and
                (not isinstance(other, Comparable) or
                 self.__class__ is not other.__class__ or
                 self._hash != other._hash and
                 self._basis != other._basis))


#
# Auto-import utility.
#


def autoimport(name):
    """
    Imports all modules (including subpackages) in a package.

    `name` (a string)
        The package name.
    """
    # Import the package itself.
    package = __import__(name, fromlist=['__name__'])
    # It must be the package we asked for.
    assert hasattr(package, '__name__') and package.__name__ == name
    # Make sure it is indeed a package (has `__name__`).
    assert hasattr(package, '__path__')
    # Get the list of modules in the package directory; prepend the module
    # names with the package name.  That also includes any subpackages.
    modules = pkgutil.walk_packages(package.__path__, name+'.')
    # Import the modules in the package.
    for importer, module_name, is_package in modules:
        __import__(module_name)


#
# Timezone implementations.
#


class UTC(datetime.tzinfo):

    def utcoffset(self, dt):
        return datetime.timedelta(0)

    def dst(self, dt):
        return datetime.timedelta(0)

    def tzname(self, dt):
        return "Z"


class FixedTZ(datetime.tzinfo):

    def __init__(self, offset):
        self.offset = offset

    def utcoffset(self, dt):
        return datetime.timedelta(minutes=self.offset)

    def dst(self, dt):
        return datetime.timedelta(0)

    def tzname(self, dt):
        hour = abs(self.offset) / 60
        minute = abs(self.offset) % 60
        sign = '+'
        if self.offset < 0:
            sign = '-'
        if minute:
            return "%s%02d:%02d" % (sign, hour, minute)
        else:
            return "%s%d" % (sign, hour)


class LocalTZ(datetime.tzinfo):

    def utcoffset(self, dt):
        if self.isdst(dt):
            return datetime.timedelta(seconds=-time.altzone)
        else:
            return datetime.timedelta(seconds=-time.timezone)

    def dst(self, dt):
        if self.isdst(dt):
            return datetime.timedelta(seconds=(time.timezone-time.altzone))
        else:
            return datetime.timedelta(0)

    def tzname(self, dt):
        if self.isdst(dt):
            offset = -time.altzone/60
        else:
            offset = -time.timezone/60
        hour = abs(offset) / 60
        minute = abs(offset) % 60
        sign = '+'
        if offset < 0:
            sign = '-'
        if minute:
            return "%s%02d:%02d" % (sign, hour, minute)
        else:
            return "%s%d" % (sign, hour)

    def isdst(self, dt):
        tt = (dt.year, dt.month, dt.day,
              dt.hour, dt.minute, dt.second,
              dt.weekday(), 0, 0)
        stamp = time.mktime(tt)
        tt = time.localtime(stamp)
        return tt.tm_isdst > 0


#
# String similarity.
#


def similar(model, sample):
    """
    Checks if `sample` is similar to `model`.
    """
    assert isinstance(model, unicode)
    assert isinstance(sample, unicode)
    if not model or not sample:
        return False
    if len(model) > 1 and sample.startswith(model):
        return True
    M = len(model)
    N = len(sample)
    threshold = 1+M/5
    INF = threshold+1
    if abs(M-N) > threshold:
        return False
    distance = {}
    for i in range(min(M, threshold)+1):
        distance[i, 0] = i
    for j in range(min(N, threshold)+1):
        distance[0, j] = j
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
    return ((M, N) in distance)


