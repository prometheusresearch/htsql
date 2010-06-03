#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
This module provides various hard-to-categorize utilities.
"""


import re
import sys
import urllib


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

        For SQLite, the path to the database file.

    `options`
        A dictionary containing extra connection parameters.

        Currently ignored by all engines.

    The parameters `username`, `password`, `host`, `port` are
    ignored by the SQLite engine.
    """

    # List of supported and soon-to-be-supported engines.
    valid_engines = ['pgsql', 'sqlite', 'mysql',
                     'oracle', 'mssql', 'db2']

    # Regular expression for parsing a connection URI of the form:
    # 'engine://username:password@host:port/database?options'.
    key_chars = r'''[%0-9a-zA-Z_.-]+'''
    value_chars = r'''[%0-9a-zA-Z`~!#$^*()_+\\|\[\]{};'",.<>/-]+'''
    pattern = r'''(?x)
        ^
        (?P<engine> %(key_chars)s )
        ://
        (?: (?P<username> %(key_chars)s )?
            (?: : (?P<password> %(value_chars)s )? )? @ )?
        (?: (?P<host> %(key_chars)s )?
            (?: : (?P<port> %(key_chars)s )? )? )?
        /
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
        assert isinstance(engine, str) and engine in self.valid_engines
        assert isinstance(username, maybe(str))
        assert isinstance(password, maybe(str))
        assert isinstance(host, maybe(str))
        assert isinstance(port, maybe(str))
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

        # Check if the engine is valid.
        if engine not in cls.valid_engines:
            raise ValueError("invalid engine: %r" % engine)

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

    # For Python 2.5, we can't use `__instancecheck__`; in this case,
    # we let ``isinstance(oneof(...)) == isinstance(object)``.
    if sys.version_info < (2, 6):
        def __new__(cls, *args, **kwds):
            return object

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

    # For Python 2.5, we can't use `__instancecheck__`; in this case,
    # we let ``isinstance(oneof(...)) == isinstance(object)``.
    if sys.version_info < (2, 6):
        def __new__(cls, *args, **kwds):
            return object

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

    # For Python 2.5, we can't use `__instancecheck__`; in this case,
    # we let ``isinstance(listof(...)) == isinstance(list)``.
    if sys.version_info < (2, 6):
        def __new__(cls, *args, **kwds):
            return list

    def __init__(self, item_type):
        self.item_type = item_type

    def __instancecheck__(self, value):
        return (isinstance(value, list) and
                all(isinstance(item, self.item_type) for item in value))


class tupleof(object):
    """
    Checks if a value is a tuple with the fixed number of elements
    of the specified types.

    Usage::

        isinstance(value, tupleof(T1, T2, ..., TN))
    """

    # For Python 2.5, we can't use `__instancecheck__`; in this case,
    # we let ``isinstance(tupleof(...)) == isinstance(tuple)``.
    if sys.version_info < (2, 6):
        def __new__(cls, *args, **kwds):
            return tuple

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

    # For Python 2.5, we can't use `__instancecheck__`; in this case,
    # we let ``isinstance(dictof(...)) == isinstance(dict)``.
    if sys.version_info < (2, 6):
        def __new__(cls, *args, **kwds):
            return dict

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

    # For Python 2.5, we can't use `__instancecheck__`; in this case,
    # we let ``isinstance(subclassof(...)) == isinstance(type)``.
    if sys.version_info < (2, 6):
        def __new__(cls, *args, **kwds):
            return type

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

    # For Python 2.5, we can't use `__instancecheck__`; in this case,
    # we let ``isinstance(filelike()) == isinstance(object)``.
    if sys.version_info < (2, 6):
        def __new__(cls, *args, **kwds):
            return object

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


def toposort(elements, preorder):
    """
    Implements topological sort.

    Takes a list of elements and a preorder relation.  Returns
    the elements reordered to satisfy the preorder.

    A (finite) preorder relation is an acyclic directed graph.

    `elements` (a list)
        A list of elements.

    `preorder` (a callable)
        A function ``preorder(element) -> [list of elements]`` representing
        the preorder relation.  For an element `x`, ``preorder(x)`` must
        produce a list of elements less than `x`.
    """
    # For a description of the algorithm, see, for example,
    #   http://en.wikipedia.org/wiki/Topological_sorting
    # In short, we apply depth-first search to the DAG represented
    # by the preorder.  As soon as the search finishes exploring
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
        assert node not in active,  \
                "loop detected in %s" % path[path.index(node):]
        active.add(node)

        # Get the list of adjacent nodes.
        adjacents = preorder(node)
        # Sort the adjacent elements according to their order in the
        # original list.  It helps to keep the original order when possible.
        adjacents = sorted(adjacents, key=(lambda i: positions[i]))

        # Visit the adjacent nodes.
        for adjacent in adjacents:
            dfs(adjacent)

        # Add the node to the sorted list.
        ordered.append(node)

        # Remove the node from the path; add it to the set of processed nodes.
        path.pop()
        active.remove(node)
        visited.add(node)

    # Apply the DFS to the whole DAG.
    for element in elements:
        dfs(element)

    return ordered


