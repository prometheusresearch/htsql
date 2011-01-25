#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.dump`
====================

This module implements the SQL serialization process.
"""


from ..util import listof, maybe
from ..adapter import Adapter, Protocol, adapts, named
from .error import SerializeError
from ..domain import (Domain, BooleanDomain, IntegerDomain, DecimalDomain,
                      FloatDomain, StringDomain, EnumDomain, DateDomain)
from .syntax import IdentifierSyntax, CallSyntax, LiteralSyntax
from .frame import (Clause, Frame, TableFrame, BranchFrame, NestedFrame,
                    SegmentFrame, QueryFrame,
                    Phrase, NullPhrase, CastPhrase, LiteralPhrase,
                    ColumnPhrase, ReferencePhrase, EmbeddingPhrase,
                    FormulaPhrase, Anchor, LeadingAnchor)
from .signature import (Signature, isformula, IsEqualSig, IsTotallyEqualSig,
                        IsInSig, IsNullSig, IfNullSig, NullIfSig, CompareSig,
                        AndSig, OrSig, NotSig)
from .plan import Plan
import StringIO
import re


class Stream(StringIO.StringIO, object):
    """
    Implements a writable file-like object.

    Use :meth:`write` to write a string to the stream.  The data is
    accumulated in an internal buffer of the stream.

    Use :meth:`flush` to get the accumulated content and truncate
    the stream.

    :class:`Stream` also provides means for automatic indentation.
    Use :meth:`indent` to set a new indentation level, :meth:`dedent`
    to revert to the previous indentation level, :meth:`newline`
    to set the position to the current indentation level.
    """
    # Note: we inherit from `object` to be able to use `super()`.

    def __init__(self):
        # Initialize the `StringIO` object.
        super(Stream, self).__init__()
        # The current cursor position.
        self.column = 0
        # The current indentation level.
        self.indentation = 0
        # The stack of previous indentation levels.
        self.indentation_stack = []

    def write(self, data):
        """
        Writes a string to the stream.
        """
        # Call `StringIO.write`, which performs the action.
        super(Stream, self).write(data)
        # Update the cursor position.  Note that we count
        # Unicode codepoints rather than bytes.
        data = data.decode('utf-8')
        if u"\n" in data:
            self.column = len(data)-data.rindex(u"\n")-1
        else:
            self.column += len(data)

    def newline(self):
        """
        Sets the cursor to the current indentation level.
        """
        if self.column <= self.indentation:
            self.write(" "*(self.indentation-self.column))
        else:
            self.write("\n"+" "*self.indentation)

    def indent(self):
        """
        Sets the indentation level to the current cursor position.
        """
        self.indentation_stack.append(self.indentation)
        self.indentation = self.column

    def dedent(self):
        """
        Reverts to the previous indentation level.
        """
        self.indentation = self.indentation_stack.pop()

    def flush(self):
        """
        Returns the accumulated content and truncates the stream.
        """
        # FIXME: we override the builtin `StringIO.flush()`
        # (which is no-op though)

        # Make sure the indentation level is at zero position.
        assert self.indentation == 0 and not self.indentation_stack
        # The accumulated content of the stream.
        output = self.getvalue()
        # Blank the stream and return the content.
        self.truncate(0)
        self.column = 0
        return output


class Hook(object):
    """
    Encapsulates serializing hints and instructions.

    `with_aliases` (Boolean)
        If set, indicates that the generated ``SELECT`` clause
        must contain aliases.
    """

    def __init__(self, with_aliases):
        assert isinstance(with_aliases, bool)
        self.with_aliases = with_aliases


class SerializingState(object):
    """
    Encapsulates the state of the serializing process.

    State attributes:

    `stream` (:class:`Stream`)
        A file-like object accumulating the generated SQL statement.

    `frame_by_tag` (a mapping: integer -> :class:`htsql.tr.frame.Frame`)
        Maps the frame tag to the respective frame.

    `select_aliases_by_tag` (a mapping: integer -> a list of aliases)
        Maps the frame tag to the list of aliases for the frame
        ``SELECT`` clause.

    `frame_alias_by_tag` (a mapping: integer -> an alias)
        Maps the frame tag to the alias of the frame.

    `hook` (:class:`Hook`)
        Encapsulates serializing hints and directives.
    """

    def __init__(self):
        # The stream that accumulates the generated SQL.
        self.stream = Stream()
        # A mapping: tag -> frame.
        self.frame_by_tag = {}
        # A mapping: tag -> a list of `SELECT` aliases.
        self.select_aliases_by_tag = {}
        # A mapping: tag -> the frame alias.
        self.frame_alias_by_tag = {}
        # The stack of previous values of serializing directives.
        self.hook_stack = []
        # The active serializing hints and directives.
        self.hook = None

    def set_tree(self, frame):
        """
        Initializes the serializing state.

        This method must be called before dumping any clauses.

        `frame` (:class:`htsql.tr.frame.SegmentFrame`)
            The term corresponding to the top-level ``SELECT`` statement.
        """
        assert isinstance(frame, SegmentFrame)
        # Initialize serializing directives.
        self.hook = Hook(with_aliases=False)
        # Populate `frame_by_tag` mapping: use BFS over the frame tree.
        queue = [frame]
        while queue:
            frame = queue.pop(0)
            self.frame_by_tag[frame.tag] = frame
            queue.extend(frame.kids)

    def push_hook(self, with_aliases):
        """
        Updates serializing directives.

        `with_aliases` (Boolean)
            If set, indicates that the ``SELECT`` clause must have aliases.
        """
        assert isinstance(with_aliases, bool)
        self.hook_stack.append(self.hook)
        self.hook = Hook(with_aliases=with_aliases)

    def pop_hook(self):
        """
        Restores the previous serializing directives.
        """
        self.hook = self.hook_stack.pop()

    def flush(self):
        """
        Clears the serializing state and returns the generated SQL.
        """
        # Revert all attributes to their pristine state.  We assume
        # that the `hook_stack` must be already empty.
        self.frame_by_tag = {}
        self.select_aliases_by_tag = {}
        self.frame_alias_by_tag = {}
        assert not self.hook_stack
        self.hook = None
        # Truncate the stream and return the accumulated data.
        return self.stream.flush()

    def serialize(self, clause):
        """
        Serializes the given clause.

        `clause` (:class:`htsql.tr.frame.Clause`)
            The clause to serialize.
        """
        # Realize and call the `Serialize` adapter.
        return serialize(clause, self)

    def dump(self, clause):
        """
        Writes SQL for the given clause.

        `clause` (:class:`htsql.tr.frame.Clause`)
            The clause to dump.
        """
        # Realize and call the `Dump` adapter.
        dump = Dump(clause, self)
        # Note: returns `None`.
        return dump()

    def dub(self, clause):
        """
        Generates a preform alias for the given clause.

        `clause` (:class:`htsql.tr.frame.Clause`)
            The clause to generate an alias for.
        """
        # Realize and call the `Dub` adapter.
        dub = Dub(clause, self)
        return dub()


class Serialize(Adapter):
    """
    Translates a clause to SQL.

    This is an interface adapter; see subclasses for implementations.

    The :class:`Serialize` has the following signature::

        Serialize: (Clause, SerializingState) -> SQL

    The adapter is polymorphic on the `Clause` argument.

    `clause` (:class:`htsql.tr.frame.Clause`)
        The clause to serialize.

    `state` (:class:`SerializingState`)
        The current state of the serializing process.
    """

    adapts(Clause)

    def __init__(self, clause, state):
        assert isinstance(clause, Clause)
        assert isinstance(state, SerializingState)
        self.clause = clause
        self.state = state

    def __call__(self):
        # Must be implemented in subclasses.
        raise NotImplementedError("the serialize adapter is not implemented"
                                  " for a %r node" % self.clause)


class SerializeQuery(Serialize):
    """
    Serializes an HTSQL query to an execution plan.
    """

    adapts(QueryFrame)

    def __call__(self):
        # When exists, serialize the query segment.
        sql = None
        if self.clause.segment is not None:
            sql = self.state.serialize(self.clause.segment)
        # Produce an execution plan.
        return Plan(sql, self.clause)


class SerializeSegment(Serialize):
    """
    Serializes an HTSQL segment to SQL.

    Class attributes:

    `max_alias_length` (an integer)
        The maximum length of an alias.
    """

    adapts(SegmentFrame)

    # The maximum length of an alias.  We could choose an arbitrary
    # value and let the backends override it.  Here we have chosen
    # the PostgreSQL limit `NAMEDATALEN-1`.
    max_alias_length = 63

    def __call__(self):
        # Populate the `frame_by_tag` mapping.
        self.state.set_tree(self.clause)
        # Generate `SELECT` and `FROM` aliases.
        self.aliasing()
        # Dump the `SELECT` statement.
        self.state.dump(self.clause)
        # Retrieve and return the generated SQL.
        sql = self.state.flush()
        return sql

    def aliasing(self, frame=None,
                 taken_select_aliases=None,
                 taken_include_aliases=None):
        """
        Generates ``SELECT`` and ``FROM`` aliases.

        `frame` (:class:`htsql.tr.frame.BranchFrame` or ``None``)
            The frame to generate aliases for.

        `taken_select_aliases` (a set or ``None``)
            The ``SELECT`` aliases to avoid.  Note that :meth:`aliasing`
            may update the collection.

        `taken_include_aliases` (a set of ``None``)
            The ``FROM`` aliases to avoid.  Note that :meth:`aliasing`
            may update the collection.
        """
        # Initialize default values for the arguments.
        if frame is None:
            frame = self.clause
        if taken_select_aliases is None:
            taken_select_aliases = set()
        if taken_include_aliases is None:
            taken_include_aliases = set()
        # Get preform aliases for the `SELECT` phrases.
        select_names = [self.state.dub(phrase)
                        for phrase in frame.select]
        # Complete and assign the `SELECT` aliases.
        select_aliases = self.names_to_aliases(select_names,
                                               taken_select_aliases)
        self.state.select_aliases_by_tag[frame.tag] = select_aliases
        # Get preform aliases for the `FROM` subframes.
        include_names = [self.state.dub(anchor.frame)
                         for anchor in frame.include]
        # Complete and assign the `FROM` aliases.
        include_aliases = self.names_to_aliases(include_names,
                                                taken_include_aliases)
        for alias, anchor in zip(include_aliases, frame.include):
            self.state.frame_alias_by_tag[anchor.frame.tag] = alias
        # Generate aliases for each sub-`SELECT` in the `FROM` clause.
        for anchor in frame.include:
            if anchor.frame.is_branch:
                self.aliasing(anchor.frame)
        # Generate aliases for the embedded subframes.  Since embedded
        # subframes may refer to its parent frame's subframes, we need
        # to pass the aliases reserved by the parent frame.
        # FIXME: only need to pass `FROM` aliases since `SELECT` aliases
        # are not referrable from an embedded subframe?  It's a moot
        # point since embedded subframes do not use `SELECT` aliases anyway.
        for subframe in frame.embed:
            self.aliasing(subframe,
                          taken_select_aliases.copy(),
                          taken_include_aliases.copy())

    def names_to_aliases(self, names, taken_aliases):
        # Converts a list of preform aliases to actual aliases.

        # We got a list of proposed aliases, now we need to amend it
        # to make sure that:
        # - all aliases are unique among themselves and do not clash
        #   with any reserved aliases;
        # - the length of an alias does not exceed `max_alias_length`.
        # We generate aliases of the form `preform_N`, where `N`
        # is a sequiental numeric index; however, when possible, we try
        # to avoid adding the `_N` suffix.

        # A mapping: preform alias -> the next suffix to try.
        next_number_by_name = {}
        # Initialize the mapping with `1` for duplicate preforms.
        duplicates = set()
        for name in names:
            if name in duplicates:
                next_number_by_name[name] = 1
            duplicates.add(name)

        # The generated aliases.
        aliases = []
        # For each preform, generate an alias.
        for name in names:
            # The generated alias.
            alias = None
            # Try it until the alias is generated.
            while alias is None:
                # Generate an alias of the form `preform` or `preform_N`,
                # where `N` are consecutive numbers.  Note that we may need
                # to cut the preform to fit the allowed alias length limit.
                number = next_number_by_name.get(name)
                if number is None:
                    alias = name[:self.max_alias_length]
                    number = 1
                else:
                    cut = self.max_alias_length - len(str(number)) - 1
                    alias = "%s_%s" % (name[:cut], number)
                    number += 1
                next_number_by_name[name] = number
                # Check if the alias is already reserved.
                if alias in taken_aliases:
                    alias = None
            # A unique alias is generated; save and reserve it.
            aliases.append(alias)
            taken_aliases.add(alias)

        return aliases


class DumpBase(Adapter):
    """
    Translates a clause node to SQL.

    This is a base class for the family of `Dump` adapters; it encapsulates
    methods and attributes shared between these adapters.

    A `Dump` adapter generates a SQL expression for the given frame or
    phrase clause node and writes it to the stream that accumulates a SQL
    statement.

    `clause` (:class:`htsql.tr.frame.Clause`)
        A clause to serialize.

    `state` (:class:`SerializingState`)
        The current state of the serializing process.
    """

    # A pattern to match expressions of the forms:
    #   {name}
    #   {name:kind}
    #   {name:kind{modifier}}
    #   ...a chunk of text without {}...
    template_pattern = r"""
        \{
            (?P<name> \w+ )
            (?:
                :
                (?P<kind> \w+ )
                (?:
                    \{
                        (?P<modifier> [^{}]* )
                    \}
                )?
            )?
        \}
        |
        (?P<chunk> (?: [^{}] | \{\{ | \}\} )+ )
    """
    template_regexp = re.compile(template_pattern, re.X)

    def __init__(self, clause, state):
        assert isinstance(clause, Clause)
        assert isinstance(state, SerializingState)
        self.clause = clause
        self.state = state
        self.stream = state.stream

    def __call__(self):
        # By default, generate an error.
        raise SerializeError("unable to serialize an expression",
                             self.clause.mark)

    def format(self, template, *namespaces, **keywords):
        """
        Serializes a set of variables according to a template.

        The :meth:`format` method expects a template string containing
        variable fields denoted by ``{}``.  The method writes the string
        to the SQL stream replacing variables with respective values.

        The format of the variable fields is::

            {name}
            {name:kind}
            {name:kind{modifier}}

        Here,

        * `name` is the name of the variable;
        * `kind` indicates how the variable is converted to a string;
        * `modifier` is an optional conversion parameter.

        The :class:`Format` protocol defines various conversion methods.

        `template` (a string)
            A template string.

        `namespaces`, `keywords` (dictionaries)
            Dictionaries containing substitution variables.
        """
        assert isinstance(template, str)
        # Aggregate variables from the given namespaces.  A namespace is
        # either a dictionary or an object with variables as attributes.
        variables = {}
        for namespace in namespaces:
            if isinstance(namespace, dict):
                variables.update(namespace)
            else:
                assert hasattr(namespace, '__dict__')
                variables.update(namespace.__dict__)
        variables.update(keywords)
        # Parse the template string till the end.
        start = 0
        while start < len(template):
            # Extract the next part from the template string.  It is
            # either a chunk of regular text or a variable field.
            match = self.template_regexp.match(template, start)
            assert match is not None, (template, start)
            start = match.end()
            # Is it a regular text?  Write it to the stream then.
            # Note that we have escaping rules:
            #   {{ -> {
            #   }} -> }
            chunk = match.group('chunk')
            if chunk is not None:
                chunk = chunk.replace("{{", "{").replace("}}", "}")
                self.stream.write(chunk)
            # It must be variable substitution then.  Extract the value
            # of the variable and realize a `Format` instance to perform
            # the substitution.
            else:
                name = match.group('name')
                kind = match.group('kind')
                if kind is None:
                    kind = 'default'
                modifier = match.group('modifier')
                assert name in variables, name
                value = variables[name]
                format = Format(kind, value, modifier, self.state)
                format()

    def write(self, data):
        """
        Writes a string to the SQL stream.
        """
        self.stream.write(data)

    def indent(self):
        """
        Sets the indentation level to the current cursor position.
        """
        self.stream.indent()

    def dedent(self):
        """
        Reverts to the previous indentation level.
        """
        self.stream.dedent()

    def newline(self):
        """
        Sets the cursor to the current indentation level.
        """
        self.stream.newline()


class Format(Protocol):
    """
    Serializes a substitution variable.

    This is an auxiliary protocol used by :meth:`DumpBase.format`.  It is
    called to serialize a substitution variable of the form::

        {variable:kind{modifier}}

    `kind` (a string)
        The name of the conversion operation.  The protocol is polymorphic
        on this argument.

    `value` (an object)
        The value of the variable to be serialized.

    `modifier` (an object or ``None``)
        An optional conversion parameter.

    `state` (:class:`SerializingState`)
        The current state of the serializing process.
    """

    def __init__(self, kind, value, modifier, state):
        assert isinstance(kind, str)
        assert isinstance(state, SerializingState)
        self.kind = kind
        self.value = value
        self.modifier = modifier
        self.state = state
        self.stream = state.stream

    def __call__(self):
        # Must be overridden in subclasses.
        raise NotImplementedError("the format %r is not implemented"
                                  % self.kind)


class FormatDefault(Format):
    """
    Dumps a clause node.

    This is the default conversion, used when the conversion kind is
    not specified explicitly::

        {clause}

    Here, the value of `clause` is a :class:`htsql.tr.frame.Clause`
    instance to serialize.
    """

    named('default')

    def __init__(self, kind, value, modifier, state):
        assert isinstance(value, Clause)
        assert modifier is None
        super(FormatDefault, self).__init__(kind, value, modifier, state)

    def __call__(self):
        # Delegate serialization to the `Dump` adapter.
        self.state.dump(self.value)


class FormatUnion(Format):
    """
    Dumps a list of clause nodes.

    Usage::

        {clauses:union}
        {clauses:union{ separator }}

    Here,

    * the value of the `clauses` variable is a list of
      :class:`htsql.tr.frame.Clause` nodes;
    * `separator` is a separator between clauses (``', '``, by default).
    """

    named('union')

    def __init__(self, kind, value, modifier, state):
        assert isinstance(value, listof(Clause)) and len(value) > 0
        assert isinstance(modifier, maybe(str))
        # The default separator is `', '`.
        if modifier is None:
            modifier = ", "
        super(FormatUnion, self).__init__(kind, value, modifier, state)

    def __call__(self):
        # Dump:
        #   <clause><separator><clause><separator>...
        for index, phrase in enumerate(self.value):
            if index > 0:
                self.stream.write(self.modifier)
            self.state.dump(phrase)


class FormatName(Format):
    """
    Dumps a SQL identifier.

    Usage::

        {identifier:name}

    The value of `identifier` is a string, which is serialized
    as a quoted SQL identifier.
    """

    named('name')

    def __init__(self, kind, value, modifier, state):
        assert isinstance(value, str)
        assert modifier is None
        # This is the last place where we could prevent an injection attack,
        # so check that the string is well-formed.
        assert "\0" not in value
        assert len(value) > 0
        value.decode('utf-8')
        super(FormatName, self).__init__(kind, value, modifier, state)

    def __call__(self):
        # Assume standard SQL quoting rules for identifiers:
        # - an identifier is enclosed by `"`;
        # - any `"` character must be replaced with `""`.
        # A backend with non-standard quoting rules must override this method.
        self.stream.write("\"%s\"" % self.value.replace("\"", "\"\""))


class FormatLiteral(Format):
    """
    Dumps a SQL literal.

    Usage::

        {value:literal}

    The value of the `value` variable is serialized as a quoted SQL literal.
    """

    named('literal')

    def __init__(self, kind, value, modifier, state):
        assert isinstance(value, str)
        assert modifier is None
        # This is the last place where we could prevent an injection attack,
        # so check that the string is well-formed.
        assert "\0" not in value
        value.decode('utf-8')
        super(FormatLiteral, self).__init__(kind, value, modifier, state)

    def __call__(self):
        # Assume standard SQL quoting rules for literals:
        # - a value is enclosed by `'`;
        # - any `'` character must be replaced with `''`.
        # A backend with non-standard quoting rules must override this method.
        self.stream.write("'%s'" % self.value.replace("'", "''"))


class FormatNot(Format):
    """
    Dumps a ``NOT`` clause.

    Usage::

        {polarity:not}

    The action depends on the value of the `polarity` variable:

    * writes nothing when `polarity` is equal to ``+1``;
    * writes ``NOT`` followed by a space when `polarity` is equal to ``-1``.
    """

    named('not')

    def __init__(self, kind, value, modifier, state):
        assert value in [+1, -1]
        assert modifier is None
        super(FormatNot, self).__init__(kind, value, modifier, state)

    def __call__(self):
        # For `{polarity:not}`, dump:
        #   polarity=+1 => ""
        #   polarity=-1 => "NOT "
        if self.value < 0:
            self.stream.write("NOT ")


class FormatSwitch(Format):
    """
    Dumps one of two given clauses.

    Usage::

        {polarity:switch{P|N}}

    The action depends on the value of the `polarity` variable:

    * writes the ``P`` clause when `polarity` is equal to ``+1``;
    * writes the ``N`` clause when `polarity` is equal to ``-1``.
    """

    named('switch')

    def __init__(self, kind, value, modifier, state):
        assert value in [+1, -1]
        assert isinstance(modifier, str) and modifier.count("|")
        super(FormatSwitch, self).__init__(kind, value, modifier, state)

    def __call__(self):
        # For `{polarity:switch{P|N}}`, dump
        #   polarity=+1 => P
        #   polarity=-1 => N
        positive, negative = self.modifier.split("|")
        if self.value > 0:
            self.stream.write(positive)
        else:
            self.stream.write(negative)


class FormatPass(Format):
    """
    Dumps the given string.

    Usage::

        {string:pass}

    The value of the `string` variable is written directly to the SQL stream.
    """

    named('pass')

    def __init__(self, kind, value, modifier, state):
        assert isinstance(value, str)
        assert modifier is None
        super(FormatPass, self).__init__(kind, value, modifier, state)

    def __call__(self):
        self.stream.write(self.value)


class Dub(Adapter):
    """
    Generates a preform alias name for a clause node.

    This is an auxiliary adapter used to generate aliases for
    ``SELECT`` and ``FROM`` clauses.

    `clause` (:class:`htsql.tr.frame.Clause`)
        A clause node to make an alias for.

    `state` (:class:`SerializingState`)
        The current state of the serializing process.
    """

    adapts(Clause)

    def __init__(self, clause, state):
        self.clause = clause
        self.state = state

    def __call__(self):
        # The adapter must never fail, so the default implementation
        # provides a valid, though meaningless, alias name.
        return "!"


class Dump(DumpBase):
    """
    Translates a clause node to SQL.

    This is an interface adapter; see subclasses for implementations.

    The :class:`Dump` adapter has the following signature::

        Dump: (Clause, SerializingState) -> writes to SQL stream

    The adapter is polymorphic on the `Clause` argument.

    The adapter generates a SQL clause for the given node and
    writes it to the stream that accumulates a SQL statement.

    `clause` (:class:`htsql.tr.frame.Clause`)
        A clause to serialize.

    `state` (:class:`SerializingState`)
        The current state of the serializing process.
    """

    adapts(Clause)


class DubFrame(Dub):
    """
    Generates a preform alias for a frame node.
    """

    adapts(Frame)

    def __call__(self):
        # For a frame, a good alias is the name of the table
        # represented by the frame.
        space = self.clause.space
        if space.table is not None:
            return space.table.name
        # Use the default alias when the frame does not represent
        # any table.
        return super(DubFrame, self).__call__()


class DumpFrame(Dump):
    """
    Translates a frame node to SQL.

    `frame` (:class:`htsql.tr.frame.Frame`)
        A frame node to serialize.
    """

    adapts(Frame)

    def __init__(self, frame, state):
        super(DumpFrame, self).__init__(frame, state)
        self.frame = frame


class DubPhrase(Dub):
    """
    Generates a preform alias for a phrase node.
    """

    adapts(Phrase)

    def __call__(self):
        # Generate a useful alias for a phrase node.  We base the alias
        # on the underlying syntax node.
        syntax = self.clause.syntax
        # For an identifier node, take the identifier name.
        if isinstance(syntax, IdentifierSyntax):
            return syntax.value
        # For a function call node, take the function name.
        if isinstance(syntax, CallSyntax):
            return syntax.name
        # For a literal node, take the value of the literal.
        if isinstance(syntax, LiteralSyntax):
            return syntax.value
        # Otherwise, use the default alias.
        return super(DubPhrase, self).__call__()


class DumpPhrase(Dump):
    """
    Translates a phrase node to SQL.

    `phrase` (:class:`htsql.tr.frame.Phrase`)
        A phrase node to serialize.
    """

    adapts(Phrase)

    def __init__(self, phrase, state):
        super(DumpPhrase, self).__init__(phrase, state)
        self.phrase = phrase


class DumpTable(Dump):
    """
    Serializes a table frame.
    """

    adapts(TableFrame)

    def __call__(self):
        # Serialize a table reference in a `FROM` clause.  Dump:
        #   <schema>.<table>
        # Must be overridden for backends which lack schemas.
        table = self.frame.space.table
        self.format("{schema:name}.{table:name}",
                    schema=table.schema_name,
                    table=table.name)


class DumpBranch(Dump):
    """
    Serializes a ``SELECT`` frame.
    """

    adapts(BranchFrame)

    def __call__(self):
        # Sequentially serialize the clauses of the `SELECT` frame.
        self.dump_select()
        self.dump_include()
        self.dump_where()
        self.dump_group()
        self.dump_having()
        self.dump_order()
        self.dump_limit()

    def dump_select(self):
        # Serialize a `SELECT` clause.  Dump:
        #   SELECT <phrase> AS <alias>,
        #          <phrase> AS <alias>,
        #          ...

        # The `SELECT` aliases for the current frame.
        aliases = self.state.select_aliases_by_tag[self.frame.tag]
        # Write `SELECT` and set the indentation level to the position
        # after `SELECT`.
        self.write("SELECT ")
        self.indent()
        # Serialize the selection phrases.
        for index, phrase in enumerate(self.frame.select):
            # Check if we need to provide an alias, and if so, fetch it.
            alias = None
            if self.state.hook.with_aliases:
                alias = aliases[index]
                # Now even if we are required to provide an alias, we may
                # still be able to omit it if it coincides with an implicit
                # alias generated by the SQL engine.  It may happen in two
                # cases.
                # (1) The selection refers to a column, and the alias
                #     coincides with the column name.
                if isinstance(phrase, ColumnPhrase):
                    if alias == phrase.column.name:
                        alias = None
                # (2) The selection exports a value from a nested `SELECT`,
                #     and the alias coincides with the alias of the nested
                #     selection.
                if isinstance(phrase, ReferencePhrase):
                    target_alias = (self.state.select_aliases_by_tag
                                            [phrase.tag][phrase.index])
                    if alias == target_alias:
                        alias = None
            # Write the selection and, if needed, its alias.
            if alias is not None:
                self.format("{selection} AS {alias:name}",
                            selection=phrase, alias=alias)
            else:
                self.format("{selection}",
                            selection=phrase)
            # Write the trailing comma.
            if index < len(self.frame.select)-1:
                self.write(",")
                self.newline()
        # Restore the original indentation level.
        self.dedent()

    def dump_include(self):
        # Serialize a `FROM` clause.  Dump:
        #   FROM <leading_anchor>
        #        <anchor>
        #        ...

        # Nothing two write if there are no nested subframes.
        if not self.frame.include:
            return
        # Write `FROM` and set the indentation level to the next position.
        self.newline()
        self.write("FROM ")
        self.indent()
        # Serialize the subframes.
        for index, anchor in enumerate(self.frame.include):
            self.format("{anchor}", anchor=anchor)
        # Restore the original indentation level.
        self.dedent()

    def dump_where(self):
        # Serialize a `WHERE` clause.  Dump:
        #   WHERE <phrase>
        # or, if the top-level phrase is an `AND` operator,
        #   WHERE <op>
        #         AND <op>
        #         ...

        # Nothing to write if there is no `WHERE` condition.
        if self.frame.where is None:
            return
        self.newline()
        # Handle the case when the condition is an `AND` operator.
        if isformula(self.frame.where, AndSig):
            self.write("WHERE ")
            self.indent()
            for index, op in enumerate(self.frame.where.ops):
                self.format("{op}", op=op)
                if index < len(self.frame.where.ops)-1:
                    self.newline()
                    self.write("AND ")
            self.dedent()
        # Handle the regular case.
        else:
            self.format("WHERE {condition}",
                        condition=self.frame.where)

    def dump_group(self):
        # Serialize a `GROUP BY` clause.  Dump:
        #   GROUP BY <phrase>, ...

        # Nothing to write if there is no `GROUP BY` items.
        if not self.frame.group:
            return
        self.newline()
        self.write("GROUP BY ")
        # Write the `GROUP BY` items.
        for index, phrase in enumerate(self.frame.group):
            # SQL syntax allows us to refer to a `SELECT` column in
            # a `GROUP BY` clause by position.  Thus, when a `GROUP BY`
            # element coincides with some `SELECT` phrase, we could avoid
            # serializing the same phrase twice.
            if phrase in self.frame.select:
                position = self.frame.select.index(phrase)+1
                self.write(str(position))
            # Otherwise, just serialize the phrase.
            else:
                self.format("{kernel}", kernel=phrase)
            # Dump the trailing comma.
            if index < len(self.frame.group)-1:
                self.write(", ")

    def dump_having(self):
        # Serialize a `HAVING` clause.  Dump:
        #   HAVING <phrase>
        # or, if the top-level phrase is an `AND` operator,
        #   HAVING <op>
        #          AND <op>
        #          ...

        # Note: currently unused as the assembler never generates a `HAVING`
        # clause.

        # Nothing to write if there is no `HAVING` condition.
        if self.frame.having is None:
            return
        self.newline()
        # Handle the case when the condition is an `AND` operator.
        if isformula(self.frame.having, AndSig):
            self.write("HAVING ")
            self.indent()
            for index, op in enumerate(self.frame.having.ops):
                self.format("{op}", op=op)
                if index < len(self.frame.having.ops)-1:
                    self.newline()
                    self.write("AND ")
            self.dedent()
        # Handle the regular case.
        else:
            self.format("HAVING {condition}",
                        condition=self.frame.having)

    def dump_order(self):
        # Serialize an `ORDER BY` clause.  Dump:
        #   ORDER BY <phrase> (ASC|DESC), ...
        # Note: the default serializer assumes that the `ASC` modifier
        # lists `NULL` values first, and the `DESC` modifier lists `NULL`
        # values last.  Backends for which it is not so must override
        # this method.

        # Nothing to write if there is no `ORDER BY` items.
        if not self.frame.order:
            return
        self.newline()
        self.format("ORDER BY ")
        # Write the `GROUP BY` items.
        for index, (phrase, direction) in enumerate(self.frame.order):
            # Just as with `GROUP BY`, an `ORDER BY` item could refer
            # to a `SELECT` column by position.  We do it when possible
            # to avoid serializing the same phrase node twice.
            if phrase in self.frame.select:
                position = self.frame.select.index(phrase)+1
                self.write(str(position))
            # The regular case: serialize the node.
            else:
                self.format("{kernel}", kernel=phrase)
            # Write the direction modifier.
            self.format(" {direction:switch{ASC|DESC}}", direction=direction)
            # Write the trailing comma.
            if index < len(self.frame.order)-1:
                self.write(", ")

    def dump_limit(self):
        # Serialize `LIMIT` and `OFFSET` clauses.  Dump:
        #   LIMIT <limit>
        #   OFFSET <offset>
        # Note: this syntax is commonly used, but not standard.  A backend
        # with a different syntax for `LIMIT` and `SELECT` clauses must
        # override this method.

        # Nothing to write if there is no `LIMIT` or `OFFSET` clause.
        if self.frame.limit is None and self.frame.offset is None:
            return
        # Dump a `LIMIT` clause.
        if self.frame.limit is not None:
            self.newline()
            self.format("LIMIT "+str(self.frame.limit))
        # Dump an `OFFSET` clause.
        if self.frame.offset is not None:
            self.newline()
            self.format("OFFSET "+str(self.frame.offset))


class DumpNested(Dump):
    """
    Serializes a nested ``SELECT`` frame.
    """

    adapts(NestedFrame)

    def __call__(self):
        # Dump:
        #   (SELECT ...
        #    ...)
        self.format("(")
        self.indent()
        super(DumpNested, self).__call__()
        self.dedent()
        self.format(")")


class DumpSegment(Dump):
    """
    Serializes a top-level ``SELECT`` frame.
    """

    adapts(SegmentFrame)

    def __call__(self):
        super(DumpSegment, self).__call__()
        # FIXME: add a semicolon?
        # Make sure the statement ends with a new line.
        self.newline()


class DumpLeadingAnchor(Dump):
    """
    Serializes the leading subframe in a ``FROM`` clause.
    """

    adapts(LeadingAnchor)

    def __call__(self):
        # Dump:
        #   <frame> AS <alias>
        alias = self.state.frame_alias_by_tag[self.clause.frame.tag]
        self.state.push_hook(with_aliases=True)
        self.format("{frame} AS {alias:name}",
                    frame=self.clause.frame, alias=alias)
        self.state.pop_hook()


class DumpAnchor(Dump):
    """
    Serializes a successive subframe in a ``FROM`` clause.
    """

    adapts(Anchor)

    def __call__(self):
        # Dump:
        #   (CROSS|INNER|...) JOIN <frame> AS <alias>
        #                          ON <condition>
        alias = self.state.frame_alias_by_tag[self.clause.frame.tag]
        self.newline()
        if self.clause.is_cross:
            self.write("CROSS JOIN ")
        elif self.clause.is_inner:
            self.write("INNER JOIN ")
        elif self.clause.is_left and not self.clause.is_right:
            self.write("LEFT OUTER JOIN ")
        elif self.clause.is_right and not self.clause.is_left:
            self.write("RIGHT OUTER JOIN ")
        else:
            self.write("FULL OUTER JOIN ")
        self.indent()
        self.state.push_hook(with_aliases=True)
        self.format("{frame} AS {alias:name}",
                    frame=self.clause.frame, alias=alias)
        self.state.pop_hook()
        if self.clause.condition is not None:
            self.newline()
            self.format("ON {condition}",
                        condition=self.clause.condition)
        self.dedent()


class DubColumn(Dub):
    """
    Generates a preform alias for a column reference.
    """

    adapts(ColumnPhrase)

    def __call__(self):
        # Use the name of the column as an alias.
        return self.clause.column.name


class DumpColumn(Dump):
    """
    Serializes a reference to a table frame.
    """

    adapts(ColumnPhrase)

    def __call__(self):
        # Dump:
        #   <alias>.<column>
        parent = self.state.frame_alias_by_tag[self.phrase.tag]
        child = self.phrase.column.name
        self.format("{parent:name}.{child:name}",
                    parent=parent, child=child)


class DubReference(Dub):
    """
    Generates a preform alias for a reference to a nested ``SELECT``.
    """

    adapts(ReferencePhrase)

    def __call__(self):
        # Use the same alias as the target phrase.
        frame = self.state.frame_by_tag[self.clause.tag]
        phrase = frame.select[self.clause.index]
        return self.state.dub(phrase)


class DumpReference(Dump):
    """
    Serializes a reference to a nested subframe.
    """

    adapts(ReferencePhrase)

    def __call__(self):
        # Dump:
        #   <frame>.<column>
        parent = self.state.frame_alias_by_tag[self.phrase.tag]
        select_aliases = self.state.select_aliases_by_tag[self.phrase.tag]
        child = select_aliases[self.phrase.index]
        self.format("{parent:name}.{child:name}",
                    parent=parent, child=child)


class DubEmbedding(Dub):
    """
    Generates a preform alias for a correlated subquery.
    """

    adapts(EmbeddingPhrase)

    def __call__(self):
        # Use the same alias as the (only) output column of the subquery.
        frame = self.state.frame_by_tag[self.clause.tag]
        [phrase] = frame.select
        return self.state.dub(phrase)


class DumpEmbedding(Dump):
    """
    Serializes an embedded subquery.
    """

    adapts(EmbeddingPhrase)

    def __call__(self):
        # Fetch and serialize the suframe.
        frame = self.state.frame_by_tag[self.phrase.tag]
        self.state.push_hook(with_aliases=False)
        self.format("{frame}", frame=frame)
        self.state.pop_hook()


class DumpLiteral(Dump):
    """
    Serializes a literal node.

    Serialiation is delegated to the :class:`DumpByDomain` adapter.
    """

    adapts(LiteralPhrase)

    def __call__(self):
        # Delegate serialization to `DumpByDomain`.
        dump = DumpByDomain(self.phrase, self.state)
        return dump()


class DumpNull(Dump):
    """
    Serializes a ``NULL`` value.
    """

    adapts(NullPhrase)

    def __call__(self):
        # We serialize a `NULL` value here to avoid checking for
        # `NULL` in every implementation of `DumpByDomain`.
        # FIXME: This assumes that we never need to add an explicit type
        # specifier to a `NULL` value.
        self.write("NULL")


class DumpByDomain(DumpBase):
    """
    Serializes a literal node.

    This is an auxiliary adapter used for serialization of literal nodes.
    The adapter is polymorphic on the domain of the literal.

    `phrase` (:class:`htsql.tr.frame.LiteralPhrase`)
        A literal node to serialize.

    `state` (:class:`SerializingState`)
        The current state of the serializing process.

    Other attributes:

    `value` (depends on the domain or ``None``)
        The value of the literal.

    `domain` (:class:`htsql.domain.Domain`)
        The domain of the literal.
    """

    adapts(Domain)

    @classmethod
    def dispatch(interface, phrase, *args, **kwds):
        # Dispatch the adapter on the domain of the literal node.
        assert isinstance(phrase, LiteralPhrase)
        return (type(phrase.domain),)

    def __init__(self, phrase, state):
        assert isinstance(phrase, LiteralPhrase)
        assert phrase.value is not None
        super(DumpByDomain, self).__init__(phrase, state)
        self.phrase = phrase
        # Extract commonly used attributes of the literal node.
        self.value = phrase.value
        self.domain = phrase.domain


class DumpBoolean(DumpByDomain):
    """
    Serializes a Boolean literal.
    """

    adapts(BooleanDomain)

    def __call__(self):
        # Use the SQL standard constants: `TRUE` and `FALSE`.
        # Backends not supporting those must override this implementation.
        if self.value is True:
            self.write("TRUE")
        if self.value is False:
            self.write("FALSE")


class DumpInteger(DumpByDomain):
    """
    Serializes an integer literal.
    """

    adapts(IntegerDomain)

    def __call__(self):
        # Dump an integer number.  A backend may override this implementation
        # to support a different range of integer values.

        # We assume that the database supports 8-byte signed integer values
        # natively and complain if the value is out of this range.
        if not (-2**63 <= self.value < 2**63):
            raise SerializeError("invalid integer value",
                                 self.phrase.mark)
        # Write the number.
        self.write(str(self.value))


class DumpFloat(DumpByDomain):
    """
    Serializes a float literal.
    """

    adapts(FloatDomain)

    def __call__(self):
        # Dump a floating-point number.  A backend may override this method
        # to support a different range of floating-point values or to
        # provide an exact type specifier.

        # Last check that we didn't get a non-number.
        # FIXME: Python 2.5/win32?
        assert str(self.value) not in ['inf', '-inf', 'nan']
        # Write the standard representation of the number assuming that
        # the database could figure out its type from the context.
        self.write(repr(self.value))


class DumpDecimal(DumpByDomain):
    """
    Serializes a decimal literal.
    """

    adapts(DecimalDomain)

    def __call__(self):
        # Dump a decimal number.  A backend may override this method
        # to support a different range of values or to add an exact
        # type specifier.

        # Last check that we didn't get a non-number.
        assert self.value.is_finite()
        # Write the standard representation of the number assuming that
        # the database could figure out its type from the context.
        self.write(str(self.value))


class DumpString(DumpByDomain):
    """
    Serializes a string literal.
    """

    adapts(StringDomain)

    def __call__(self):
        # Dump the value as a quoted literal.
        self.format("{value:literal}", value=self.value)


class DumpEnum(DumpByDomain):
    """
    Serializes a value of an enumerated type.
    """

    adapts(EnumDomain)

    def __call__(self):
        # There is no an enumerated type in the SQL standard, but most
        # backends which support enumerated types accept quoted literals.
        self.format("{value:literal}", value=self.value)


class DumpDate(DumpByDomain):
    """
    Serializes a date literal.
    """

    adapts(DateDomain)

    def __call__(self):
        # Dump:
        #   DATE 'YYYY-MM-DD'
        # A backend with a different (or no) date represetation may need
        # to override this implementation.
        self.format("DATE {value:literal}", value=str(self.value))


class DumpCast(Dump):
    """
    Serializes a ``CAST`` clause.

    Serialiation is delegated to the :class:`DumpByDomain` adapter.
    """

    adapts(CastPhrase)

    def __call__(self):
        # Delegate serialization to `DumpToDomain`.
        dump = DumpToDomain(self.phrase, self.state)
        return dump()


class DumpToDomain(DumpBase):
    """
    Serializes a ``CAST`` clause.

    This is an auxiliary adapter used for serialization of cast phrase nodes.
    The adapter is polymorphic on the pair of the origin and the target
    domains.

    `phrase` (:class:`htsql.tr.frame.CastPhrase`)
        A cast node to serialize.

    `state` (:class:`SerializingState`)
        The current state of the serializing process.

    Other attributes:

    `base` (:class:`htsql.tr.frame.Phrase`)
        The operand of the ``CAST`` expression.

    `domain` (:class:`htsql.domain.Domain`)
        The target domain.
    """

    adapts(Domain, Domain)

    @classmethod
    def dispatch(interface, phrase, *args, **kwds):
        # Dispatch the adapter on the origin and the target domains
        # of the cast.
        assert isinstance(phrase, CastPhrase)
        return (type(phrase.base.domain), type(phrase.domain))

    def __init__(self, phrase, state):
        assert isinstance(phrase, CastPhrase)
        super(DumpToDomain, self).__init__(phrase, state)
        self.phrase = phrase
        # Extract commonly used attributes.
        self.base = phrase.base
        self.domain = phrase.domain


class DumpToInteger(DumpToDomain):
    """
    Serializes conversion to an integer value.

    Handles conversion from a string and other numeric data types.
    """

    adapts(Domain, IntegerDomain)

    def __call__(self):
        # Dump:
        #   CAST(<base> AS INTEGER)
        # A backend with no integer data type or an integer data type
        # with a different name needs to override this implementation.
        self.format("CAST({base} AS INTEGER)", base=self.base)


class DumpToFloat(DumpToDomain):
    """
    Serializes conversion to a floating-point number.

    Handles conversion from a string and other numeric data types.
    """

    adapts(Domain, FloatDomain)

    def __call__(self):
        # Dump:
        #   CAST(<base> AS DOUBLE PRECISION)
        # A backend with no floating-point data type or a floating-point
        # data type with a different name needs to override this
        # implementation.
        self.format("CAST({base} AS DOUBLE PRECISION)", base=self.base)


class DumpToDecimal(DumpToDomain):
    """
    Serializes conversion to a decimal number.

    Handles conversion from a string and other numeric data types.
    """

    adapts(Domain, DecimalDomain)

    def __call__(self):
        # Dump:
        #   CAST(<base> AS DECIMAL)
        # A backend with no decimal data type or a decimal data type
        # with a different name needs to override this implementation.
        self.format("CAST({base} AS DECIMAL)", base=self.base)


class DumpToString(DumpToDomain):
    """
    Serializes conversion to a string.

    Handles conversion from other data types to a string.
    """

    adapts(Domain, StringDomain)

    def __call__(self):
        # Dump:
        #   CAST(<base> AS CHARACTER VARYING)
        # A backend that supports many character types may choose a different
        # target data type.
        self.format("CAST({base} AS CHARACTER VARYING)", base=self.base)


class DumpToDate(DumpToDomain):
    """
    Serializes conversion to a date value.

    Handles conversion from a string.
    """

    adapts(Domain, DateDomain)

    def __call__(self):
        self.format("CAST({base} AS DATE)", base=self.base)


class DumpFormula(Dump):
    """
    Serializes a formula node.

    Serialiation is delegated to the :class:`DumpBySignature` adapter.
    """

    adapts(FormulaPhrase)

    def __call__(self):
        # Delegate serialization to `DumpBySignature`.
        dump = DumpBySignature(self.phrase, self.state)
        return dump()


class DumpBySignature(DumpBase):
    """
    Serializes a formula node.

    This is an auxiliary adapter used for serialization of formula nodes.
    The adapter is polymorphic on the formula signature.

    `phrase` (:class:`htsql.tr.frame.FormulaPhrase`)
        A formula node to serialize.

    `state` (:class:`SerializingState`)
        The current state of the serializing process.

    Other attributes:

    `signature` (:class:`htsql.tr.signature.Signature`)
        The signature of the formula.

    `domain` (:class:`htsql.tr.domain.Domain`)
        The co-domain of the formula.

    `arguments` (:class:`htsql.tr.signature.Bag`)
        The arguments of the formula.
    """

    adapts(Signature)

    @classmethod
    def dispatch(interface, phrase, *args, **kwds):
        # Dispatch the adapter on the signature of the formula.
        assert isinstance(phrase, FormulaPhrase)
        return (type(phrase.signature),)

    def __init__(self, phrase, state):
        assert isinstance(phrase, FormulaPhrase)
        super(DumpBySignature, self).__init__(phrase, state)
        self.phrase = phrase
        # Extract commonly used attributes of the formula.
        self.signature = phrase.signature
        self.domain = phrase.domain
        self.arguments = phrase.arguments


class DumpIsEqual(DumpBySignature):
    """
    Serializes an (in)equality (``=`` or ``!=``) operator
    """

    adapts(IsEqualSig)

    def __call__(self):
        # Dump:
        #   (<lop> (=|<>) <rop>)
        self.format("({lop} {polarity:switch{=|<>}} {rop})",
                    self.arguments, self.signature)


class DumpIsTotallyEqual(DumpBySignature):
    """
    Serializes a total (in)equality (``==`` or ``!==``) operator.
    """

    adapts(IsTotallyEqualSig)

    def __call__(self):
        # Dump:
        #   (<lop> IS (NOT) DISTINCT FROM <rop>)
        # Note that many backends do not support `IS DISTINCT FROM`
        # operator and need to reimplement this implementation using
        # the regular equality operator and `IS NULL`.
        self.format("({lop} IS {polarity:not}DISTINCT FROM {rop})",
                    self.arguments, polarity=-self.signature.polarity)


class DumpIsIn(DumpBySignature):
    """
    Serializes an N-ary equality (``={}``) operator.
    """

    adapts(IsInSig)

    def __call__(self):
        # Dump:
        #   (<lop> (NOT) IN (rop1, rop2, ...))
        self.format("({lop} {polarity:not}IN ({rops:union{, }}))",
                    self.arguments, self.signature)


class DumpAnd(DumpBySignature):
    """
    Serializes a logical "AND" (``&``) operator.
    """

    adapts(AndSig)

    def __call__(self):
        # Dump:
        #   (<op1> AND <op2> AND ...)
        self.format("({ops:union{ AND }})", self.arguments)


class DumpOr(DumpBySignature):
    """
    Serializes a logical "OR" (``|``) operator.
    """

    adapts(OrSig)

    def __call__(self):
        # Dump:
        #   (<op1> OR <op2> OR ...)
        self.format("({ops:union{ OR }})", self.arguments)


class DumpNot(DumpBySignature):
    """
    Serializes a logical "NOT" (``!``) operator.
    """

    adapts(NotSig)

    def __call__(self):
        # Dump:
        #   (NOT <op>)
        self.format("(NOT {op})", self.arguments)


class DumpIsNull(DumpBySignature):
    """
    Serializes an ``is_null()`` operator.
    """

    adapts(IsNullSig)

    def __call__(self):
        # Dump:
        #   (<op> IS (NOT) NULL)
        self.format("({op} IS {polarity:not}NULL)",
                    self.arguments, self.signature)


class DumpIfNull(DumpBySignature):
    """
    Serializes an ``if_null()`` operator.
    """

    adapts(IfNullSig)

    def __call__(self):
        # Dump:
        #   COALESCE(<lop>, <rop>)
        self.format("COALESCE({lop}, {rop})", self.arguments)


class DumpNullIf(DumpBySignature):
    """
    Serializes a ``null_if()`` operator.
    """

    adapts(NullIfSig)

    def __call__(self):
        # Dump:
        #   NULLIF(<lop>, <rop>)
        self.format("NULLIF({lop}, {rop})", self.arguments)


class DumpCompare(DumpBySignature):
    """
    Serializes a comparison operator.
    """

    adapts(CompareSig)

    def __call__(self):
        # Dump:
        #   (<lop> (<|<=|>|>=) <rop>)
        self.format("({lop} {relation:pass} {rop})",
                    self.arguments, self.signature)


def serialize(clause, state=None):
    """
    Translates a clause node to SQL.

    `clause` (:class:`htsql.tr.frame.Clause`)
        The clause to serialize.

    `state` (:class:`SerializingState` or ``None``)
        The serializing state to use.  If not set, a new serializing
        state is instantiated.
    """
    # Create a new serializing state if necessary.
    if state is None:
        state = SerializingState()
    # Realize and apply the `Serialize` adapter.
    serialize = Serialize(clause, state)
    return serialize()


