#
# Copyright (c) 2006-2010, Prometheus Research, LLC
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
from .error import DumpError
from ..domain import (Domain, BooleanDomain, NumberDomain, IntegerDomain,
                      DecimalDomain, FloatDomain, StringDomain, EnumDomain,
                      DateDomain)
from .syntax import IdentifierSyntax, CallSyntax, LiteralSyntax
from .frame import (Clause, Frame, LeafFrame, ScalarFrame, TableFrame,
                    BranchFrame, NestedFrame, SegmentFrame, QueryFrame,
                    Phrase, NullPhrase, CastPhrase, LiteralPhrase,
                    ColumnPhrase, ReferencePhrase, EmbeddingPhrase,
                    FormulaPhrase, Anchor, LeadingAnchor)
from .signature import (Signature, IsEqualSig, IsTotallyEqualSig, IsInSig,
                        IsNullSig, IfNullSig, NullIfSig, CompareSig,
                        AndSig, OrSig, NotSig)
from .plan import Plan
import decimal
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


class SerializingState(object):

    def __init__(self):
        self.stream = Stream()
        self.frame_by_tag = {}
        self.select_aliases_by_tag = {}
        self.frame_alias_by_tag = {}
        self.with_aliases_stack = []
        self.with_aliases = False

    def push_with_aliases(self, with_aliases):
        assert isinstance(with_aliases, bool)
        self.with_aliases_stack.append(self.with_aliases)
        self.with_aliases = with_aliases

    def pop_with_aliases(self):
        self.with_aliases = self.with_aliases_stack.pop()

    def flush(self):
        self.frame_by_tag = {}
        self.select_aliases_by_tag = {}
        self.frame_alias_by_tag = {}
        self.with_aliases_stack = []
        self.with_aliases = False
        return self.stream.flush()

    def set_tree(self, frame):
        queue = [frame]
        while queue:
            frame = queue.pop(0)
            self.frame_by_tag[frame.tag] = frame
            queue.extend(frame.kids)

    def serialize(self, clause):
        return serialize(clause, self)

    def dump(self, clause):
        dump = Dump(clause, self)
        return dump()

    def dub(self, clause):
        dub = Dub(clause, self)
        return dub()


class Serialize(Adapter):

    adapts(Clause)

    def __init__(self, clause, state):
        assert isinstance(clause, Clause)
        assert isinstance(state, SerializingState)
        self.clause = clause
        self.state = state

    def __call__(self):
        raise NotImplementedError(repr(self.clause))


class SerializeQuery(Serialize):

    adapts(QueryFrame)

    def __call__(self):
        sql = None
        if self.clause.segment is not None:
            sql = self.state.serialize(self.clause.segment)
        return Plan(self.clause, sql, self.clause.mark)


class SerializeSegment(Serialize):

    adapts(SegmentFrame)

    max_alias_length = 63

    def __call__(self):
        self.state.set_tree(self.clause)
        self.aliasing()
        self.state.dump(self.clause)
        sql = self.state.flush()
        return sql

    def aliasing(self, frame=None,
                 taken_select_aliases=None,
                 taken_include_aliases=None):
        if frame is None:
            frame = self.clause
        if taken_select_aliases is None:
            taken_select_aliases = set()
        if taken_include_aliases is None:
            taken_include_aliases = set()
        select_names = [self.state.dub(phrase)
                        for phrase in frame.select]
        select_aliases = self.names_to_aliases(select_names,
                                               taken_select_aliases)
        self.state.select_aliases_by_tag[frame.tag] = select_aliases
        include_names = [self.state.dub(anchor.frame)
                         for anchor in frame.include]
        include_aliases = self.names_to_aliases(include_names,
                                                taken_include_aliases)
        for alias, anchor in zip(include_aliases, frame.include):
            self.state.frame_alias_by_tag[anchor.frame.tag] = alias
        for anchor in frame.include:
            if anchor.frame.is_branch:
                self.aliasing(anchor.frame)
        for subframe in frame.embed:
            self.aliasing(subframe,
                          taken_select_aliases.copy(),
                          taken_include_aliases.copy())

    def names_to_aliases(self, names, taken_aliases):
        next_number_by_name = {}
        duplicates = set()
        for name in names:
            if name in duplicates:
                next_number_by_name[name] = 1
            duplicates.add(name)
        aliases = []
        for name in names:
            alias = None
            while alias is None:
                number = next_number_by_name.get(name)
                if number is None:
                    alias = name[:self.max_alias_length]
                    number = 1
                else:
                    cut = self.max_alias_length - len(str(number)) - 1
                    alias = "%s_%s" % (name[:cut], number)
                    number += 1
                next_number_by_name[name] = number
                if alias in taken_aliases:
                    alias = None
            aliases.append(alias)
            taken_aliases.add(alias)
        return aliases


class DumpBase(Adapter):

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
        (?P<chunk> [^{}]+ )
    """
    template_regexp = re.compile(template_pattern, re.X)

    def __init__(self, clause, state):
        assert isinstance(clause, Clause)
        assert isinstance(state, SerializingState)
        self.clause = clause
        self.state = state
        self.stream = state.stream

    def __call__(self):
        raise NotImplementedError(repr(self.clause))

    def format(self, template, *args, **kwds):
        variables = {}
        for arg in args:
            if isinstance(arg, dict):
                variables.update(arg)
            else:
                assert hasattr(arg, '__dict__')
                variables.update(arg.__dict__)
        variables.update(kwds)
        start = 0
        while start < len(template):
            match = self.template_regexp.match(template, start)
            assert match is not None, (template, start)
            start = match.end()
            chunk = match.group('chunk')
            if chunk is not None:
                self.stream.write(chunk)
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
        self.stream.write(data)

    def indent(self):
        self.stream.indent()

    def dedent(self):
        self.stream.dedent()

    def newline(self):
        self.stream.newline()


class Format(Protocol):

    def __init__(self, name, value, modifier, state):
        assert isinstance(name, str)
        assert isinstance(state, SerializingState)
        self.name = name
        self.value = value
        self.modifier = modifier
        self.state = state
        self.stream = state.stream

    def __call__(self):
        raise NotImplementedError(self.name)


class FormatDefault(Format):

    named('default')

    def __init__(self, name, value, modifier, state):
        assert isinstance(value, Clause)
        assert modifier is None
        super(FormatDefault, self).__init__(name, value, modifier, state)

    def __call__(self):
        assert isinstance(self.value, Clause)
        assert self.modifier is None
        self.state.dump(self.value)


class FormatUnion(Format):

    named('union')

    def __init__(self, name, value, modifier, state):
        assert isinstance(value, listof(Clause))
        assert isinstance(modifier, maybe(str))
        if modifier is None:
            modifier = ", "
        super(FormatUnion, self).__init__(name, value, modifier, state)

    def __call__(self):
        for index, phrase in enumerate(self.value):
            if index > 0:
                self.stream.write(self.modifier)
            self.state.dump(phrase)


class FormatName(Format):

    named('name')

    def __init__(self, name, value, modifier, state):
        assert isinstance(value, str)
        assert modifier is None
        assert "\0" not in value
        assert len(value) > 0
        value.decode('utf-8')
        super(FormatName, self).__init__(name, value, modifier, state)

    def __call__(self):
        self.stream.write("\"%s\"" % self.value.replace("\"", "\"\""))


class FormatLiteral(Format):

    named('literal')

    def __init__(self, name, value, modifier, state):
        assert isinstance(value, str)
        assert modifier is None
        assert "\0" not in value
        value.decode('utf-8')
        super(FormatLiteral, self).__init__(name, value, modifier, state)

    def __call__(self):
        self.stream.write("'%s'" % self.value.replace("'", "''"))


class FormatNot(Format):

    named('not')

    def __init__(self, name, value, modifier, state):
        assert value in [+1, -1]
        assert modifier is None
        super(FormatNot, self).__init__(name, value, modifier, state)

    def __call__(self):
        if self.value == -1:
            self.stream.write("NOT ")


class FormatPass(Format):

    named('pass')

    def __init__(self, name, value, modifier, state):
        assert isinstance(value, str)
        assert modifier is None
        super(FormatPass, self).__init__(name, value, modifier, state)

    def __call__(self):
        self.stream.write(self.value)


class Dub(Adapter):

    adapts(Clause)

    def __init__(self, clause, state):
        self.clause = clause
        self.state = state

    def __call__(self):
        return "!"


class Dump(DumpBase):

    adapts(Clause)


class DubFrame(Dub):

    adapts(Frame)

    def __init__(self, frame, state):
        super(DubFrame, self).__init__(frame, state)
        self.frame = frame
        self.term = frame.term
        self.space = frame.space
        self.binding = frame.binding
        self.syntax = frame.syntax

    def __call__(self):
        if self.space.table is not None:
            return self.space.table.name
        return super(DubFrame, self).__call__()


class DumpFrame(Dump):

    adapts(Frame)

    def __init__(self, frame, state):
        super(DumpFrame, self).__init__(frame, state)
        self.frame = frame


class DubPhrase(Dub):

    adapts(Phrase)

    def __init__(self, phrase, state):
        super(DubPhrase, self).__init__(phrase, state)
        self.phrase = phrase
        self.expression = phrase.expression
        self.binding = phrase.binding
        self.syntax = phrase.syntax

    def __call__(self):
        if isinstance(self.syntax, IdentifierSyntax):
            return self.syntax.value
        if isinstance(self.syntax, CallSyntax):
            return self.syntax.name
        if isinstance(self.syntax, LiteralSyntax):
            return self.syntax.value
        return super(DubPhrase, self).__call__()


class DumpPhrase(Dump):

    adapts(Phrase)

    def __init__(self, phrase, state):
        super(DumpPhrase, self).__init__(phrase, state)
        self.phrase = phrase


class DumpTable(Dump):

    adapts(TableFrame)

    def __call__(self):
        table = self.frame.space.table
        self.format("{schema:name}.{table:name}",
                    schema=table.schema_name,
                    table=table.name)


class DumpBranch(Dump):

    adapts(BranchFrame)

    def __call__(self):
        self.dump_select()
        self.dump_include()
        self.dump_where()
        self.dump_group()
        self.dump_having()
        self.dump_order()
        self.dump_limit()

    def dump_select(self):
        aliases = self.state.select_aliases_by_tag[self.frame.tag]
        self.format("SELECT ")
        self.indent()
        for index, phrase in enumerate(self.frame.select):
            if self.state.with_aliases:
                alias = aliases[index]
                self.format("{selection} AS {alias:name}",
                            selection=phrase, alias=alias)
            else:
                self.format("{selection}",
                            selection=phrase)
            if index < len(self.frame.select)-1:
                self.format(",")
                self.newline()
        self.dedent()

    def dump_include(self):
        if not self.frame.include:
            return
        self.newline()
        self.format("FROM ")
        self.indent()
        for index, anchor in enumerate(self.frame.include):
            self.format("{anchor}", anchor=anchor)
        self.dedent()

    def dump_where(self):
        if self.frame.where is None:
            return
        self.newline()
        self.format("WHERE {condition}",
                    condition=self.frame.where)

    def dump_group(self):
        if not self.frame.group:
            return
        self.newline()
        self.format("GROUP BY ")
        for index, phrase in enumerate(self.frame.group):
            if phrase in self.frame.select:
                position = self.frame.select.index(phrase)+1
                self.format(str(position))
            else:
                self.format("{kernel}", kernel=phrase)
            if index < len(self.frame.group)-1:
                self.format(", ")

    def dump_having(self):
        if self.frame.having is None:
            return
        self.newline()
        self.format("HAVING {condition}",
                    condition=self.frame.having)

    def dump_order(self):
        if not self.frame.order:
            return
        self.newline()
        self.format("ORDER BY ")
        for index, (phrase, direction) in enumerate(self.frame.order):
            if phrase in self.frame.select:
                position = self.frame.select.index(phrase)+1
                self.format(str(position))
            else:
                self.format("{kernel}", kernel=phrase)
            if direction == +1:
                self.format(" ASC")
            if direction == -1:
                self.format(" DESC")
            if index < len(self.frame.order)-1:
                self.format(", ")

    def dump_limit(self):
        if self.frame.limit is None and self.frame.offset is None:
            return
        if self.frame.limit is not None:
            self.newline()
            self.format("LIMIT "+str(self.frame.limit))
        if self.frame.offset is not None:
            self.newline()
            self.format("OFFSET "+str(self.frame.offset))


class DumpNested(Dump):

    adapts(NestedFrame)

    def __call__(self):
        self.format("(")
        self.indent()
        super(DumpNested, self).__call__()
        self.dedent()
        self.format(")")


class DumpSegment(Dump):

    adapts(SegmentFrame)

    def __call__(self):
        super(DumpSegment, self).__call__()
        self.newline()


class DumpLeadingAnchor(Dump):

    adapts(LeadingAnchor)

    def __call__(self):
        alias = self.state.frame_alias_by_tag[self.clause.frame.tag]
        self.format("{frame} AS {alias:name}",
                    frame=self.clause.frame, alias=alias)


class DumpAnchor(Dump):

    adapts(Anchor)

    def __call__(self):
        alias = self.state.frame_alias_by_tag[self.clause.frame.tag]
        self.newline()
        if self.clause.is_cross:
            self.format("    CROSS JOIN")
        elif self.clause.is_inner:
            self.format("    INNER JOIN")
        elif self.clause.is_left and not self.clause.is_right:
            self.format("    LEFT OUTER JOIN")
        elif self.clause.is_right and not self.clause.is_left:
            self.format("    RIGHT OUTER JOIN")
        self.newline()
        self.state.push_with_aliases(True)
        self.format("{frame} AS {alias:name}",
                    frame=self.clause.frame, alias=alias)
        self.state.pop_with_aliases()
        if self.clause.condition is not None:
            self.newline()
            self.format("    ON ({condition})",
                        condition=self.clause.condition)


class DubColumn(Dub):

    adapts(ColumnPhrase)

    def __call__(self):
        return self.phrase.column.name


class DumpColumn(Dump):

    adapts(ColumnPhrase)

    def __call__(self):
        parent = self.state.frame_alias_by_tag[self.phrase.tag]
        child = self.phrase.column.name
        self.format("{parent:name}.{child:name}",
                    parent=parent, child=child)


class DubReference(Dub):

    adapts(ReferencePhrase)

    def __call__(self):
        frame = self.state.frame_by_tag[self.phrase.tag]
        phrase = frame.select[self.phrase.index]
        return self.state.dub(phrase)


class DumpReference(Dump):

    adapts(ReferencePhrase)

    def __call__(self):
        parent = self.state.frame_alias_by_tag[self.phrase.tag]
        select_aliases = self.state.select_aliases_by_tag[self.phrase.tag]
        child = select_aliases[self.phrase.index]
        self.format("{parent:name}.{child:name}",
                    parent=parent, child=child)


class DubEmbedding(Dub):

    adapts(EmbeddingPhrase)

    def __call__(self):
        frame = self.state.frame_by_tag[self.phrase.tag]
        phrase = frame.select[0]
        return self.state.dub(phrase)


class DumpEmbedding(Dump):

    adapts(EmbeddingPhrase)

    def __call__(self):
        frame = self.state.frame_by_tag[self.phrase.tag]
        self.state.push_with_aliases(False)
        self.format("{frame}", frame=frame)
        self.state.pop_with_aliases()


class DumpLiteral(Dump):

    adapts(LiteralPhrase)

    def __call__(self):
        dump = DumpByDomain(self.phrase, self.state)
        return dump()


class DumpNull(Dump):

    adapts(NullPhrase)

    def __call__(self):
        self.format("NULL")


class DumpByDomain(DumpBase):

    adapts(Domain)

    @classmethod
    def dispatch(interface, phrase, *args, **kwds):
        assert isinstance(phrase, LiteralPhrase)
        return (type(phrase.domain),)

    def __init__(self, phrase, state):
        assert isinstance(phrase, LiteralPhrase)
        assert phrase.value is not None
        super(DumpByDomain, self).__init__(phrase, state)
        self.phrase = phrase
        self.value = phrase.value
        self.domain = phrase.domain


class DumpBoolean(DumpByDomain):

    adapts(BooleanDomain)

    def __call__(self):
        if self.value is True:
            self.format("(1 = 1)")
        if self.value is False:
            self.format("(1 = 0)")


class DumpInteger(DumpByDomain):

    adapts(IntegerDomain)

    def __call__(self):
        if not (-2**63 <= self.value < 2**63):
            raise DumpError("invalid integer value",
                            self.phrase.mark)
        self.format(str(self.value))


class DumpFloat(DumpByDomain):

    adapts(FloatDomain)

    def __call__(self):
        if str(self.value) in ['inf', '-inf', 'nan']:
            raise DumpError("invalid float value",
                            self.phrase.mark)
        self.format(repr(self.value))


class DumpDecimal(DumpByDomain):

    adapts(DecimalDomain)

    def __call__(self):
        self.format(str(self.value))


class DumpString(DumpByDomain):

    adapts(StringDomain)

    def __call__(self):
        self.format("{value:literal}", value=self.value)


class DumpEnum(DumpByDomain):

    adapts(EnumDomain)

    def __call__(self):
        self.format("{value:literal}", value=self.value)


class DumpCast(Dump):

    adapts(CastPhrase)

    def __call__(self):
        dump = DumpToDomain(self.phrase, self.state)
        return dump()


class DumpToDomain(DumpBase):

    adapts(Domain, Domain)

    @classmethod
    def dispatch(interface, phrase, *args, **kwds):
        assert isinstance(phrase, CastPhrase)
        return (type(phrase.base.domain), type(phrase.domain))

    def __init__(self, phrase, state):
        assert isinstance(phrase, CastPhrase)
        super(DumpToDomain, self).__init__(phrase, state)
        self.phrase = phrase
        self.base = phrase.base
        self.domain = phrase.domain


class DumpToInteger(DumpToDomain):

    adapts(Domain, IntegerDomain)

    def __call__(self):
        self.format("CAST({base} AS INTEGER)", base=self.base)


class DumpToFloat(DumpToDomain):

    adapts(Domain, FloatDomain)

    def __call__(self):
        self.format("CAST({base} AS DOUBLE PRECISION)", base=self.base)


class DumpToDecimal(DumpToDomain):

    adapts(Domain, DecimalDomain)

    def __call__(self):
        self.format("CAST({base} AS DECIMAL)", base=self.base)


class DumpToString(DumpToDomain):

    adapts(Domain, StringDomain)

    def __call__(self):
        self.format("CAST({base} AS CHARACTER VARYING)", base=self.base)


class DumpFormula(Dump):

    adapts(FormulaPhrase)

    def __call__(self):
        dump = DumpBySignature(self.phrase, self.state)
        return dump()


class DumpBySignature(DumpBase):

    adapts(Signature)

    @classmethod
    def dispatch(interface, phrase, *args, **kwds):
        assert isinstance(phrase, FormulaPhrase)
        return (type(phrase.signature),)

    def __init__(self, phrase, state):
        assert isinstance(phrase, FormulaPhrase)
        super(DumpBySignature, self).__init__(phrase, state)
        self.phrase = phrase
        self.signature = phrase.signature
        self.domain = phrase.domain
        self.arguments = phrase.arguments


class DumpIsEqual(DumpBySignature):

    adapts(IsEqualSig)

    def __call__(self):
        if self.signature.polarity > 0:
            self.format("({lop} = {rop})", self.arguments)
        else:
            self.format("({lop} <> {rop})", self.arguments)


class DumpIsTotallyEqual(DumpBySignature):

    adapts(IsTotallyEqualSig)

    def __call__(self):
        if self.signature.polarity > 0:
            self.format("(CASE WHEN (({lop} = {rop}) OR"
                        " (({lop} IS NULL) AND ({rop} IS NULL)))"
                        " THEN 1 ELSE 0 END)",
                        self.arguments)
        else:
            self.format("(CASE WHEN (({lop} <> {rop}) AND"
                        " (({lop} IS NOT NULL) OR ({rop} IS NOT NULL)))"
                        " THEN 1 ELSE 0 END)",
                        self.arguments)


class DumpIsIn(DumpBySignature):

    adapts(IsInSig)

    def __call__(self):
        self.format("({lop} {polarity:not}IN ({rops:union{, }}))",
                    self.arguments, self.signature)


class DumpAnd(DumpBySignature):

    adapts(AndSig)

    def __call__(self):
        self.format("({ops:union{ AND }})", self.arguments)


class DumpOr(DumpBySignature):

    adapts(OrSig)

    def __call__(self):
        self.format("({ops:union{ OR }})", self.arguments)


class DumpNot(DumpBySignature):

    adapts(NotSig)

    def __call__(self):
        self.format("(NOT {op})", self.arguments)


class DumpIsNull(DumpBySignature):

    adapts(IsNullSig)

    def __call__(self):
        self.format("({op} IS {polarity:not}NULL)",
                    self.arguments, self.signature)


class DumpIfNull(DumpBySignature):

    adapts(IfNullSig)

    def __call__(self):
        self.format("COALESCE({lop}, {rop})", self.arguments)


class DumpNullIf(DumpBySignature):

    adapts(NullIfSig)

    def __call__(self):
        self.format("NULLIF({lop}, {rop})", self.arguments)


class DumpCompare(DumpBySignature):

    adapts(CompareSig)

    def __call__(self):
        self.format("({lop} {relation:pass} {rop})",
                    self.arguments, self.signature)


def serialize(clause, state=None):
    if state is None:
        state = SerializingState()
    serialize = Serialize(clause, state)
    return serialize()


