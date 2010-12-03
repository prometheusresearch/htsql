#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.dump`
====================

This module implements the SQL serializing process.
"""


from ..util import listof
from ..adapter import Adapter, Utility, adapts
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


class Stream(object):

    def __init__(self):
        self.io = StringIO.StringIO()
        self.line = 0
        self.column = 0
        self.indentation = 0
        self.indentation_stack = []

    def write(self, data):
        self.io.write(data)
        data = data.decode('utf-8')
        self.line += data.count(u"\n")
        if u"\n" in data:
            self.column = len(data)-data.rindex(u"\n")-1
        else:
            self.column += len(data)

    def newline(self):
        if self.column <= self.indentation:
            self.write(" "*(self.indentation-self.column))
        else:
            self.write("\n"+" "*self.indentation)

    def indent(self):
        self.indentation_stack.append(self.indentation)
        self.indentation = self.column

    def dedent(self):
        self.indentation = self.indentation_stack.pop()

    def flush(self):
        output = self.io.getvalue()
        self.io = StringIO.StringIO()
        self.line = 0
        self.column = 0
        return output


class DumpingState(object):

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

    def format(self, template, *args, **kwds):
        variables = {}
        for arg in args:
            if isinstance(arg, dict):
                variables.update(arg)
            else:
                assert hasattr(arg, '__dict__')
                variables.update(arg.__dict__)
        variables.update(kwds)
        format = Format(self, template, variables)
        return format()

    def indent(self):
        self.stream.indent()

    def dedent(self):
        self.stream.dedent()

    def newline(self):
        self.stream.newline()

    def flush(self):
        return self.stream.flush()

    def set_tree(self, frame):
        queue = [frame]
        while queue:
            frame = queue.pop(0)
            self.frame_by_tag[frame.tag] = frame
            queue.extend(frame.kids)

    def dub(self, clause):
        dub = Dub(clause, self)
        return dub()

    def dump(self, clause):
        return dump(clause, self)


class Format(Utility):

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

    def __init__(self, state, template, variables):
        assert isinstance(state, DumpingState)
        assert isinstance(template, str)
        assert isinstance(variables, dict)
        self.state = state
        self.stream = state.stream
        self.template = template
        self.variables = variables

    def __call__(self):
        start = 0
        while start < len(self.template):
            match = self.template_regexp.match(self.template, start)
            assert match is not None, (self.template, start)
            start = match.end()
            chunk = match.group('chunk')
            if chunk is not None:
                self.stream.write(chunk)
            else:
                name = match.group('name')
                kind = match.group('kind')
                modifier = match.group('modifier')
                assert name in self.variables, name
                assert kind is None or hasattr(self, kind), kind
                value = self.variables[name]
                if kind is not None:
                    method = getattr(self, kind)
                else:
                    method = self.default
                if modifier is None:
                    method(value)
                else:
                    method(value, modifier)

    def default(self, value, modifier=None):
        assert isinstance(value, Clause)
        assert modifier is None
        self.state.dump(value)

    def join(self, value, modifier=", "):
        assert isinstance(value, listof(Clause))
        assert isinstance(modifier, str)
        for index, phrase in enumerate(value):
            if index > 0:
                self.stream.write(modifier)
            self.state.dump(phrase)

    def name(self, value, modifier=None):
        assert isinstance(value, str)
        assert modifier is None
        assert "\0" not in value
        assert len(value) > 0
        value.decode('utf-8')
        self.stream.write("\"%s\"" % value.replace("\"", "\"\""))

    def literal(self, value, modifier=None):
        assert isinstance(value, str)
        assert modifier is None
        assert "\0" not in value
        value.decode('utf-8')
        self.stream.write("'%s'" % value.replace("'", "''"))

    def polarity(self, value, modifier=None):
        assert value in [+1, -1]
        assert modifier is None
        if value == -1:
            self.stream.write("NOT ")

    def asis(self, value, modifier=None):
        assert isinstance(value, str)
        assert modifier is None
        self.stream.write(value)


class Dub(Adapter):

    adapts(Clause)

    def __init__(self, clause, state):
        self.clause = clause
        self.state = state

    def __call__(self):
        return "!"


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


class DubColumn(Dub):

    adapts(ColumnPhrase)

    def __call__(self):
        return self.phrase.column.name


class DubReference(Dub):

    adapts(ReferencePhrase)

    def __call__(self):
        frame = self.state.frame_by_tag[self.phrase.tag]
        phrase = frame.select[self.phrase.index]
        return self.state.dub(phrase)


class DubEmbedding(Dub):

    adapts(EmbeddingPhrase)

    def __call__(self):
        frame = self.state.frame_by_tag[self.phrase.tag]
        phrase = frame.select[0]
        return self.state.dub(phrase)


class Dump(Adapter):

    adapts(Clause)

    def __init__(self, clause, state):
        assert isinstance(clause, Clause)
        assert isinstance(state, DumpingState)
        self.clause = clause
        self.state = state

    def __call__(self):
        raise NotImplementedError(repr(self.clause))


class DumpFrame(Dump):

    adapts(Frame)

    def __init__(self, frame, state):
        super(DumpFrame, self).__init__(frame, state)
        self.frame = frame


class DumpPhrase(Dump):

    adapts(Phrase)

    def __init__(self, phrase, state):
        super(DumpPhrase, self).__init__(phrase, state)
        self.phrase = phrase


class DumpTable(Dump):

    adapts(TableFrame)

    def __call__(self):
        table = self.frame.space.table
        self.state.format("{schema:name}.{table:name}",
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
        self.state.format("SELECT ")
        self.state.indent()
        for index, phrase in enumerate(self.frame.select):
            if self.state.with_aliases:
                alias = aliases[index]
                self.state.format("{selection} AS {alias:name}",
                                  selection=phrase, alias=alias)
            else:
                self.state.format("{selection}",
                                  selection=phrase)
            if index < len(self.frame.select)-1:
                self.state.format(",")
                self.state.newline()
        self.state.dedent()

    def dump_include(self):
        if not self.frame.include:
            return
        self.state.newline()
        self.state.format("FROM ")
        self.state.indent()
        for index, anchor in enumerate(self.frame.include):
            self.state.format("{anchor}", anchor=anchor)
        self.state.dedent()

    def dump_where(self):
        if self.frame.where is None:
            return
        self.state.newline()
        self.state.format("WHERE {condition}",
                          condition=self.frame.where)

    def dump_group(self):
        if not self.frame.group:
            return
        self.state.newline()
        self.state.format("GROUP BY ")
        for index, phrase in enumerate(self.frame.group):
            if phrase in self.frame.select:
                position = self.frame.select.index(phrase)+1
                self.state.format(str(position))
            else:
                self.state.format("{kernel}", kernel=phrase)
            if index < len(self.frame.group)-1:
                self.state.format(", ")

    def dump_having(self):
        if self.frame.having is None:
            return
        self.state.newline()
        self.state.format("HAVING {condition}",
                          condition=self.frame.having)

    def dump_order(self):
        if not self.frame.order:
            return
        self.state.newline()
        self.state.format("ORDER BY ")
        for index, (phrase, direction) in enumerate(self.frame.order):
            if phrase in self.frame.select:
                position = self.frame.select.index(phrase)+1
                self.state.format(str(position))
            else:
                self.state.format("{kernel}", kernel=phrase)
            if direction == +1:
                self.state.format(" ASC")
            if direction == -1:
                self.state.format(" DESC")
            if index < len(self.frame.order)-1:
                self.state.format(", ")

    def dump_limit(self):
        if self.frame.limit is None and self.frame.offset is None:
            return
        if self.frame.limit is not None:
            self.state.newline()
            self.state.format("LIMIT "+str(self.frame.limit))
        if self.frame.offset is not None:
            self.state.newline()
            self.state.format("OFFSET "+str(self.frame.offset))


class DumpNested(Dump):

    adapts(NestedFrame)

    def __call__(self):
        self.state.format("(")
        self.state.indent()
        super(DumpNested, self).__call__()
        self.state.dedent()
        self.state.format(")")


class DumpSegment(Dump):

    adapts(SegmentFrame)

    max_alias_length = 63

    def __call__(self):
        self.aliasing()
        super(DumpSegment, self).__call__()
        self.state.newline()

    def aliasing(self, frame=None,
                 taken_select_aliases=None,
                 taken_include_aliases=None):
        if frame is None:
            frame = self.frame
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


class DumpLeadingAnchor(Dump):

    adapts(LeadingAnchor)

    def __call__(self):
        alias = self.state.frame_alias_by_tag[self.clause.frame.tag]
        self.state.format("{frame} AS {alias:name}",
                          frame=self.clause.frame, alias=alias)


class DumpAnchor(Dump):

    adapts(Anchor)

    def __call__(self):
        alias = self.state.frame_alias_by_tag[self.clause.frame.tag]
        self.state.newline()
        if self.clause.is_cross:
            self.state.format("    CROSS JOIN")
        elif self.clause.is_inner:
            self.state.format("    INNER JOIN")
        elif self.clause.is_left and not self.clause.is_right:
            self.state.format("    LEFT OUTER JOIN")
        elif self.clause.is_right and not self.clause.is_left:
            self.state.format("    RIGHT OUTER JOIN")
        self.state.newline()
        self.state.push_with_aliases(True)
        self.state.format("{frame} AS {alias:name}",
                          frame=self.clause.frame, alias=alias)
        self.state.pop_with_aliases()
        if self.clause.condition is not None:
            self.state.newline()
            self.state.format("    ON ({condition})",
                              condition=self.clause.condition)


class DumpLiteral(Dump):

    adapts(LiteralPhrase)

    def __call__(self):
        dump = DumpByDomain(self.phrase, self.state)
        return dump()


class DumpNull(Dump):

    adapts(NullPhrase)

    def __call__(self):
        self.state.format("NULL")


class DumpByDomain(Adapter):

    adapts(Domain)

    @classmethod
    def dispatch(interface, phrase, *args, **kwds):
        assert isinstance(phrase, LiteralPhrase)
        return (type(phrase.domain),)

    def __init__(self, phrase, state):
        assert isinstance(phrase, LiteralPhrase)
        assert phrase.value is not None
        assert isinstance(state, DumpingState)
        self.phrase = phrase
        self.state = state
        self.value = phrase.value
        self.domain = phrase.domain

    def __call__(self):
        raise NotImplementedError()


class DumpBoolean(DumpByDomain):

    adapts(BooleanDomain)

    def __call__(self):
        if self.value is True:
            self.state.format("(1 = 1)")
        if self.value is False:
            self.state.format("(1 = 0)")


class DumpInteger(DumpByDomain):

    adapts(IntegerDomain)

    def __call__(self):
        if not (-2**63 <= self.value < 2**63):
            raise DumpError("invalid integer value",
                                 self.phrase.mark)
        self.state.format(str(self.value))


class DumpFloat(DumpByDomain):

    adapts(FloatDomain)

    def __call__(self):
        if str(self.value) in ['inf', '-inf', 'nan']:
            raise DumpError("invalid float value",
                                 self.phrase.mark)
        self.state.format(repr(self.value))


class DumpDecimal(DumpByDomain):

    adapts(DecimalDomain)

    def __call__(self):
        self.state.format(str(self.value))


class DumpString(DumpByDomain):

    adapts(StringDomain)

    def __call__(self):
        self.state.format("{value:literal}", value=self.value)


class DumpEnum(DumpByDomain):

    adapts(EnumDomain)

    def __call__(self):
        self.state.format("{value:literal}", value=self.value)


class DumpCast(Dump):

    adapts(CastPhrase)

    def __call__(self):
        dump = DumpToDomain(self.phrase, self.state)
        return dump()


class DumpToDomain(Adapter):

    adapts(Domain, Domain)

    @classmethod
    def dispatch(interface, phrase, *args, **kwds):
        assert isinstance(phrase, CastPhrase)
        return (type(phrase.base.domain), type(phrase.domain))

    def __init__(self, phrase, state):
        assert isinstance(phrase, CastPhrase)
        assert isinstance(state, DumpingState)
        self.phrase = phrase
        self.base = phrase.base
        self.domain = phrase.domain
        self.state = state

    def __call__(self):
        raise NotImplementedError()


class DumpToInteger(DumpToDomain):

    adapts(Domain, IntegerDomain)

    def __call__(self):
        self.state.format("CAST({base} AS INTEGER)", base=self.base)


class DumpToFloat(DumpToDomain):

    adapts(Domain, FloatDomain)

    def __call__(self):
        self.state.format("CAST({base} AS DOUBLE PRECISION)", base=self.base)


class DumpToDecimal(DumpToDomain):

    adapts(Domain, DecimalDomain)

    def __call__(self):
        self.state.format("CAST({base} AS DECIMAL)", base=self.base)


class DumpToString(DumpToDomain):

    adapts(Domain, StringDomain)

    def __call__(self):
        self.state.format("CAST({base} AS CHARACTER VARYING)", base=self.base)


class DumpFormula(Dump):

    adapts(FormulaPhrase)

    def __call__(self):
        dump = DumpBySignature(self.phrase, self.state)
        return dump()


class DumpBySignature(Adapter):

    adapts(Signature)

    @classmethod
    def dispatch(interface, phrase, *args, **kwds):
        assert isinstance(phrase, FormulaPhrase)
        return (type(phrase.signature),)

    def __init__(self, phrase, state):
        assert isinstance(phrase, FormulaPhrase)
        assert isinstance(state, DumpingState)
        self.phrase = phrase
        self.state = state
        self.signature = phrase.signature
        self.domain = phrase.domain
        self.arguments = phrase.arguments

    def __call__(self):
        raise NotImplementedError()


class DumpIsEqual(DumpBySignature):

    adapts(IsEqualSig)

    def __call__(self):
        if self.signature.polarity > 0:
            self.state.format("({lop} = {rop})", self.arguments)
        else:
            self.state.format("({lop} <> {rop})", self.arguments)


class DumpIsTotallyEqual(DumpBySignature):

    adapts(IsTotallyEqualSig)

    def __call__(self):
        if self.signature.polarity > 0:
            self.state.format("(CASE WHEN (({lop} = {rop}) OR"
                              " (({lop} IS NULL) AND ({rop} IS NULL)))"
                              " THEN 1 ELSE 0 END)",
                              self.arguments)
        else:
            self.state.format("(CASE WHEN (({lop} <> {rop}) AND"
                              " (({lop} IS NOT NULL) OR ({rop} IS NOT NULL)))"
                              " THEN 1 ELSE 0 END)",
                              self.arguments)


class DumpIsIn(DumpBySignature):

    adapts(IsInSig)

    def __call__(self):
        self.state.format("({lop} {polarity:polarity}IN ({rops:join{, }}))",
                          self.arguments, self.signature)


class DumpAnd(DumpBySignature):

    adapts(AndSig)

    def __call__(self):
        self.state.format("({ops:join{ AND }})", self.arguments)


class DumpOr(DumpBySignature):

    adapts(OrSig)

    def __call__(self):
        self.state.format("({ops:join{ OR }})", self.arguments)


class DumpNot(DumpBySignature):

    adapts(NotSig)

    def __call__(self):
        self.state.format("(NOT {op})", self.arguments)


class DumpIsNull(DumpBySignature):

    adapts(IsNullSig)

    def __call__(self):
        self.state.format("({op} IS {polarity:polarity}NULL)",
                          self.arguments, self.signature)


class DumpIfNull(DumpBySignature):

    adapts(IfNullSig)

    def __call__(self):
        self.state.format("COALESCE({lop}, {rop})", self.arguments)


class DumpNullIf(DumpBySignature):

    adapts(NullIfSig)

    def __call__(self):
        self.state.format("NULLIF({lop}, {rop})", self.arguments)


class DumpCompare(DumpBySignature):

    adapts(CompareSig)

    def __call__(self):
        self.state.format("({lop} {relation:asis} {rop})",
                          self.arguments, self.signature)


class DumpColumn(Dump):

    adapts(ColumnPhrase)

    def __call__(self):
        parent = self.state.frame_alias_by_tag[self.phrase.tag]
        child = self.phrase.column.name
        self.state.format("{parent:name}.{child:name}",
                          parent=parent, child=child)


class DumpReference(Dump):

    adapts(ReferencePhrase)

    def __call__(self):
        parent = self.state.frame_alias_by_tag[self.phrase.tag]
        select_aliases = self.state.select_aliases_by_tag[self.phrase.tag]
        child = select_aliases[self.phrase.index]
        self.state.format("{parent:name}.{child:name}",
                          parent=parent, child=child)


class DumpEmbedding(Dump):

    adapts(EmbeddingPhrase)

    def __call__(self):
        frame = self.state.frame_by_tag[self.phrase.tag]
        self.state.push_with_aliases(False)
        self.state.format("{frame}", frame=frame)
        self.state.pop_with_aliases()


class DumpQuery(Dump):

    adapts(QueryFrame)

    def __call__(self):
        sql = None
        if self.clause.segment is not None:
            self.state.set_tree(self.clause.segment)
            self.state.dump(self.clause.segment)
            sql = self.state.flush()
        return Plan(self.clause, sql, self.clause.mark)


def dump(clause, state=None):
    if state is None:
        state = DumpingState()
    dump = Dump(clause, state)
    return dump()


