#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ...core.context import context
from ...core.adapter import adapt
from ...core.error import Error
from ...core.util import Printable, listof, maybe
from ...core.validator import Validator
from ...core.entity import (NamedEntity, SchemaEntity,
                            TableEntity, ColumnEntity, Join, DirectJoin,
                            ReverseJoin)
from ...core.model import (Node, HomeNode, TableNode, TableArc, ColumnArc,
                           ChainArc, SyntaxArc)
from ...core.introspect import introspect
from ...core.classify import normalize
from ...core.connect import transaction
from ...core.syn.parse import parse
from ...core.syn.syntax import Syntax
from ...core.cmd.command import Command
from ...core.cmd.summon import Summon, recognize
from ...core.cmd.act import act, Act, ProduceAction
from ...core.tr.bind import BindByName
from ...core.tr.binding import SubstitutionRecipe, ClosedRecipe
import fnmatch
import re
import weakref


def matches(entity, pattern):
    assert isinstance(entity, maybe(NamedEntity))
    assert isinstance(pattern, maybe(str))
    if entity is None:
        return (pattern is None)
    if pattern is None:
        return True
    name = normalize(entity.name) if entity.name else ""
    return fnmatch.fnmatchcase(name, pattern)


class Pattern(Printable):

    def __str__(self):
        raise NotImplementedError()


class TablePattern(Pattern):

    def __init__(self, schema_pattern, table_pattern):
        assert isinstance(schema_pattern, maybe(str))
        assert isinstance(table_pattern, str)
        self.schema_pattern = schema_pattern
        self.table_pattern = table_pattern

    def matches(self, entity):
        assert isinstance(entity, (SchemaEntity, TableEntity))
        if isinstance(entity, SchemaEntity):
            return matches(entity, self.schema_pattern)
        return (matches(entity.schema, self.schema_pattern) and
                matches(entity, self.table_pattern))

    def __str__(self):
        if self.schema_pattern is not None:
            return "%s.%s" % (self.schema_pattern, self.table_pattern)
        return self.table_pattern


class ColumnPattern(Pattern):

    def __init__(self, schema_pattern, table_pattern, column_pattern):
        assert isinstance(schema_pattern, maybe(str))
        assert isinstance(table_pattern, maybe(str))
        assert isinstance(column_pattern, str)
        self.schema_pattern = schema_pattern
        self.table_pattern = table_pattern
        self.column_pattern = column_pattern

    def matches(self, entity):
        assert isinstance(entity, (SchemaEntity, TableEntity, ColumnEntity))
        if isinstance(entity, SchemaEntity):
            return matches(entity, self.schema_pattern)
        if isinstance(entity, TableEntity):
            return (matches(entity.schema, self.schema_pattern) and
                    matches(entity, self.table_pattern))
        return (matches(entity.table.schema, self.schema_pattern) and
                matches(entity.table, self.table_pattern) and
                matches(entity, self.column_pattern))

    def __str__(self):
        chunks = []
        if self.schema_pattern is not None:
            chunks.append(self.schema_pattern)
        if self.table_pattern is not None:
            chunks.append(self.table_pattern)
        chunks.append(self.column_pattern)
        return ".".join(chunks)


class UniqueKeyPattern(Pattern):

    def __init__(self, schema_pattern, table_pattern, column_patterns,
                 is_primary, is_partial):
        assert isinstance(schema_pattern, maybe(str))
        assert isinstance(table_pattern, str)
        assert isinstance(column_patterns, listof(str))
        assert len(column_patterns) > 0
        assert isinstance(is_primary, bool)
        assert isinstance(is_partial, bool)
        assert not (is_primary and is_partial)
        self.schema_pattern = schema_pattern
        self.table_pattern = table_pattern
        self.column_patterns = column_patterns
        self.is_primary = is_primary
        self.is_partial = is_partial

    def matches(self, entity):
        assert isinstance(entity, (SchemaEntity, TableEntity))
        if isinstance(entity, SchemaEntity):
            return matches(entity, self.schema_pattern)
        return (matches(entity.schema, self.schema_pattern) and
                matches(entity, self.table_pattern))

    def extract(self, table):
        assert isinstance(table, TableEntity)
        assert self.matches(table)
        columns = []
        for pattern in self.column_patterns:
            matching = [column for column in table
                               if matches(column, pattern)]
            if len(matching) != 1:
                return
            [column] = matching
            if column in columns:
                return
            columns.append(column)
        return columns

    def __str__(self):
        chunks = []
        if self.schema_pattern is not None:
            chunks.append(self.schema_pattern)
            chunks.append(".")
        chunks.append(self.table_pattern)
        chunks.append("(%s)" % ",".join(self.column_patterns))
        if self.is_primary:
            chunks.append("!")
        if self.is_partial:
            chunks.append("?")
        return "".join(chunks)


class ForeignKeyPattern(Pattern):

    def __init__(self, schema_pattern, table_pattern, column_patterns,
                 target_schema_pattern, target_table_pattern,
                 target_column_patterns, is_partial):
        assert isinstance(schema_pattern, maybe(str))
        assert isinstance(table_pattern, str)
        assert isinstance(column_patterns, listof(str))
        assert len(column_patterns) > 0
        assert isinstance(target_schema_pattern, maybe(str))
        assert isinstance(target_table_pattern, str)
        assert isinstance(target_column_patterns, maybe(listof(str)))
        assert (target_column_patterns is None or
                len(target_column_patterns) == len(column_patterns))
        assert isinstance(is_partial, bool)
        self.schema_pattern = schema_pattern
        self.table_pattern = table_pattern
        self.column_patterns = column_patterns
        self.target_schema_pattern = target_schema_pattern
        self.target_table_pattern = target_table_pattern
        self.target_column_patterns = target_column_patterns
        self.is_partial = is_partial

    def matches(self, entity):
        assert isinstance(entity, (SchemaEntity, TableEntity))
        if isinstance(entity, SchemaEntity):
            return matches(entity, self.schema_pattern)
        return (matches(entity.schema, self.schema_pattern) and
                matches(entity, self.table_pattern))

    def matches_target(self, entity):
        assert isinstance(entity, (SchemaEntity, TableEntity))
        if isinstance(entity, SchemaEntity):
            return matches(entity, self.target_schema_pattern)
        return (matches(entity.schema, self.target_schema_pattern) and
                matches(entity, self.target_table_pattern))

    def extract(self, table):
        assert isinstance(table, TableEntity)
        assert self.matches(table)
        columns = []
        for pattern in self.column_patterns:
            matching = [column for column in table
                               if matches(column, pattern)]
            if len(matching) != 1:
                return
            [column] = matching
            if column in columns:
                return
            columns.append(column)
        return columns

    def extract_target(self, table):
        assert isinstance(table, TableEntity)
        assert self.matches_target(table)
        columns = None
        if self.target_column_patterns:
            columns = []
            for pattern in self.target_column_patterns:
                matching = [column for column in table
                                   if matches(column, pattern)]
                if len(matching) != 1:
                    return
                [column] = matching
                if column in columns:
                    return
                columns.append(column)
        else:
            if table.primary_key:
                columns = table.primary_key.origin_columns
                if len(columns) != len(self.column_patterns):
                    return
        return columns

    def __str__(self):
        chunks = []
        if self.schema_pattern is not None:
            chunks.append(self.schema_pattern)
            chunks.append(".")
        chunks.append(self.table_pattern)
        chunks.append("(%s)" % ",".join(self.column_patterns))
        if self.is_partial:
            chunks.append("?")
        chunks.append(" -> ")
        if self.target_schema_pattern is not None:
            chunks.append(self.target_schema_pattern)
            chunks.append(".")
        chunks.append(self.target_table_pattern)
        if self.target_column_patterns is not None:
            chunks.append("(%s)" % ",".join(self.target_column_patterns))
        return "".join(chunks)


class ArcPattern(Pattern):

    is_column = False
    is_table = False
    is_chain = False
    is_syntax = False

    def matches(self, arc):
        return False


class ColumnArcPattern(ArcPattern):

    is_column = True

    def __init__(self, column_pattern):
        assert isinstance(column_pattern, str)
        self.column_pattern = column_pattern

    def extract(self, node, parameters):
        assert isinstance(node, Node)
        if not isinstance(node, TableNode):
            return
        if parameters is not None:
            return
        matched_column = None
        for column in node.table:
            if not matches(column, self.column_pattern):
                continue
            if matched_column is not None:
                return
            matched_column = column
        if matched_column is None:
            return
        return ColumnArc(node.table, matched_column)

    def __str__(self):
        return self.column_pattern


class TableArcPattern(ArcPattern):

    is_table = True

    def __init__(self, schema_pattern, table_pattern):
        assert isinstance(schema_pattern, maybe(str))
        assert isinstance(table_pattern, str)
        self.schema_pattern = schema_pattern
        self.table_pattern = table_pattern

    def extract(self, node, parameters):
        assert isinstance(node, Node)
        if not isinstance(node, HomeNode):
            return
        if parameters is not None:
            return
        catalog = introspect()
        matched_table = None
        for schema in catalog:
            if not matches(schema, self.schema_pattern):
                continue
            for table in schema:
                if not matches(table, self.table_pattern):
                    continue
                if matched_table is not None:
                    return
                matched_table = table
        if matched_table is None:
            return
        return TableArc(matched_table)

    def __str__(self):
        if self.schema_pattern is not None:
            return "%s.%s" % (self.schema_pattern, self.schema_pattern)
        return self.table_pattern


class JoinPattern(Pattern):

    def __init__(self, schema_pattern, table_pattern, column_patterns,
                 target_schema_pattern, target_table_pattern,
                 target_column_patterns):
        assert isinstance(schema_pattern, maybe(str))
        assert isinstance(table_pattern, str)
        assert isinstance(column_patterns, maybe(listof(str)))
        assert column_patterns is None or len(column_patterns) > 0
        assert isinstance(target_schema_pattern, maybe(str))
        assert isinstance(target_table_pattern, str)
        assert isinstance(target_column_patterns, maybe(listof(str)))
        assert (target_column_patterns is None or
                len(target_column_patterns) > 0)
        assert (column_patterns is None or target_column_patterns is None or
                len(column_patterns) == len(target_column_patterns))
        self.schema_pattern = schema_pattern
        self.table_pattern = table_pattern
        self.column_patterns = column_patterns
        self.target_schema_pattern = target_schema_pattern
        self.target_table_pattern = target_table_pattern
        self.target_column_patterns = target_column_patterns

    def matches(self, join):
        assert isinstance(join, Join)
        return (matches(join.origin.schema, self.schema_pattern) and
                matches(join.origin, self.table_pattern) and
                (self.column_patterns is None or
                    (len(join.origin_columns) == len(self.column_patterns) and
                     all(matches(column, pattern)
                         for column, pattern in zip(join.origin_columns,
                                                    self.column_patterns)))) and
                matches(join.target.schema, self.target_schema_pattern) and
                matches(join.target, self.target_table_pattern) and
                (self.target_column_patterns is None or
                    (len(join.target_columns)
                        == len(self.target_column_patterns) and
                     all(matches(column, pattern)
                         for column, pattern
                                in zip(join.target_columns,
                                       self.target_column_patterns)))))

    def extract(self, node):
        assert isinstance(node, TableNode)
        matched_join = None
        for is_direct, foreign_keys in [(True, node.table.foreign_keys),
                                (False, node.table.referring_foreign_keys)]:
            for foreign_key in foreign_keys:
                if is_direct:
                    join = DirectJoin(foreign_key)
                else:
                    join = ReverseJoin(foreign_key)
                if not self.matches(join):
                    continue
                if matched_join is not None:
                    return
                matched_join = join
        if matched_join is None:
            return
        return matched_join

    def __str__(self):
        chunks = []
        if self.schema_pattern is not None:
            chunks.append(self.schema_pattern)
            chunks.append(".")
        chunks.append(self.table_pattern)
        if self.column_patterns is not None:
            chunks.append("(%s)" % ",".join(self.column_patterns))
        chunks.append(" -> ")
        if self.target_schema_pattern is not None:
            chunks.append(self.target_schema_pattern)
            chunks.append(".")
        chunks.append(self.target_table_pattern)
        if self.target_column_patterns is not None:
            chunks.append("(%s)" % ",".join(self.target_column_patterns))
        return "".join(chunks)


class ChainArcPattern(ArcPattern):

    is_chain = True

    def __init__(self, join_patterns):
        assert (isinstance(join_patterns, listof(JoinPattern))
                and len(join_patterns) > 0)
        self.join_patterns = join_patterns

    def extract(self, node, parameters):
        assert isinstance(node, Node)
        if not isinstance(node, TableNode):
            return
        if parameters is not None:
            return
        table = node.table
        joins = []
        for pattern in self.join_patterns:
            join = pattern.extract(node)
            if join is None:
                return
            joins.append(join)
            node = TableNode(join.target)
        return ChainArc(table, joins)

    def __str__(self):
        return ", ".join(str(pattern) for pattern in self.join_patterns)


class SyntaxArcPattern(ArcPattern):

    is_syntax = True

    def __init__(self, syntax):
        assert isinstance(syntax, str)
        self.syntax = syntax

    def extract(self, node, parameters):
        assert isinstance(node, Node)
        syntax = parse(self.syntax, 'flow_pipe')
        return SyntaxArc(node, parameters, syntax)

    def __str__(self):
        return str(self.syntax)


class BindGlobal(BindByName):

    __app__ = None
    parameters = None
    body = None

    @classmethod
    def __enabled__(component):
        if component.__app__ is None:
            return super(BindGlobal, component).__enabled__()
        return (component.__app__() is context.app)

    def __call__(self):
        body = parse(self.body, 'flow_pipe')
        recipe = SubstitutionRecipe(self.state.scope, [],
                                    self.parameters, body)
        recipe = ClosedRecipe(recipe)
        return self.state.use(recipe, self.syntax)


class GlobalPattern(ArcPattern):

    def __init__(self, syntax):
        assert isinstance(syntax, str)
        self.syntax = syntax

    def register(self, app, name, parameters):
        assert isinstance(name, str)
        class_name = "Bind%s" % name.title().replace('_', '')
        arity = None
        if parameters is not None:
            arity = len(parameters)
            parameters = list(parameters)
        namespace = {
            '__app__': weakref.ref(app),
            '__names__': [(name, arity)],
            'parameters': parameters,
            'body': self.syntax,
        }
        bind_class = type(class_name, (BindGlobal,), namespace)
        return bind_class

    def __str__(self):
        return str(self.syntax)


class CustomCmd(Command):

    def __init__(self, prelude, command):
        self.prelude = prelude
        self.command = command


class ProduceCustom(Act):

    adapt(CustomCmd, ProduceAction)

    def __call__(self):
        environment = self.action.environment.copy()
        with transaction():
            for parameter, command in self.command.prelude:
                action = self.action.clone(environment=environment)
                environment[parameter] = act(command, action)
            action = self.action.clone(environment=environment)
            return act(self.command.command, action)


class SummonCommand(Summon):

    __app__ = None
    parameters = None
    body = None

    @classmethod
    def __enabled__(component):
        if component.__app__ is None:
            return super(SummonCommand, component).__enabled__()
        return (component.__app__() is context.app)

    def __call__(self):
        if len(self.arguments) != len(self.parameters):
            expected = len(self.parameters)
            raise Error(
                    "Expected %s argument%s; got %s"
                    % (expected, "s" if expected != 1 else "",
                        len(self.arguments)))
        prelude = []
        for parameter, syntax in zip(self.parameters, self.arguments):
            prelude.append((parameter, recognize(syntax)))
        command = recognize(self.body)
        return CustomCmd(prelude, command)


class CommandPattern(Pattern):

    def __init__(self, syntax):
        assert isinstance(syntax, str)
        self.syntax = syntax

    def register(self, app, name, parameters):
        assert isinstance(name, str)
        class_name = "Summon%s" % name.title().replace('_', '')
        parameters = list(parameters)
        namespace = {
            '__app__': weakref.ref(app),
            '__names__': [name],
            'parameters': parameters,
            'body': self.syntax,
        }
        summon_class = type(class_name, (SummonCommand,), namespace)
        return summon_class

    def __str__(self):
        return str(self.syntax)


class TablePatternVal(Validator):

    pattern = r"""
        ^ \s*
        (?: (?P<schema> [\w*?]+ ) \s*\.\s* )?
        (?P<table> [\w*?]+ )
        \s* $
    """
    regexp = re.compile(pattern, re.X|re.U|re.S)

    def __call__(self, value):
        if value is None:
            return ValueError("the null value is not permitted")
        if isinstance(value, str):
            match = self.regexp.match(value)
            if match is None:
                raise ValueError("expected table pattern, got %r"
                                 % value)
            schema_pattern = match.group('schema')
            if schema_pattern is not None:
                schema_pattern = schema_pattern.lower()
            table_pattern = match.group('table').lower()
            value = TablePattern(schema_pattern, table_pattern)
        if not isinstance(value, TablePattern):
            raise ValueError("expected table pattern, got %r" % value)
        return value


class ColumnPatternVal(Validator):

    pattern = r"""
        ^ \s*
        (?: (?: (?P<schema> [\w*?]+ ) \s*\.\s* )?
            (?P<table> [\w*?]+ ) \s*\.\s* )?
        (?P<column> [\w*?]+ )
        \s* $
    """
    regexp = re.compile(pattern, re.X|re.U|re.S)

    def __call__(self, value):
        if value is None:
            return ValueError("the null value is not permitted")
        if isinstance(value, str):
            match = self.regexp.match(value)
            if match is None:
                raise ValueError("expected column pattern, got %r"
                                 % value)
            schema_pattern = match.group('schema')
            if schema_pattern is not None:
                schema_pattern = schema_pattern.lower()
            table_pattern = match.group('table')
            if table_pattern is not None:
                table_pattern = table_pattern.lower()
            column_pattern = match.group('column').lower()
            value = ColumnPattern(schema_pattern, table_pattern, column_pattern)
        if not isinstance(value, ColumnPattern):
            raise ValueError("expected column pattern, got %r" % value)
        return value


class UniqueKeyPatternVal(Validator):

    pattern = r"""
        ^ \s*
        (?: (?P<schema> [\w*?]+ ) \s*\.\s* )?
        (?P<table> [\w*?]+ )
        \s*
        \(
           \s* (?P<columns> [\w*?]+ (?: \s*,\s* [\w*?]+ )* ) \s*,?\s*
        \)
        \s*
        (?: (?P<primary> ! ) | (?P<partial> \? ) )?
        \s* $
    """
    regexp = re.compile(pattern, re.X|re.U|re.S)

    def __call__(self, value):
        if value is None:
            return ValueError("the null value is not permitted")
        if isinstance(value, str):
            match = self.regexp.match(value)
            if match is None:
                raise ValueError("expected unique key pattern, got %r"
                                 % value)
            schema_pattern = match.group('schema')
            if schema_pattern is not None:
                schema_pattern = schema_pattern.lower()
            table_pattern = match.group('table').lower()
            column_patterns = match.group('columns').lower()
            column_patterns = [pattern.strip()
                               for pattern in column_patterns.split(",")
                               if pattern.strip()]
            is_primary = bool(match.group('primary'))
            is_partial = bool(match.group('partial'))
            value = UniqueKeyPattern(schema_pattern, table_pattern,
                                     column_patterns, is_primary, is_partial)
        if not isinstance(value, UniqueKeyPattern):
            raise ValueError("expected unique key pattern, got %r" % value)
        return value


class ForeignKeyPatternVal(Validator):

    pattern = r"""
        ^ \s*
        (?: (?P<schema> [\w*?]+ ) \s*\.\s* )?
        (?P<table> [\w*?]+ )
        \s*
        \(
           \s* (?P<columns> [\w*?]+ (?: \s*,\s* [\w*?]+ )* ) \s*,?\s*
        \)
        \s* (?P<partial> \? )?
        \s* -> \s*
        (?: (?P<target_schema> [\w*?]+ ) \s*\.\s* )?
        (?P<target_table> [\w*?]+ )
        \s*
        (?: \(
           \s* (?P<target_columns> [\w*?]+ (?: \s*,\s* [\w*?]+ )* ) \s*,?\s*
        \) )?
        \s* $
    """
    regexp = re.compile(pattern, re.X|re.U|re.S)

    def __call__(self, value):
        if value is None:
            return ValueError("the null value is not permitted")
        if isinstance(value, str):
            match = self.regexp.match(value)
            if match is None:
                raise ValueError("expected foreign key pattern, got %r"
                                 % value)
            schema_pattern = match.group('schema')
            if schema_pattern is not None:
                schema_pattern = schema_pattern.lower()
            table_pattern = match.group('table').lower()
            column_patterns = match.group('columns').lower()
            column_patterns = [pattern.strip()
                               for pattern in column_patterns.split(",")
                               if pattern.strip()]
            target_schema_pattern = match.group('target_schema')
            if target_schema_pattern is not None:
                target_schema_pattern = target_schema_pattern.lower()
            target_table_pattern = match.group('target_table').lower()
            target_column_patterns = match.group('target_columns')
            if target_column_patterns is not None:
                target_column_patterns = \
                        [pattern.strip().lower()
                         for pattern in target_column_patterns.split(",")
                         if pattern.strip()]
                if len(target_column_patterns) != len(column_patterns):
                    raise ValueError("origin and target columns do not match"
                                     " in foreign key pattern %r"
                                     % value)
            is_partial = bool(match.group('partial'))
            value = ForeignKeyPattern(schema_pattern, table_pattern,
                        column_patterns, target_schema_pattern,
                        target_table_pattern, target_column_patterns,
                        is_partial)
        if not isinstance(value, ForeignKeyPattern):
            raise ValueError("expected foreign key pattern, got %r" % value)
        return value


class ClassPatternVal(Validator):

    pattern = r"""
        ^ \s* (?:
        (?P<is_table>
            (?: (?P<schema> [\w*?]+ ) \s*\.\s* )?
            (?P<table> [\w*?]+ ) )
        |
        (?P<is_syntax>
            (?P<syntax> \( .+ \) ) )
        ) \s* $
    """
    regexp = re.compile(pattern, re.X|re.U|re.S)

    def __call__(self, value):
        if value is None:
            return ValueError("the null value is not permitted")
        if isinstance(value, str):
            match = self.regexp.match(value)
            if match is None:
                raise ValueError("expected class pattern, got %r"
                                 % value)
            if match.group('is_table'):
                schema_pattern = match.group('schema')
                if schema_pattern is not None:
                    schema_pattern = schema_pattern.lower()
                table_pattern = match.group('table').lower()
                value = TableArcPattern(schema_pattern, table_pattern)
            elif match.group('is_syntax'):
                input = match.group('syntax')
                value = SyntaxArcPattern(input)
            else:
                assert False
        if not isinstance(value, ArcPattern):
            raise ValueError("expected class pattern, got %r" % value)
        return value


class FieldPatternVal(Validator):

    pattern = r"""
        ^ \s* (?:
        (?P<is_column>
            (?P<column> [\w*?]+ ) )
        |
        (?P<is_chain>
            (?: [\w*?]+ \s*\.\s* )? [\w*?]+
            \s* (?: \( \s* [\w*?]+ (?: \s*,\s* [\w*?]+ )* \s*,?\s* \) )?
            \s* -> \s*
            (?: [\w*?]+ \s*\.\s* )? [\w*?]+
            \s* (?: \( \s* [\w*?]+ (?: \s*,\s* [\w*?]+ )* \s*,?\s* \) )?
            (?: \s*,\s*
                (?: [\w*?]+ \s*\.\s* )? [\w*?]+
                \s* (?: \( \s* [\w*?]+ (?: \s*,\s* [\w*?]+ )* \s*,?\s* \) )?
                \s* -> \s*
                (?: [\w*?]+ \s*\.\s* )? [\w*?]+
                \s* (?: \( \s* [\w*?]+ (?: \s*,\s* [\w*?]+ )* \s*,?\s* \) )?
                \s*,?\s* )* )
        |
        (?P<is_syntax>
            (?P<syntax> \( .+ \) ) )
        ) \s* $
    """
    regexp = re.compile(pattern, re.X|re.U|re.S)
    join_pattern = r"""
        (?: (?P<schema> [\w*?]+ ) \s*\.\s* )?
        (?P<table> [\w*?]+ )
        \s*
        (?: \(
           \s* (?P<columns> [\w*?]+ (?: \s*,\s* [\w*?]+ )* ) \s*,?\s*
        \) )?
        \s* -> \s*
        (?: (?P<target_schema> [\w*?]+ ) \s*\.\s* )?
        (?P<target_table> [\w*?]+ )
        \s*
        (?: \(
           \s* (?P<target_columns> [\w*?]+ (?: \s*,\s* [\w*?]+ )* ) \s*,?\s*
        \) )?
        \s*,?\s*
    """
    join_regexp = re.compile(join_pattern, re.X|re.U|re.S)

    def __call__(self, value):
        if value is None:
            return ValueError("the null value is not permitted")
        if isinstance(value, str):
            match = self.regexp.match(value)
            if match is None:
                raise ValueError("expected field pattern, got %r"
                                 % value)
            if match.group('is_column'):
                column_pattern = match.group('column').lower()
                value = ColumnArcPattern(column_pattern)
            elif match.group('is_chain'):
                join_patterns = []
                start = 0
                while start < len(value):
                    match = self.join_regexp.match(value, start)
                    assert match is not None
                    schema_pattern = match.group('schema')
                    if schema_pattern is not None:
                        schema_pattern = schema_pattern.lower()
                    table_pattern = match.group('table').lower()
                    column_patterns = match.group('columns')
                    if column_patterns is not None:
                        column_patterns = \
                                [pattern.strip().lower()
                                 for pattern in column_patterns.split(",")
                                 if pattern.strip()]
                    target_schema_pattern = match.group('target_schema')
                    if target_schema_pattern is not None:
                        target_schema_pattern = target_schema_pattern.lower()
                    target_table_pattern = match.group('target_table').lower()
                    target_column_patterns = match.group('target_columns')
                    if target_column_patterns is not None:
                        target_column_patterns = \
                                [pattern.strip().lower()
                                 for pattern in target_column_patterns.split(",")
                                 if pattern.strip()]
                    if (column_patterns is not None and
                            target_column_patterns is not None and
                            len(column_patterns) != len(target_column_patterns)):
                        raise ValueError("origin and target columns do not match"
                                         " in join pattern %r"
                                         % value)
                    join_pattern = JoinPattern(schema_pattern, table_pattern,
                                               column_patterns,
                                               target_schema_pattern,
                                               target_table_pattern,
                                               target_column_patterns)
                    join_patterns.append(join_pattern)
                    start = match.end()
                value = ChainArcPattern(join_patterns)
            elif match.group('is_syntax'):
                input = match.group('syntax')
                value = SyntaxArcPattern(input)
            else:
                assert False
        if not isinstance(value, ArcPattern):
            raise ValueError("expected field pattern, got %r" % value)
        return value


class GlobalPatternVal(Validator):

    def __call__(self, value):
        if value is None:
            return ValueError("the null value is not permitted")
        if isinstance(value, str):
            value = GlobalPattern(value)
        if not isinstance(value, GlobalPattern):
            raise ValueError("expected global pattern, got %r" % value)
        return value


class CommandPatternVal(Validator):

    def __call__(self, value):
        if value is None:
            return ValueError("the null value is not permitted")
        if isinstance(value, str):
            value = CommandPattern(value)
        if not isinstance(value, CommandPattern):
            raise ValueError("expected command pattern, got %r" % value)
        return value


class LabelVal(Validator):

    pattern = r"""
        ^
        (?P<label> \w+ )
        \s*
        (?: \( \s*
            (?P<parameters> (?: \$?\w+ (?: \s*,\s* \$?\w+ )* \s*,? )? )
        \s* \) )?
        $
    """
    regexp = re.compile(pattern, re.X|re.U|re.S)

    def __call__(self, value):
        if value is None:
            return ValueError("the null value is not permitted")
        if isinstance(value, str):
            match = self.regexp.match(value)
            if match is None:
                raise ValueError("expected label, got %r"
                                 % value)
            label = normalize(match.group('label'))
            parameters = None
            if match.group('parameters') is not None:
                parameters = []
                for parameter in match.group('parameters').split(','):
                    name = parameter.strip()
                    if not name:
                        continue
                    is_reference = False
                    if name.startswith("$"):
                        is_reference = True
                        name = name[1:]
                    name = normalize(name)
                    parameters.append((name, is_reference))
                parameters = tuple(parameters)
            value = (label, parameters)
        else:
            raise ValueError("expected label, got %r" % value)
        return value


class QLabelVal(Validator):

    pattern = r"""
        ^
        (?P<qualifier> \w+ )
        \s*\.\s* (?P<label> \w+ )
        \s*
        (?: \( \s*
            (?P<parameters> (?: \$?\w+ (?: \s*,\s* \$?\w+ )* \s*,? )? )
        \s* \) )?
        $
    """
    regexp = re.compile(pattern, re.X|re.U|re.S)

    def __call__(self, value):
        if value is None:
            return ValueError("the null value is not permitted")
        if isinstance(value, str):
            match = self.regexp.match(value)
            if match is None:
                raise ValueError("expected label, got %r"
                                 % value)
            qualifier = normalize(match.group('qualifier'))
            label = normalize(match.group('label'))
            parameters = None
            if match.group('parameters') is not None:
                parameters = []
                for parameter in match.group('parameters').split(','):
                    name = parameter.strip()
                    if not name:
                        continue
                    is_reference = False
                    if name.startswith("$"):
                        is_reference = True
                        name = name[1:]
                    name = normalize(name)
                    parameters.append((name, is_reference))
                parameters = tuple(parameters)
            value = (qualifier, label, parameters)
        else:
            raise ValueError("expected label, got %r" % value)
        return value


class CommandVal(Validator):

    pattern = r"""
        ^
        (?P<label> \w+ )
        \s*
        \( \s*
            (?P<parameters> (?: \$\w+ (?: \s*,\s* \$\w+ )* \s*,? )? )
        \s* \)
        $
    """
    regexp = re.compile(pattern, re.X|re.U|re.S)

    def __call__(self, value):
        if value is None:
            return ValueError("the null value is not permitted")
        if isinstance(value, str):
            match = self.regexp.match(value)
            if match is None:
                raise ValueError("expected command label, got %r"
                                 % value)
            label = normalize(match.group('label'))
            parameters = []
            for parameter in match.group('parameters').split(','):
                name = parameter.strip()
                if not name:
                    continue
                name = name[1:]
                name = normalize(name)
                parameters.append(name)
            parameters = tuple(parameters)
            value = (label, parameters)
        else:
            raise ValueError("expected command label, got %r" % value)
        return value


