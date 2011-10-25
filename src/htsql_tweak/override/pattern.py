#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.util import Printable, listof, maybe
from htsql.validator import Validator
from htsql.entity import SchemaEntity, TableEntity, ColumnEntity
from htsql.classify import normalize
import fnmatch
import re


class SchemaPattern(Printable):

    def __init__(self, schema_pattern):
        assert isinstance(schema_pattern, str)
        self.schema_pattern = schema_pattern

    def matches(self, entity):
        assert isinstance(entity, SchemaEntity)
        return fnmatch.fnmatchcase(normalize(entity.name),
                                   self.schema_pattern)

    def __str__(self):
        return self.schema_pattern


class TablePattern(Printable):

    def __init__(self, schema_pattern, table_pattern):
        assert isinstance(schema_pattern, str)
        assert isinstance(table_pattern, str)
        self.schema_pattern = schema_pattern
        self.table_pattern = table_pattern

    def matches(self, entity):
        assert isinstance(entity, (SchemaEntity, TableEntity))
        if isinstance(entity, SchemaEntity):
            return fnmatch.fnmatchcase(normalize(entity.name),
                                       self.schema_pattern)
        return (fnmatch.fnmatchcase(normalize(entity.schema.name),
                                    self.schema_pattern) and
                fnmatch.fnmatchcase(normalize(entity.name),
                                    self.table_pattern))

    def __str__(self):
        return "%s.%s" % (self.schema_pattern, self.table_pattern)


class ColumnPattern(Printable):

    def __init__(self, schema_pattern, table_pattern, column_pattern):
        assert isinstance(schema_pattern, str)
        assert isinstance(table_pattern, str)
        assert isinstance(column_pattern, str)
        self.schema_pattern = schema_pattern
        self.table_pattern = table_pattern
        self.column_pattern = column_pattern

    def matches(self, entity):
        assert isinstance(entity, (SchemaEntity, TableEntity, ColumnEntity))
        if isinstance(entity, SchemaEntity):
            return fnmatch.fnmatchcase(normalize(entity.name),
                                       self.schema_pattern)
        if isinstance(entity, TableEntity):
            return (fnmatch.fnmatchcase(normalize(entity.schema.name),
                                        self.schema_pattern) and
                    fnmatch.fnmatchcase(normalize(entity.name),
                                        self.table_pattern))
        return (fnmatch.fnmatchcase(normalize(entity.table.schema.name),
                                    self.schema_pattern) and
                fnmatch.fnmatchcase(normalize(entity.table.name),
                                    self.table_pattern) and
                fnmatch.fnmatchcase(normalize(entity.name),
                                    self.column_pattern))

    def __str__(self):
        return "%s.%s.%s" % (self.schema_pattern,
                             self.table_pattern,
                             self.column_pattern)


class UniqueKeyPattern(Printable):

    def __init__(self, schema_pattern, table_pattern, column_patterns,
                 is_primary, is_partial):
        assert isinstance(schema_pattern, str)
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
            return fnmatch.fnmatchcase(normalize(entity.name),
                                       self.schema_pattern)
        return (fnmatch.fnmatchcase(normalize(entity.schema.name),
                                    self.schema_pattern) and
                fnmatch.fnmatchcase(normalize(entity.name),
                                    self.table_pattern))

    def extract(self, table):
        assert isinstance(table, TableEntity)
        assert self.matches(table)
        columns = []
        for pattern in self.column_patterns:
            matching = [column
                        for column in table.columns
                        if fnmatch.fnmatchcase(normalize(column.name),
                                               pattern)]
            if len(matching) != 1:
                return
            [column] = matching
            if column in columns:
                return
            columns.append(column)
        return columns

    def __str__(self):
        return ("%s.%s%s%s%s"
                % (self.schema_pattern, self.table_pattern,
                   "(%s)" % ",".join(self.column_patterns),
                   "!" if self.is_primary else "",
                   "?" if self.is_partial else ""))


class ForeignKeyPattern(Printable):

    def __init__(self, schema_pattern, table_pattern, column_patterns,
                 target_schema_pattern, target_table_pattern,
                 target_column_patterns, is_partial):
        assert isinstance(schema_pattern, str)
        assert isinstance(table_pattern, str)
        assert isinstance(column_patterns, listof(str))
        assert len(column_patterns) > 0
        assert isinstance(target_schema_pattern, str)
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
            return fnmatch.fnmatchcase(normalize(entity.name),
                                       self.schema_pattern)
        return (fnmatch.fnmatchcase(normalize(entity.schema.name),
                                    self.schema_pattern) and
                fnmatch.fnmatchcase(normalize(entity.name),
                                    self.table_pattern))

    def matches_target(self, entity):
        assert isinstance(entity, (SchemaEntity, TableEntity))
        if isinstance(entity, SchemaEntity):
            return fnmatch.fnmatchcase(normalize(entity.name),
                                       self.target_schema_pattern)
        return (fnmatch.fnmatchcase(normalize(entity.schema.name),
                                    self.target_schema_pattern) and
                fnmatch.fnmatchcase(normalize(entity.name),
                                    self.target_table_pattern))

    def extract(self, table):
        assert isinstance(table, TableEntity)
        assert self.matches(table)
        columns = []
        for pattern in self.column_patterns:
            matching = [column
                        for column in table.columns
                        if fnmatch.fnmatchcase(normalize(column.name),
                                               pattern)]
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
                matching = [column
                            for column in table.columns
                            if fnmatch.fnmatchcase(normalize(column.name),
                                                   pattern)]
                if len(matching) != 1:
                    return
                [column] = matching
                if column in columns:
                    return
                columns.append(column)
        else:
            if self.table.primary_key:
                columns = self.table.primary_key.origin_columns
                if len(columns) != len(self.column_patterns):
                    return
        return columns

    def __str__(self):
        return ("%s.%s%s%s -> %s.%s%s"
                % (self.schema_pattern, self.table_pattern,
                   "(%s)" % ",".join(self.column_patterns),
                   "?" if self.is_partial else "",
                   self.target_schema_pattern,
                   self.target_table_pattern,
                   "(%s)" % ",".join(self.target_column_patterns)
                   if self.target_column_patterns else ""))


class SchemaPatternVal(Validator):

    pattern = r"""
        ^ \s*
        (?P<schema> [\w*?]+ )
        \s* $
    """
    regexp = re.compile(pattern, re.X)

    def __call__(self, value):
        if value is None:
            return ValueError("the null value is not permitted")
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        if isinstance(value, str):
            match = self.regexp.match(value)
            if match is None:
                raise ValueError("expected schema pattern, got %r" % value)
            schema_pattern = match.group('schema').lower()
            value = SchemaPattern(schema_pattern)
        if not isinstance(value, SchemaPattern):
            raise ValueError("expected schema pattern, got %r" % value)
        return value


class TablePatternVal(Validator):

    pattern = r"""
        ^ \s*
        (?: (?P<schema> [\w*?]+ ) \s*\.\s* )?
        (?P<table> [\w*?]+ )
        \s* $
    """
    regexp = re.compile(pattern, re.X)

    def __call__(self, value):
        if value is None:
            return ValueError("the null value is not permitted")
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        if isinstance(value, str):
            match = self.regexp.match(value)
            if match is None:
                raise ValueError("expected table pattern, got %r" % value)
            schema_pattern = match.group('schema')
            schema_pattern = schema_pattern.lower() if schema_pattern else "*"
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
    regexp = re.compile(pattern, re.X)

    def __call__(self, value):
        if value is None:
            return ValueError("the null value is not permitted")
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        if isinstance(value, str):
            match = self.regexp.match(value)
            if match is None:
                raise ValueError("expected column pattern, got %r" % value)
            schema_pattern = match.group('schema')
            schema_pattern = schema_pattern.lower() if schema_pattern else "*"
            table_pattern = match.group('table')
            table_pattern = table_pattern.lower() if table_pattern else "*"
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
    regexp = re.compile(pattern, re.X)

    def __call__(self, value):
        if value is None:
            return ValueError("the null value is not permitted")
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        if isinstance(value, str):
            match = self.regexp.match(value)
            if match is None:
                raise ValueError("expected unique key pattern, got %r" % value)
            schema_pattern = match.group('schema')
            schema_pattern = schema_pattern.lower() if schema_pattern else "*"
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
    regexp = re.compile(pattern, re.X)

    def __call__(self, value):
        if value is None:
            return ValueError("the null value is not permitted")
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        if isinstance(value, str):
            match = self.regexp.match(value)
            if match is None:
                raise ValueError("expected foreign key pattern, got %r"
                                 % value)
            schema_pattern = match.group('schema')
            schema_pattern = schema_pattern.lower() if schema_pattern else "*"
            table_pattern = match.group('table').lower()
            column_patterns = match.group('columns').lower()
            column_patterns = [pattern.strip()
                               for pattern in column_patterns.split(",")
                               if pattern.strip()]
            target_schema_pattern = match.group('target_schema')
            target_schema_pattern = (target_schema_pattern.lower()
                                     if target_schema_pattern else "*")
            target_table_pattern = match.group('target_table').lower()
            target_column_patterns = match.group('target_columns')
            if target_column_patterns is not None:
                target_column_patterns = \
                        [pattern.strip().lower()
                         for pattern in target_column_patterns.split(",")
                         if pattern.strip()]
                if len(target_column_patterns) != len(column_patterns):
                    raise ValueError("origin and target columns do not match"
                                     " in foreign key pattern %r" % value)
            is_partial = bool(match.group('partial'))
            value = ForeignKeyPattern(schema_pattern, table_pattern,
                        column_patterns, target_schema_pattern,
                        target_table_pattern, target_column_patterns,
                        is_partial)
        if not isinstance(value, ForeignKeyPattern):
            raise ValueError("expected foreign key pattern, got %r" % value)
        return value


