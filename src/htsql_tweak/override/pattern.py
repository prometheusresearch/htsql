#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.util import Printable
from htsql.validator import Validator
from htsql.entity import SchemaEntity, TableEntity, ColumnEntity
from htsql.classify import normalize
import fnmatch
import re


class SchemaPattern(Printable):

    def __init__(self, schema_pattern):
        assert isinstance(schema_pattern, str)
        self.schema_pattern = schema_pattern

    def matches(self, schema):
        assert isinstance(schema, SchemaEntity)
        return fnmatch.fnmatchcase(normalize(schema.name),
                                   self.schema_pattern)

    def __str__(self):
        return self.schema_pattern


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


class TablePattern(object):

    def __init__(self, schema_pattern, table_pattern):
        assert isinstance(schema_pattern, str)
        assert isinstance(table_pattern, str)
        self.schema_pattern = schema_pattern
        self.table_pattern = table_pattern

    def matches(self, table):
        assert isinstance(table, TableEntity)
        return (fnmatch.fnmatchcase(normalize(table.schema.name),
                                    self.schema_pattern) and
                fnmatch.fnmatchcase(normalize(table.name),
                                    self.table_pattern))

    def __str__(self):
        return "%s.%s" % (self.schema_pattern, self.table_pattern)


class TablePatternVal(Validator):

    pattern = r"""
        ^ \s*
        (?: (?P<schema> [\w*?]+ ) \. )?
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


class ColumnPattern(object):

    def __init__(self, schema_pattern, table_pattern, column_pattern):
        assert isinstance(schema_pattern, str)
        assert isinstance(table_pattern, str)
        assert isinstance(column_pattern, str)
        self.schema_pattern = schema_pattern
        self.table_pattern = table_pattern
        self.column_pattern = column_pattern

    def matches(self, column):
        assert isinstance(column, ColumnEntity)
        return (fnmatch.fnmatchcase(normalize(column.table.schema.name),
                                    self.schema_pattern) and
                fnmatch.fnmatchcase(normalize(column.table.name),
                                    self.table_pattern) and
                fnmatch.fnmatchcase(normalize(column.name),
                                    self.column_pattern))

    def __str__(self):
        return "%s.%s.%s" % (self.schema_pattern,
                             self.table_pattern,
                             self.column_pattern)


class ColumnPatternVal(Validator):

    pattern = r"""
        ^ \s*
        (?: (?: (?P<schema> [\w*?]+ ) \. )?
            (?P<table> [\w*?]+ ) \. )?
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


