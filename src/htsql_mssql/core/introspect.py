#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from htsql.core.adapter import Protocol, call
from htsql.core.introspect import Introspect
from htsql.core.entity import make_catalog
from htsql.core.domain import (BooleanDomain, IntegerDomain, DecimalDomain,
                               FloatDomain, StringDomain, DateTimeDomain,
                               OpaqueDomain)
from htsql.core.connect import connect
import itertools
import fnmatch


class IntrospectMSSQL(Introspect):

    system_schema_names = [u'guest', u'INFORMATION_SCHEMA', u'sys', u'db_*']

    def __call__(self):
        connection = connect()
        cursor = connection.cursor()

        catalog = make_catalog()

        schema_by_id = {}
        cursor.execute("""
            SELECT schema_id, name
            FROM sys.schemas
            ORDER BY name
        """)
        for row in cursor.fetchnamed():
            if any(fnmatch.fnmatchcase(row.name, pattern)
                   for pattern in self.system_schema_names):
                continue
            schema = catalog.add_schema(row.name)
            schema_by_id[row.schema_id] = schema

        cursor.execute("""
            SELECT default_schema_name
            FROM sys.database_principals
            WHERE principal_id = USER_ID()
        """)
        default_schema_name = cursor.fetchone()[0]
        if default_schema_name in catalog.schemas:
            catalog.schemas[default_schema_name].set_priority(1)

        table_by_id = {}
        cursor.execute("""
            SELECT object_id, schema_id, name
            FROM sys.objects
            WHERE type in ('U', 'V')
            ORDER BY schema_id, name
        """)
        for row in cursor.fetchnamed():
            if row.schema_id not in schema_by_id:
                continue
            schema = schema_by_id[row.schema_id]
            table = schema.add_table(row.name)
            table_by_id[row.object_id] = table

        column_by_id = {}
        cursor.execute("""
            SELECT c.object_id, c.column_id, c.name, c.max_length,
                   c.precision, c.scale, c.is_nullable, c.default_object_id,
                   t.name AS type_name, s.name AS type_schema_name
            FROM sys.columns c
            JOIN sys.types t ON (c.user_type_id = t.user_type_id)
            JOIN sys.schemas s ON (t.schema_id = s.schema_id)
            ORDER BY c.object_id, c.column_id
        """)
        for row in cursor.fetchnamed():
            if row.object_id not in table_by_id:
                continue
            table = table_by_id[row.object_id]
            name = row.name
            type_schema_name = row.type_schema_name
            type_name = row.type_name
            length = row.max_length if row.max_length != -1 else None
            precision = row.precision
            scale = row.scale
            domain = IntrospectMSSQLDomain.__invoke__(type_schema_name,
                                                      type_name,
                                                      length, precision, scale)
            is_nullable = bool(row.is_nullable)
            has_default = bool(row.default_object_id)
            column = table.add_column(name, domain, is_nullable, has_default)
            column_by_id[row.object_id, row.column_id] = column

        cursor.execute("""
            SELECT object_id, index_id, is_primary_key, is_unique_constraint
            FROM sys.indexes
            WHERE (is_primary_key = 1 OR is_unique_constraint = 1) AND
                  is_disabled = 0
            ORDER BY object_id, index_id
        """)
        index_rows = cursor.fetchnamed()
        cursor.execute("""
            SELECT object_id, index_id, index_column_id, column_id
            FROM sys.index_columns
            ORDER BY object_id, index_id, index_column_id
        """)
        column_rows_by_id = \
                dict((key, list(group))
                     for key, group in itertools.groupby(cursor.fetchnamed(),
                                         lambda r: (r.object_id, r.index_id)))
        for row in index_rows:
            if row.object_id not in table_by_id:
                continue
            table = table_by_id[row.object_id]
            key = (row.object_id, row.index_id)
            if key not in column_rows_by_id:
                continue
            column_rows = column_rows_by_id[key]
            if not all((column_row.object_id, column_row.column_id)
                            in column_by_id
                       for column_row in column_rows):
                continue
            columns = [column_by_id[column_row.object_id, column_row.column_id]
                       for column_row in column_rows]
            is_primary = bool(row.is_primary_key)
            table.add_unique_key(columns, is_primary)

        cursor.execute("""
            SELECT object_id, parent_object_id, referenced_object_id
            FROM sys.foreign_keys
            WHERE is_disabled = 0
            ORDER BY object_id
        """)
        key_rows = cursor.fetchnamed()
        cursor.execute("""
            SELECT constraint_object_id, constraint_column_id,
                   parent_object_id, parent_column_id,
                   referenced_object_id, referenced_column_id
            FROM sys.foreign_key_columns
            ORDER BY constraint_object_id, constraint_column_id
        """)
        key_column_rows_by_id = \
                dict((key, list(group))
                     for key, group in itertools.groupby(cursor.fetchnamed(),
                                            lambda r: r.constraint_object_id))
        for row in key_rows:
            if row.parent_object_id not in table_by_id:
                continue
            table = table_by_id[row.parent_object_id]
            if row.referenced_object_id not in table_by_id:
                continue
            target_table = table_by_id[row.referenced_object_id]
            if row.object_id not in key_column_rows_by_id:
                continue
            column_rows = key_column_rows_by_id[row.object_id]
            column_ids = [(column_row.parent_object_id,
                           column_row.parent_column_id)
                          for column_row in column_rows]
            target_column_ids = [(column_row.referenced_object_id,
                                  column_row.referenced_column_id)
                                 for column_row in column_rows]
            if not all(column_id in column_by_id
                       for column_id in column_ids):
                continue
            columns = [column_by_id[column_id]
                       for column_id in column_ids]
            if not all(column_id in column_by_id
                       for column_id in target_column_ids):
                continue
            target_columns = [column_by_id[column_id]
                              for column_id in target_column_ids]
            table.add_foreign_key(columns, target_table, target_columns)

        connection.release()
        return catalog


class IntrospectMSSQLDomain(Protocol):

    @classmethod
    def __dispatch__(component, schema_name, type_name, *args, **kwds):
        return (schema_name.encode('utf-8'), type_name.encode('utf-8'))

    @classmethod
    def __matches__(component, dispatch_key):
        return (dispatch_key in component.__names__)

    def __init__(self, schema_name, type_name,
                 length, precision, scale):
        self.schema_name = schema_name
        self.type_name = type_name
        self.length = length
        self.precision = precision
        self.scale = scale

    def __call__(self):
        return OpaqueDomain()


class IntrospectMSSQLCharDomain(IntrospectMSSQLDomain):

    call(('sys', 'char'), ('sys', 'nchar'))

    def __call__(self):
        return StringDomain(length=self.length, is_varying=False)


class IntrospectMSSQLVarCharDomain(IntrospectMSSQLDomain):

    call(('sys', 'varchar'), ('sys', 'nvarchar'))

    def __call__(self):
        return StringDomain(length=self.length, is_varying=False)


class IntrospectMSSQLBitDomain(IntrospectMSSQLDomain):

    call(('sys', 'bit'))

    def __call__(self):
        return BooleanDomain()


class IntrospectMSSQLIntegerDomain(IntrospectMSSQLDomain):

    call(('sys', 'tinyint'), ('sys', 'smallint'),
          ('sys', 'int'), ('sys', 'bigint'))

    def __call__(self):
        return IntegerDomain(size=self.length*8)


class IntrospectMSSQLDecimalDomain(IntrospectMSSQLDomain):

    call(('sys', 'decimal'), ('sys', 'numeric'))

    def __call__(self):
        return DecimalDomain(precision=self.precision, scale=self.scale)


class IntrospectMSSQLFloatDomain(IntrospectMSSQLDomain):

    call(('sys', 'real'), ('sys', 'float'))

    def __call__(self):
        return FloatDomain(size=self.length*8)


class IntrospectMSSQLDateTimeDomain(IntrospectMSSQLDomain):

    call(('sys', 'datetime'), ('sys', 'smalldatetime'))

    def __call__(self):
        return DateTimeDomain()


