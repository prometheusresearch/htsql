#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from htsql.core.adapter import Protocol, call
from htsql.core.introspect import Introspect
from htsql.core.entity import make_catalog
from htsql.core.domain import (BooleanDomain, IntegerDomain,
                               DecimalDomain, FloatDomain, StringDomain,
                               EnumDomain, DateDomain, TimeDomain,
                               DateTimeDomain, OpaqueDomain)
from htsql.core.connect import connect
import itertools


class IntrospectMySQL(Introspect):

    system_schema_names = [u'mysql', u'information_schema']

    def __call__(self):
        connection = connect()
        cursor = connection.cursor()

        catalog = make_catalog()

        cursor.execute("""
            SELECT s.schema_name
            FROM information_schema.schemata s
            ORDER BY 1
        """)
        for row in cursor.fetchnamed():
            catalog.add_schema(row.schema_name)
        cursor.execute("""
            SELECT DATABASE()
        """)
        database_name = cursor.fetchone()[0]
        if database_name in catalog.schemas:
            catalog.schemas[database_name].set_priority(1)

        cursor.execute("""
            SELECT t.table_schema, t.table_name
            FROM information_schema.tables t
            WHERE t.table_type IN ('BASE TABLE', 'VIEW')
            ORDER BY 1, 2
        """)
        for row in cursor.fetchnamed():
            if row.table_schema not in catalog.schemas:
                continue
            schema = catalog.schemas[row.table_schema]
            schema.add_table(row.table_name)

        cursor.execute("""
            SELECT c.table_schema, c.table_name, c.ordinal_position,
                   c.column_name, c.is_nullable, c.column_default,
                   c.data_type, c.column_type, c.character_maximum_length,
                   c.numeric_precision, c.numeric_scale
            FROM information_schema.columns c
            ORDER BY 1, 2, 3
        """)
        for row in cursor.fetchnamed():
            if row.table_schema not in catalog.schemas:
                continue
            schema = catalog.schemas[row.table_schema]
            if row.table_name not in schema.tables:
                continue
            table = schema.tables[row.table_name]
            name = row.column_name
            is_nullable = (row.is_nullable == 'YES')
            has_default = (row.column_default is not None)
            data_type = row.data_type
            column_type = row.column_type
            length = row.character_maximum_length
            if isinstance(length, long):
                length = int(length)
                if isinstance(length, long): # LONGTEXT
                    length = None
            precision = row.numeric_precision
            if isinstance(precision, long):
                precision = int(precision)
            scale = row.numeric_scale
            if isinstance(scale, long):
                scale = int(scale)
            domain = IntrospectMySQLDomain.__invoke__(data_type, column_type,
                                                      length, precision, scale)
            table.add_column(name, domain, is_nullable, has_default)

        cursor.execute("""
            SELECT c.table_schema, c.table_name,
                   c.constraint_schema, c.constraint_name,
                   c.constraint_type
            FROM information_schema.table_constraints c
            WHERE c.constraint_type IN ('PRIMARY KEY', 'UNIQUE', 'FOREIGN KEY')
            ORDER BY 1, 2, 3, 4
        """)
        constraint_rows = cursor.fetchnamed()
        cursor.execute("""
            SELECT u.table_schema, u.table_name,
                   u.constraint_schema, u.constraint_name,
                   u.ordinal_position,
                   u.column_name,
                   u.referenced_table_schema,
                   u.referenced_table_name,
                   u.referenced_column_name
            FROM information_schema.key_column_usage u
            ORDER BY 1, 2, 3, 4, 5
        """)
        usage_rows_by_constraint_key = \
                dict((key, list(group))
                     for key, group in itertools.groupby(cursor.fetchnamed(),
                         lambda r: (r.table_schema, r.table_name,
                                    r.constraint_schema, r.constraint_name)))
        for constraint_row in constraint_rows:
            key = (constraint_row.table_schema,
                   constraint_row.table_name,
                   constraint_row.constraint_schema,
                   constraint_row.constraint_name)
            if key not in usage_rows_by_constraint_key:
                continue
            usage_rows = usage_rows_by_constraint_key[key]
            if constraint_row.table_schema not in catalog.schemas:
                continue
            schema = catalog.schemas[constraint_row.table_schema]
            if constraint_row.table_name not in schema.tables:
                continue
            table = schema.tables[constraint_row.table_name]
            if not all(row.column_name in table.columns
                       for row in usage_rows):
                continue
            columns = [table.columns[row.column_name] for row in usage_rows]
            if constraint_row.constraint_type in ('PRIMARY KEY', 'UNIQUE'):
                is_primary = (constraint_row.constraint_type == 'PRIMARY KEY')
                table.add_unique_key(columns, is_primary)
            elif constraint_row.constraint_type == 'FOREIGN KEY':
                row = usage_rows[0]
                if row.referenced_table_schema not in catalog.schemas:
                    continue
                target_schema = catalog.schemas[row.referenced_table_schema]
                if row.referenced_table_name not in target_schema.tables:
                    continue
                target_table = target_schema.tables[row.referenced_table_name]
                if not all(row.referenced_column_name in target_table.columns
                           for row in usage_rows):
                    continue
                target_columns = \
                        [target_table.columns[row.referenced_column_name]
                         for row in usage_rows]
                table.add_foreign_key(columns, target_table, target_columns)

        connection.release()
        return catalog


class IntrospectMySQLDomain(Protocol):

    @classmethod
    def __dispatch__(component, data_type, *args, **kwds):
        return data_type.encode('utf-8')

    def __init__(self, data_type, column_type, length, precision, scale):
        self.data_type = data_type
        self.column_type = column_type
        self.length = length
        self.precision = precision
        self.scale = scale

    def __call__(self):
        return OpaqueDomain()


class IntrospectMySQLCharDomain(IntrospectMySQLDomain):

    call('char')

    def __call__(self):
        return StringDomain(length=self.length, is_varying=False)


class IntrospectMySQLVarCharDomain(IntrospectMySQLDomain):

    call('varchar', 'tinytext', 'text', 'mediumtext', 'longtext')

    def __call__(self):
        return StringDomain(length=self.length, is_varying=True)


class IntrospectMySQLEnumDomain(IntrospectMySQLDomain):

    call('enum')

    def __call__(self):
        column_type = self.column_type
        if column_type.startswith('enum(') and column_type.endswith(')'):
            labels = [item[1:-1]
                      for item in column_type[5:-1].split(',')]
            return EnumDomain(labels=labels)
        return super(IntrospectMySQLEnumDomain, self).__call__()


class IntrospectMySQLIntegerDomain(IntrospectMySQLDomain):

    call('tinyint', 'smallint', 'mediumint', 'int', 'bigint')

    def __call__(self):
        if self.data_type == 'tinyint' and self.column_type == 'tinyint(1)':
            return BooleanDomain()
        return IntegerDomain()


class IntrospectMySQLDecimalDomain(IntrospectMySQLDomain):

    call('decimal')

    def __call__(self):
        return DecimalDomain(precision=self.precision, scale=self.scale)


class IntrospectMySQLFloatDomain(IntrospectMySQLDomain):

    call('float', 'double')

    def __call__(self):
        return FloatDomain()


class IntrospectMySQLDateDomain(IntrospectMySQLDomain):

    call('date')

    def __call__(self):
        return DateDomain()


class IntrospectMySQLTimeDomain(IntrospectMySQLDomain):

    call('time')

    def __call__(self):
        return TimeDomain()


class IntrospectMySQLDateTimeDomain(IntrospectMySQLDomain):

    call('datetime', 'timestamp')

    def __call__(self):
        return DateTimeDomain()


