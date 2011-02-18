#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_mysql.introspect`
=============================

This module implements the introspection adapter for MySQL.
"""


from htsql.introspect import Introspect
from htsql.entity import (CatalogEntity, SchemaEntity, TableEntity,
                          ColumnEntity, UniqueKeyEntity, PrimaryKeyEntity,
                          ForeignKeyEntity)
from .domain import (MySQLBooleanDomain, MySQLIntegerDomain,
                     MySQLDecimalDomain, MySQLFloatDomain, MySQLStringDomain,
                     MySQLEnumDomain, MySQLDateDomain, MySQLOpaqueDomain)
from htsql.connect import Connect
from htsql.util import Record


class Meta(object):
    """
    Loads raw meta-data from `information_schema`.
    """

    def __init__(self):
        connect = Connect()
        connection = connect()
        cursor = connection.cursor()
        self.schemata = self.fetch(cursor,
                'information_schema.schemata',
                ['schema_name'])
        self.tables = self.fetch(cursor,
                'information_schema.tables',
                ['table_schema', 'table_name'])
        self.columns = self.fetch(cursor,
                'information_schema.columns',
                ['table_schema', 'table_name', 'ordinal_position'])
        self.constraints = self.fetch(cursor,
                'information_schema.table_constraints',
                ['table_schema', 'table_name',
                 'constraint_schema', 'constraint_name'])
        self.key_columns = self.fetch(cursor,
                'information_schema.key_column_usage',
                ['table_schema', 'table_name',
                 'constraint_schema', 'constraint_name',
                 'ordinal_position'])
        self.tables_by_schema = self.group(self.tables, self.schemata)
        self.columns_by_table = self.group(self.columns, self.tables)
        self.constraints_by_table = self.group(self.constraints, self.tables)
        self.key_columns_by_constraint = self.group(self.key_columns,
                                                    self.constraints)

    def fetch(self, cursor, table_name, id_names):
        rows = {}
        cursor.execute("SELECT * FROM %s" % table_name)
        for items in cursor.fetchall():
            attributes = {}
            for kind, item in zip(cursor.description, items):
                name = kind[0].lower()
                if isinstance(item, unicode):
                    item = item.encode('utf-8')
                attributes[name] = item
            key = tuple(attributes[name] for name in id_names)
            record = Record(**attributes)
            rows[key] = record
        return rows

    def group(self, targets, bases):
        groups = {}
        if not targets or not bases:
            return groups
        target_length = max(len(key) for key in targets)
        assert all(target_length == len(key) for key in targets)
        base_length = max(len(key) for key in bases)
        assert all(base_length == len(key) for key in bases)
        assert base_length < target_length
        for key in bases:
            groups[key] = []
        for key in sorted(targets):
            base_key = key[:base_length]
            assert base_key in groups
            groups[base_key].append(key)
        return groups


class IntrospectMySQL(Introspect):
    """
    Implements the introspection adapter for MySQL.
    """

    def __init__(self):
        super(IntrospectMySQL, self).__init__()
        self.meta = Meta()

    def __call__(self):
        return self.introspect_catalog()

    def introspect_catalog(self):
        schemas = self.introspect_schemas()
        return CatalogEntity(schemas)

    def permit_schema(self, schema_name):
        if schema_name in ['mysql', 'information_schema']:
            return False
        return True

    def permit_table(self, schema_name, table_name):
        return True

    def permit_column(self, schema_name, table_name, column_name):
        return True

    def introspect_schemas(self):
        schemas = []
        for key in sorted(self.meta.schemata):
            record = self.meta.schemata[key]
            name = record.schema_name
            if not self.permit_schema(name):
                continue
            tables = self.introspect_tables(key)
            schema = SchemaEntity(name, tables)
            schemas.append(schema)
        schemas.sort(key=(lambda s: s.name))
        return schemas

    def introspect_tables(self, schema_key):
        tables = []
        for key in self.meta.tables_by_schema[schema_key]:
            record = self.meta.tables[key]
            if record.table_type not in ['BASE TABLE', 'VIEW']:
                continue
            schema_name = record.table_schema
            name = record.table_name
            if not self.permit_table(schema_name, name):
                continue
            columns = self.introspect_columns(key)
            unique_keys = self.introspect_unique_keys(key)
            foreign_keys = self.introspect_foreign_keys(key)
            table = TableEntity(schema_name, name,
                                columns, unique_keys, foreign_keys)
            tables.append(table)
        tables.sort(key=(lambda t: t.name))
        return tables

    def introspect_columns(self, table_key):
        columns = []
        for key in self.meta.columns_by_table[table_key]:
            record = self.meta.columns[key]
            schema_name = record.table_schema
            table_name = record.table_name
            name = record.column_name
            if not self.permit_column(schema_name, table_name, name):
                continue
            domain = self.introspect_domain(key)
            is_nullable = (record.is_nullable == 'YES')
            has_default = (record.column_default is not None)
            column = ColumnEntity(schema_name, table_name, name, domain,
                                  is_nullable, has_default)
            columns.append(column)
        return columns

    def introspect_unique_keys(self, table_key):
        unique_keys = []
        for key in self.meta.constraints_by_table[table_key]:
            record = self.meta.constraints[key]
            if record.constraint_type not in ['PRIMARY KEY', 'UNIQUE']:
                continue
            schema_name = record.table_schema
            table_name = record.table_name
            column_names = []
            for column_key in self.meta.key_columns_by_constraint[key]:
                column_record = self.meta.key_columns[column_key]
                column_names.append(column_record.column_name)
            if not all(self.permit_column(schema_name, table_name, column_name)
                       for column_name in column_names):
                continue
            if record.constraint_type == 'PRIMARY KEY':
                unique_key = PrimaryKeyEntity(schema_name, table_name,
                                              column_names)
            else:
                unique_key = UniqueKeyEntity(schema_name, table_name,
                                             column_names)
            unique_keys.append(unique_key)
        return unique_keys

    def introspect_foreign_keys(self, table_key):
        foreign_keys = []
        for key in self.meta.constraints_by_table[table_key]:
            record = self.meta.constraints[key]
            if record.constraint_type != 'FOREIGN KEY':
                continue
            schema_name = record.table_schema
            target_schema_name = None
            table_name = record.table_name
            target_table_name = None
            column_names = []
            target_column_names = []
            for column_key in self.meta.key_columns_by_constraint[key]:
                column_record = self.meta.key_columns[column_key]
                if target_schema_name is None:
                    target_schema_name = column_record.referenced_table_schema
                if target_table_name is None:
                    target_table_name = column_record.referenced_table_name
                column_names.append(column_record.column_name)
                target_column_names.append(column_record.referenced_column_name)
            if not self.permit_schema(target_schema_name):
                continue
            if not self.permit_table(target_schema_name, target_table_name):
                continue
            if not all(self.permit_column(schema_name, table_name, column_name)
                       for column_name in column_names):
                continue
            if not all(self.permit_column(target_schema_name,
                                          target_table_name,
                                          target_column_name)
                       for target_column_name in target_column_names):
                continue
            foreign_key = ForeignKeyEntity(schema_name, table_name,
                                           column_names,
                                           target_schema_name,
                                           target_table_name,
                                           target_column_names)
            foreign_keys.append(foreign_key)
        return foreign_keys

    def introspect_domain(self, key):
        record = self.meta.columns[key]
        data_type = record.data_type
        character_maximum_length = record.character_maximum_length
        if isinstance(character_maximum_length, long):
            character_maximum_length = int(character_maximum_length)
        numeric_precision = record.numeric_precision
        if isinstance(numeric_precision, long):
            numeric_precision = int(numeric_precision)
        numeric_scale = record.numeric_scale
        if isinstance(numeric_scale, long):
            numeric_scale = int(numeric_scale)
        column_type = record.column_type
        if data_type == 'char':
            return MySQLStringDomain(data_type,
                                     length=character_maximum_length,
                                     is_varying=False)
        elif data_type in ['varchar', 'tinytext',
                           'text', 'mediumtext', 'longtext']:
            return MySQLStringDomain(data_type,
                                     length=character_maximum_length,
                                     is_varying=True)
        elif (data_type == 'enum' and column_type.startswith('enum(') and
                                      column_type.endswith(')')):
            labels = [item[1:-1] for item in column_type[5:-1].split(',')]
            return MySQLEnumDomain(data_type, labels=labels)
        elif data_type == 'tinyint' and column_type == 'tinyint(1)':
            return MySQLBooleanDomain(data_type)
        elif data_type in ['tinyint', 'smallint', 'mediumint',
                           'int', 'bigint']:
            return MySQLIntegerDomain(data_type)
        elif data_type == 'decimal':
            return MySQLDecimalDomain(data_type,
                                      precision=numeric_precision,
                                      scale=numeric_scale)
        elif data_type in ['float', 'double']:
            return MySQLFloatDomain(data_type)
        elif data_type == 'date':
            return MySQLDateDomain(data_type)
        return MySQLOpaqueDomain(data_type)


