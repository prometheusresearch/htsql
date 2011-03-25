#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_mssql.introspect`
=============================

This module implements the introspection adapter for MS SQL Server.
"""


from htsql.introspect import Introspect
from htsql.entity import (CatalogEntity, SchemaEntity, TableEntity,
                          ColumnEntity, UniqueKeyEntity, PrimaryKeyEntity,
                          ForeignKeyEntity)
from .domain import (MSSQLBooleanDomain, MSSQLIntegerDomain,
                     MSSQLDecimalDomain, MSSQLFloatDomain, MSSQLStringDomain,
                     MSSQLDateTimeDomain, MSSQLOpaqueDomain)
from htsql.connect import Connect
from htsql.util import Record


class Meta(object):
    """
    Loads raw meta-data from the `sys` schema.
    """

    def __init__(self):
        connect = Connect()
        connection = connect()
        cursor = connection.cursor()
        self.schemas = self.fetch(cursor, 'sys.schemas', ['schema_id'])
        self.objects = self.fetch(cursor, 'sys.objects', ['object_id'])
        self.columns = self.fetch(cursor, 'sys.columns',
                                  ['object_id', 'column_id'])
        self.types = self.fetch(cursor, 'sys.types', ['user_type_id'])
        self.key_constraints = self.fetch(cursor, 'sys.key_constraints',
                                          ['object_id'])
        self.indexes = self.fetch(cursor, 'sys.indexes',
                                  ['object_id', 'index_id'])
        self.index_columns = self.fetch(cursor, 'sys.index_columns',
                                        ['object_id', 'index_id',
                                         'index_column_id'])
        self.foreign_keys = self.fetch(cursor, 'sys.foreign_keys',
                                      ['object_id'])
        self.foreign_key_columns = self.fetch(cursor, 'sys.foreign_key_columns',
                                              ['constraint_object_id',
                                               'constraint_column_id'])
        self.objects_by_schema = self.group(self.objects, self.schemas,
                                            ['schema_id'])
        self.columns_by_object = self.group(self.columns, self.objects,
                                            ['object_id'])
        self.key_constraints_by_parent = self.group(self.key_constraints,
                                                    self.objects,
                                                    ['parent_object_id'])
        self.foreign_keys_by_parent = self.group(self.foreign_keys,
                                                 self.objects,
                                                 ['parent_object_id'])
        self.columns_by_index = self.group(self.index_columns, self.indexes,
                                           ['object_id', 'index_id'])
        self.columns_by_foreign_key = self.group(self.foreign_key_columns,
                                                 self.foreign_keys,
                                                 ['constraint_object_id'])

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

    def group(self, targets, bases, id_names):
        groups = {}
        if not targets or not bases:
            return groups
        for key in bases:
            groups[key] = []
        for key in sorted(targets):
            record = targets[key]
            base_key = tuple(getattr(record, name) for name in id_names)
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
        if schema_name in ['dbo', 'guest', 'INFORMATION_SCHEMA', 'sys']:
            return False
        if schema_name.startswith('db_'):
            return False
        return True

    def permit_table(self, schema_name, table_name):
        return True

    def permit_column(self, schema_name, table_name, column_name):
        return True

    def introspect_schemas(self):
        schemas = []
        for key in sorted(self.meta.schemas):
            record = self.meta.schemas[key]
            name = record.name
            if not self.permit_schema(name):
                continue
            tables = self.introspect_tables(key)
            schema = SchemaEntity(name, tables)
            schemas.append(schema)
        schemas.sort(key=(lambda s: s.name))
        return schemas

    def introspect_tables(self, schema_key):
        schema_record = self.meta.schemas[schema_key]
        schema_name = schema_record.name
        tables = []
        for key in self.meta.objects_by_schema[schema_key]:
            record = self.meta.objects[key]
            if record.type not in ['U ', 'V ']:
                continue
            name = record.name
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
        table_record = self.meta.objects[table_key]
        schema_record = self.meta.schemas[table_record.schema_id,]
        schema_name = schema_record.name
        table_name = table_record.name
        columns = []
        for key in self.meta.columns_by_object[table_key]:
            record = self.meta.columns[key]
            name = record.name
            if not self.permit_column(schema_name, table_name, name):
                continue
            domain = self.introspect_domain(key)
            is_nullable = bool(record.is_nullable)
            has_default = bool(record.default_object_id)
            column = ColumnEntity(schema_name, table_name, name, domain,
                                  is_nullable, has_default)
            columns.append(column)
        return columns

    def introspect_unique_keys(self, table_key):
        table_record = self.meta.objects[table_key]
        schema_record = self.meta.schemas[table_record.schema_id,]
        schema_name = schema_record.name
        table_name = table_record.name
        unique_keys = []
        for key in self.meta.key_constraints_by_parent[table_key]:
            record = self.meta.key_constraints[key]
            index_key = (record.parent_object_id, record.unique_index_id)
            index_record = self.meta.indexes[index_key]
            if index_record.is_disabled:
                continue
            assert (index_record.is_primary_key or
                    index_record.is_unique_constraint)
            column_names = []
            for index_column_key in self.meta.columns_by_index[index_key]:
                index_column_record = self.meta.index_columns[index_column_key]
                column_key = (index_column_record.object_id,
                              index_column_record.column_id)
                column_record = self.meta.columns[column_key]
                column_names.append(column_record.name)
            if not all(self.permit_column(schema_name, table_name, column_name)
                       for column_name in column_names):
                continue
            if index_record.is_primary_key:
                unique_key = PrimaryKeyEntity(schema_name, table_name,
                                              column_names)
            else:
                unique_key = UniqueKeyEntity(schema_name, table_name,
                                             column_names)
            unique_keys.append(unique_key)
        return unique_keys

    def introspect_foreign_keys(self, table_key):
        table_record = self.meta.objects[table_key]
        schema_record = self.meta.schemas[table_record.schema_id,]
        schema_name = schema_record.name
        table_name = table_record.name
        foreign_keys = []
        for key in self.meta.foreign_keys_by_parent[table_key]:
            record = self.meta.foreign_keys[key]
            if record.is_disabled:
                continue
            target_table_key = (record.referenced_object_id,)
            target_table_record = self.meta.objects[target_table_key]
            target_schema_key = (target_table_record.schema_id,)
            target_schema_record = self.meta.schemas[target_schema_key]
            target_schema_name = target_schema_record.name
            target_table_name = target_table_record.name
            column_names = []
            target_column_names = []
            for column_key in self.meta.columns_by_foreign_key[key]:
                fk_column_record = self.meta.foreign_key_columns[column_key]
                column_record = self.meta.columns[
                                        fk_column_record.parent_object_id,
                                        fk_column_record.parent_column_id]
                target_column_record = self.meta.columns[
                                        fk_column_record.referenced_object_id,
                                        fk_column_record.referenced_column_id]
                column_names.append(column_record.name)
                target_column_names.append(target_column_record.name)
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
        column_record = self.meta.columns[key]
        type_record = self.meta.types[column_record.system_type_id,]
        schema_record = self.meta.schemas[type_record.schema_id,]
        schema_name = schema_record.name
        type_name = type_record.name
        name = (schema_name, type_name)
        max_length = column_record.max_length
        if max_length == -1:
            max_length = None
        precision = column_record.precision
        scale = column_record.scale
        if name in [('sys', 'char'), ('sys', 'nchar')]:
            return MSSQLStringDomain(schema_name, type_name,
                                     length=max_length,
                                     is_varying=False)
        if name in [('sys', 'varchar'), ('sys', 'nvarchar')]:
            return MSSQLStringDomain(schema_name, type_name,
                                     length=max_length,
                                     is_varying=False)
        elif name == ('sys', 'bit'):
            return MSSQLBooleanDomain(schema_name, type_name)
        elif name in [('sys', 'smallint'), ('sys', 'int'), ('sys', 'bigint')]:
            return MSSQLIntegerDomain(schema_name, type_name,
                                      size=max_length*8)
        elif name in [('sys', 'decimal'), ('sys', 'numeric')]:
            return MSSQLDecimalDomain(schema_name, type_name,
                                      precision=precision, scale=scale)
        elif name in [('sys', 'real'), ('sys', 'float')]:
            return MSSQLFloatDomain(schema_name, type_name,
                                    size=max_length*8)
        elif name in [('sys', 'datetime'), ('sys', 'smalldatetime')]:
            return MSSQLDateTimeDomain(schema_name, type_name)
        return MSSQLOpaqueDomain(schema_name, type_name)


