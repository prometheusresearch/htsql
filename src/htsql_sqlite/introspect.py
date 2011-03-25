#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_sqlite.introspect`
==============================

This module implements the introspection adapter for SQLite.
"""


from htsql.introspect import Introspect
from htsql.entity import (CatalogEntity, SchemaEntity, TableEntity,
                          ColumnEntity, UniqueKeyEntity, PrimaryKeyEntity,
                          ForeignKeyEntity)
from .domain import (SQLiteBooleanDomain, SQLiteIntegerDomain,
                     SQLiteFloatDomain, SQLiteTextDomain, SQLiteDateDomain,
                     SQLiteDateTimeDomain, SQLiteOpaqueDomain)
from htsql.connect import Connect
from htsql.util import Record


class Meta(object):
    """
    Loads raw meta-data from SQLite system tables.
    """

    def __init__(self):
        connect = Connect()
        connection = connect()
        cursor = connection.cursor()
        self.sqlite_master = self.fetch(cursor,
                                        """SELECT *
                                           FROM sqlite_master
                                           WHERE type = 'table'
                                           ORDER BY name""")
        self.table_info = {}
        self.index_list = {}
        self.index_info = {}
        self.foreign_key_list = {}
        for row in self.sqlite_master:
            rows = self.fetch(cursor, """PRAGMA table_info(%s)""", row.name)
            self.table_info[row.name] = rows
            rows = self.fetch(cursor, """PRAGMA index_list(%s)""", row.name)
            self.index_list[row.name] = rows
            for index_row in self.index_list[row.name]:
                rows = self.fetch(cursor, """PRAGMA index_info(%s)""",
                                  index_row.name)
                self.index_info[index_row.name] = rows
            rows = self.fetch(cursor, """PRAGMA foreign_key_list(%s)""",
                              row.name)
            self.foreign_key_list[row.name] = rows

    def fetch(self, cursor, query, name=None):
        if name is not None:
            query = query % ('"%s"' % name.replace('"', '""'))
        rows = []
        cursor.execute(query)
        for items in cursor.fetchall():
            attributes = {}
            for kind, item in zip(cursor.description, items):
                name = kind[0]
                if isinstance(item, unicode):
                    item = item.encode('utf-8')
                attributes[name] = item
            record = Record(**attributes)
            rows.append(record)
        return rows


class IntrospectSQLite(Introspect):
    """
    Implements the introspection adapter for SQLite.
    """

    def __init__(self):
        super(IntrospectSQLite, self).__init__()
        self.meta = Meta()

    def __call__(self):
        return self.introspect_catalog()

    def permit_schema(self, schema_name):
        return True

    def permit_table(self, schema_name, table_name):
        return True

    def permit_column(self, schema_name, table_name, column_name):
        return True

    def introspect_catalog(self):
        tables = self.introspect_tables()
        schema = SchemaEntity('_', tables)
        catalog = CatalogEntity([schema])
        return catalog

    def introspect_tables(self):
        tables = []
        for row in self.meta.sqlite_master:
            name = row.name
            if not self.permit_table('_', name):
                continue
            columns = self.introspect_columns(name)
            unique_keys = self.introspect_unique_keys(name)
            foreign_keys = self.introspect_foreign_keys(name)
            table = TableEntity('_', name, columns, unique_keys, foreign_keys)
            tables.append(table)
        return tables

    def introspect_columns(self, table_name):
        columns = []
        for row in self.meta.table_info[table_name]:
            name = row.name
            if not self.permit_column('_', table_name, name):
                continue
            domain = self.introspect_domain(table_name, name, row.type)
            is_nullable = (not row.notnull)
            has_default = (row.dflt_value is not None)
            column = ColumnEntity('_', table_name, name, domain,
                                  is_nullable, has_default)
            columns.append(column)
        return columns

    def introspect_unique_keys(self, table_name):
        unique_keys = []
        column_names = []
        for row in self.meta.table_info[table_name]:
            if row.pk:
                column_names.append(row.name)
        if column_names:
            unique_key = PrimaryKeyEntity('_', table_name, column_names)
            unique_keys.append(unique_key)
        for row in self.meta.index_list[table_name]:
            if not row.unique:
                continue
            index_name = row.name
            column_names = []
            for index_row in self.meta.index_info[index_name]:
                column_names.append(index_row.name)
            if not all(self.permit_column('_', table_name, column_name)
                       for column_name in column_names):
                continue
            unique_key = UniqueKeyEntity('_', table_name, column_names)
            unique_keys.append(unique_key)
        return unique_keys

    def introspect_foreign_keys(self, table_name):
        foreign_keys = []
        ids = []
        columns_by_id = {}
        target_table_by_id = {}
        target_columns_by_id = {}
        for row in self.meta.foreign_key_list[table_name]:
            if row.id not in target_table_by_id:
                ids.append(row.id)
                columns_by_id[row.id] = []
                target_table_by_id[row.id] = row.table
                target_columns_by_id[row.id] = []
            columns_by_id[row.id].append(getattr(row, 'from'))
            target_columns_by_id[row.id].append(getattr(row, 'to'))
        for id in ids:
            column_names = columns_by_id[id]
            target_table_name = target_table_by_id[id]
            target_column_names = target_columns_by_id[id]
            if not all(self.permit_column('_', table_name, column_name)
                       for column_name in column_names):
                continue
            if not self.permit_table('_', target_table_name):
                continue
            if not all(self.permit_column('_', target_table_name, column_name)
                       for column_name in target_column_names):
                continue
            foreign_key = ForeignKeyEntity('_', table_name, column_names,
                                           '_', target_table_name,
                                           target_column_names)
            foreign_keys.append(foreign_key)
        return foreign_keys

    def introspect_domain(self, table_name, column_name, type_name):
        name = type_name
        type_name = type_name.lower()
        if 'int' in type_name:
            return SQLiteIntegerDomain(name)
        if 'char' in type_name or 'clob' in type_name or 'text' in type_name:
            return SQLiteTextDomain(name)
        if 'real' in type_name or 'floa' in type_name or 'doub' in type_name:
            return SQLiteFloatDomain(name)
        if 'bool' in type_name:
            return SQLiteBooleanDomain(name)
        if 'datetime' in type_name or 'timestamp' in type_name:
            return SQLiteDateTimeDomain(name)
        if 'date' in type_name:
            return SQLiteDateDomain(name)
        return SQLiteOpaqueDomain(name)


