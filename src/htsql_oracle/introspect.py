#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_oracle.introspect`
==============================

This module implements the introspection adapter for Oracle.
"""


from htsql.introspect import Introspect
from htsql.entity import (CatalogEntity, SchemaEntity, TableEntity,
                          ColumnEntity, UniqueKeyEntity, PrimaryKeyEntity,
                          ForeignKeyEntity)
from .domain import (OracleBooleanDomain, OracleIntegerDomain,
                     OracleDecimalDomain, OracleFloatDomain,
                     OracleStringDomain, OracleDateTimeDomain,
                     OracleOpaqueDomain)
from htsql.connect import Connect
from htsql.util import Record
import re


class Meta(object):
    """
    Loads raw meta-data from `information_schema`.
    """

    def __init__(self):
        connect = Connect()
        connection = connect()
        cursor = connection.cursor()
        self.users = self.fetch(cursor, 'all_users', ['username'])
        self.tables = self.fetch(cursor, 'all_catalog',
                                 ['owner', 'table_name'])
        self.columns = self.fetch(cursor, 'all_tab_columns',
                                  ['owner', 'table_name', 'column_id'])
        self.constraints = self.fetch(cursor, 'all_constraints',
                                      ['owner', 'constraint_name'])
        self.key_columns = self.fetch(cursor, 'all_cons_columns',
                                      ['owner', 'constraint_name',
                                       'position'])
        self.tables_by_user = self.group(self.tables, self.users,
                                         ['owner'])
        self.columns_by_table = self.group(self.columns, self.tables,
                                           ['owner', 'table_name'])
        self.constraints_by_table = self.group(self.constraints, self.tables,
                                               ['owner', 'table_name'])
        self.key_columns_by_constraint = self.group(self.key_columns,
                        self.constraints, ['owner', 'constraint_name'])

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
            if base_key not in groups:
                continue
            groups[base_key].append(key)
        return groups


class IntrospectOracle(Introspect):
    """
    Implements the introspection adapter for Oracle.
    """

    def __init__(self):
        super(IntrospectOracle, self).__init__()
        self.meta = Meta()

    def __call__(self):
        return self.introspect_catalog()

    def introspect_catalog(self):
        schemas = self.introspect_schemas()
        return CatalogEntity(schemas)

    def permit_schema(self, schema_name):
        if schema_name in ['SYS', 'SYSTEM', 'OUTLN', 'DIP', 'TSMSYS', 'DBSNMP',
                           'CTXSYS', 'XDB', 'ANONYMOUS', 'MDSYS', 'HR',
                           'FLOWS_FILES', 'FLOWS_020100']:
            return False
        if '$' in schema_name:
            return False
        return True

    def permit_table(self, schema_name, table_name):
        if '$' in schema_name or '$' in table_name:
            return False
        return True

    def permit_column(self, schema_name, table_name, column_name):
        if '$' in schema_name or '$' in table_name or '$' in column_name:
            return False
        return True

    def introspect_schemas(self):
        schemas = []
        for key in sorted(self.meta.users):
            record = self.meta.users[key]
            name = record.username
            if not self.permit_schema(name):
                continue
            tables = self.introspect_tables(key)
            schema = SchemaEntity(name, tables)
            schemas.append(schema)
        schemas.sort(key=(lambda s: s.name))
        return schemas

    def introspect_tables(self, schema_key):
        tables = []
        for key in self.meta.tables_by_user[schema_key]:
            record = self.meta.tables[key]
            if record.table_type not in ['TABLE', 'VIEW']:
                continue
            schema_name = record.owner
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
            schema_name = record.owner
            table_name = record.table_name
            name = record.column_name
            if not self.permit_column(schema_name, table_name, name):
                continue
            domain = self.introspect_domain(key)
            is_nullable = (record.nullable == 'Y')
            has_default = (record.data_default is not None)
            column = ColumnEntity(schema_name, table_name, name, domain,
                                  is_nullable, has_default)
            columns.append(column)
        return columns

    def introspect_unique_keys(self, table_key):
        unique_keys = []
        for key in self.meta.constraints_by_table[table_key]:
            record = self.meta.constraints[key]
            if record.constraint_type not in ['P', 'U']:
                continue
            if record.status != 'ENABLED' or record.validated != 'VALIDATED':
                continue
            schema_name = record.owner
            table_name = record.table_name
            column_names = []
            for column_key in self.meta.key_columns_by_constraint[key]:
                column_record = self.meta.key_columns[column_key]
                column_names.append(column_record.column_name)
            if not all(self.permit_column(schema_name, table_name, column_name)
                       for column_name in column_names):
                continue
            if record.constraint_type == 'P':
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
            if record.constraint_type != 'R':
                continue
            if record.status != 'ENABLED' or record.validated != 'VALIDATED':
                continue
            target_key = (record.r_owner, record.r_constraint_name)
            target_record = self.meta.constraints[target_key]
            schema_name = record.owner
            target_schema_name = target_record.owner
            table_name = record.table_name
            target_table_name = target_record.table_name
            column_names = []
            target_column_names = []
            for column_key in self.meta.key_columns_by_constraint[key]:
                column_record = self.meta.key_columns[column_key]
                column_names.append(column_record.column_name)
            for column_key in self.meta.key_columns_by_constraint[target_key]:
                column_record = self.meta.key_columns[column_key]
                target_column_names.append(column_record.column_name)
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

    boolean_pattern = r"""
        ^ %s \s+ IN \s+ \( (?: 0 \s* , \s* 1 | 1 \s* , \s* 0 ) \) $
    """

    def introspect_domain(self, key):
        record = self.meta.columns[key]
        table_key = (record.owner, record.table_name)
        data_type = record.data_type
        data_length = record.data_length
        data_precision = record.data_precision
        data_scale = record.data_scale
        if data_type in ['CHAR', 'NCHAR']:
            return OracleStringDomain(data_type,
                                      length=data_length,
                                      is_varying=False)
        elif data_type in ['VARCHAR2', 'NVARCHAR2', 'CLOB', 'NCLOB', 'LONG']:
            return OracleStringDomain(data_type,
                                      length=data_length,
                                      is_varying=True)
        elif data_type == 'NUMBER':
            if (data_precision, data_scale) == (1, 0):
                for constraint_key in self.meta.constraints_by_table[table_key]:
                    constraint_record = self.meta.constraints[constraint_key]
                    if (constraint_record.constraint_type == 'C' and
                        re.match(self.boolean_pattern
                                 % re.escape(record.column_name),
                                 constraint_record.search_condition,
                                 re.X|re.I)):
                        return OracleBooleanDomain(data_type)
            if (data_precision, data_scale) == (38, 0):
                return OracleIntegerDomain(data_type)
            return OracleDecimalDomain(data_type,
                                       precision=data_precision,
                                       scale=data_scale)
        elif data_type in ['BINARY_FLOAT', 'BINARY_DOUBLE']:
            return OracleFloatDomain(data_type)
        elif data_type == 'DATE' or data_type.startswith('TIMESTAMP'):
            return OracleDateTimeDomain(data_type)
        return OracleOpaqueDomain(data_type)


