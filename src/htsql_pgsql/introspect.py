#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_pgsql.introspect`
=============================

This module implements the introspection adapter for PostgreSQL.
"""


from htsql.introspect import Introspect
from htsql.entity import (CatalogEntity, SchemaEntity, TableEntity,
                          ColumnEntity, UniqueKeyEntity, PrimaryKeyEntity,
                          ForeignKeyEntity)
from .domain import (PGBooleanDomain, PGIntegerDomain, PGFloatDomain,
                     PGDecimalDomain, PGCharDomain, PGVarCharDomain,
                     PGTextDomain, PGEnumDomain, PGDateDomain,
                     PGOpaqueDomain)
from htsql.connect import Connect
from htsql.util import Record


class Meta(object):
    """
    Loads raw meta-data from the `pg_catalog` schema.
    """

    def __init__(self):
        connect = Connect()
        connection = connect()
        cursor = connection.cursor()
        self.pg_namespace = self.fetch(cursor, 'pg_catalog.pg_namespace')
        self.pg_class = self.fetch(cursor, 'pg_catalog.pg_class')
        self.pg_class_by_namespace = self.group(self.pg_class,
                                                self.pg_namespace,
                                                'relnamespace')
        self.pg_type = self.fetch(cursor, 'pg_catalog.pg_type')
        self.pg_attribute = self.fetch(cursor, 'pg_catalog.pg_attribute',
                                       ('attrelid', 'attnum'))
        self.pg_attribute_by_class = self.group(self.pg_attribute,
                                                self.pg_class,
                                                'attrelid')
        self.pg_enum = self.fetch(cursor, 'pg_catalog.pg_enum')
        self.pg_enum_by_type = self.group(self.pg_enum, self.pg_type,
                                          'enumtypid')
        self.pg_constraint = self.fetch(cursor, 'pg_catalog.pg_constraint')
        self.pg_constraint_by_class = self.group(self.pg_constraint,
                                                 self.pg_class, 'conrelid')

    def fetch(self, cursor, table_name, key_names=('oid',)):
        rows = {}
        cursor.execute("SELECT %s, * FROM %s" % (", ".join(key_names),
                                                 table_name))
        for items in cursor.fetchall():
            key = tuple(items[idx] for idx in range(len(key_names)))
            if len(key) == 1:
                key = key[0]
            attributes = {}
            for kind, item in zip(cursor.description, items)[len(key_names):]:
                name = kind[0]
                attributes[name] = item
            record = Record(**attributes)
            rows[key] = record
        return rows

    def group(self, targets, bases, attribute):
        groups = {}
        for base_key in bases:
            groups[base_key] = {}
        for target_key in targets:
            target = targets[target_key]
            group_key = getattr(target, attribute)
            if group_key not in groups:
                continue
            groups[group_key][target_key] = target
        return groups


class IntrospectPGSQL(Introspect):
    """
    Implements the introspection adapter for PostgreSQL.
    """

    def __init__(self):
        super(IntrospectPGSQL, self).__init__()
        self.meta = Meta()

    def __call__(self):
        return self.introspect_catalog()

    def introspect_catalog(self):
        schemas = self.introspect_schemas()
        return CatalogEntity(schemas)

    def permit_schema(self, schema_name):
        if schema_name in ['pg_catalog', 'information_schema']:
            return False
        return True

    def permit_table(self, schema_name, table_name):
        return True

    def permit_column(self, schema_name, table_name, column_name):
        if column_name in ['tableoid', 'cmax', 'xmax', 'cmin', 'xmin', 'ctid']:
            return False
        return True

    def introspect_schemas(self):
        schemas = []
        for oid in sorted(self.meta.pg_namespace):
            nsp = self.meta.pg_namespace[oid]
            name = nsp.nspname
            if not self.permit_schema(name):
                continue
            tables = self.introspect_tables(oid)
            schema = SchemaEntity(name, tables)
            schemas.append(schema)
        schemas.sort(key=(lambda s: s.name))
        return schemas

    def introspect_tables(self, schema_oid):
        schema_name = self.meta.pg_namespace[schema_oid].nspname
        tables = []
        for oid in sorted(self.meta.pg_class_by_namespace[schema_oid]):
            rel = self.meta.pg_class[oid]
            if rel.relkind not in ('r', 'v'):
                continue
            name = rel.relname
            if not self.permit_table(schema_name, name):
                continue
            columns = self.introspect_columns(oid)
            unique_keys = self.introspect_unique_keys(oid)
            foreign_keys = self.introspect_foreign_keys(oid)
            table = TableEntity(schema_name, name,
                                columns, unique_keys, foreign_keys)
            tables.append(table)
        tables.sort(key=(lambda t: t.name))
        return tables

    def introspect_columns(self, table_oid):
        rel = self.meta.pg_class[table_oid]
        schema_name = self.meta.pg_namespace[rel.relnamespace].nspname
        table_name = rel.relname
        columns = []
        for relid, num in sorted(self.meta.pg_attribute_by_class[table_oid]):
            att = self.meta.pg_attribute[relid, num]
            name = att.attname
            if att.attisdropped:
                continue
            if not self.permit_column(schema_name, table_name, name):
                continue
            domain = self.introspect_domain(relid, num)
            typ = self.meta.pg_type[att.atttypid]
            is_nullable = (not att.attnotnull)
            has_default = (att.atthasdef or typ.typdefault is not None)
            column = ColumnEntity(schema_name, table_name, name, domain,
                                  is_nullable, has_default)
            columns.append(column)
        return columns

    def introspect_unique_keys(self, table_oid):
        rel = self.meta.pg_class[table_oid]
        schema_name = self.meta.pg_namespace[rel.relnamespace].nspname
        table_name = rel.relname
        unique_keys = []
        for oid in sorted(self.meta.pg_constraint_by_class[table_oid]):
            con = self.meta.pg_constraint[oid]
            if con.contype not in ('p', 'u'):
                continue
            column_names = []
            for key in con.conkey:
                att = self.meta.pg_attribute[table_oid, key]
                column_names.append(att.attname)
            if not all(self.permit_column(schema_name, table_name, column_name)
                       for column_name in column_names):
                continue
            if con.contype == 'p':
                unique_key = PrimaryKeyEntity(schema_name, table_name,
                                              column_names)
            else:
                unique_key = UniqueKeyEntity(schema_name, table_name,
                                             column_names)
            unique_keys.append(unique_key)
        return unique_keys

    def introspect_foreign_keys(self, table_oid):
        rel = self.meta.pg_class[table_oid]
        schema_name = self.meta.pg_namespace[rel.relnamespace].nspname
        table_name = rel.relname
        foreign_keys = []
        for oid in sorted(self.meta.pg_constraint_by_class[table_oid]):
            con = self.meta.pg_constraint[oid]
            if con.contype != 'f':
                continue
            column_names = []
            for key in con.conkey:
                att = self.meta.pg_attribute[table_oid, key]
                column_names.append(att.attname)
            rel = self.meta.pg_class[con.confrelid]
            nsp = self.meta.pg_namespace[rel.relnamespace]
            target_schema_name = nsp.nspname
            target_table_name = rel.relname
            target_column_names = []
            for key in con.confkey:
                att = self.meta.pg_attribute[con.confrelid, key]
                target_column_names.append(att.attname)
            if not all(self.permit_column(schema_name, table_name, column_name)
                       for column_name in column_names):
                continue
            if not self.permit_schema(target_schema_name):
                continue
            if not self.permit_table(target_schema_name, target_table_name):
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

    def introspect_domain(self, relid, num):
        att = self.meta.pg_attribute[relid, num]
        typ = self.meta.pg_type[att.atttypid]
        schema_name = self.meta.pg_namespace[typ.typnamespace].nspname
        name = typ.typname
        base = typ
        while base.typtype == 'd':
            if att.atttypmod == -1:
                att.atttypmod = base.typtypmod
            base = self.meta.pg_type[base.typbasetype]
        base_schema_name = self.meta.pg_namespace[base.typnamespace].nspname
        base_name = base.typname
        if base.typtype == 'e':
            labels = []
            for oid in sorted(self.meta.pg_enum_by_type[att.atttypid]):
                enum = self.meta.pg_enum[oid]
                labels.append(enum.enumlabel)
            return PGEnumDomain(schema_name, name, labels=labels)
        if base_schema_name == 'pg_catalog':
            if base_name == 'bool':
                return PGBooleanDomain(schema_name, name)
            if base_name in ['int2', 'int4', 'int8']:
                return PGIntegerDomain(schema_name, name, size=8*base.typlen)
            if base_name in ['float4', 'float8']:
                return PGFloatDomain(schema_name, name, size=8*base.typlen)
            if base_name == 'numeric':
                precision = None
                scale = None
                if att.atttypmod != -1:
                    precision = ((att.atttypmod-4) >> 0x10) & 0xFFFF
                    scale = (att.atttypmod-4) & 0xFFFF
                return PGDecimalDomain(schema_name, name,
                                       precision=precision, scale=scale)
            if base_name == 'bpchar':
                length = att.atttypmod-4 if att.atttypmod != -1 else None
                return PGCharDomain(schema_name, name, length=length)
            if base_name == 'varchar':
                length = att.atttypmod-4 if att.atttypmod != -1 else None
                return PGVarCharDomain(schema_name, name, length=length)
            if base_name == 'text':
                return PGTextDomain(schema_name, name)
            if base_name == 'date':
                return PGDateDomain(schema_name, name)
        return PGOpaqueDomain(schema_name, name)


