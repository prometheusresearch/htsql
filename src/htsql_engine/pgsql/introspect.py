#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql_engine.pgsql.introspect`
====================================

This module implements the introspection adapter for PostgreSQL.
"""


from htsql.adapter import Protocol, named
from htsql.introspect import Introspect
from htsql.entity import make_catalog
from .domain import (PGBooleanDomain, PGIntegerDomain, PGFloatDomain,
                     PGDecimalDomain, PGCharDomain, PGVarCharDomain,
                     PGTextDomain, PGEnumDomain, PGDateDomain,
                     PGTimeDomain, PGDateTimeDomain, PGOpaqueDomain)
from htsql.connect import connect
import itertools
import fnmatch


class IntrospectPGSQL(Introspect):

    system_schema_names = [u'pg_*', u'information_schema']
    system_table_names = []
    system_column_names = [u'tableoid', u'cmax', u'xmax',
                           u'cmin', u'xmin', u'ctid']

    def __call__(self):
        connection = connect()
        cursor = connection.cursor()

        catalog = make_catalog()

        cursor.execute("""
            SELECT n.oid, n.nspname
            FROM pg_catalog.pg_namespace n
            ORDER BY n.nspname
        """)
        schema_by_oid = {}
        for row in cursor.fetchnamed():
            name = row.nspname
            if any(fnmatch.fnmatchcase(name, pattern)
                   for pattern in self.system_schema_names):
                continue
            schema = catalog.add_schema(name)
            schema_by_oid[row.oid] = schema

        cursor.execute("""
            SELECT CURRENT_SCHEMAS(TRUE)
        """)
        search_path = cursor.fetchone()[0]
        for idx, name in enumerate(search_path):
            priority = len(search_path)-idx
            if name in catalog.schemas:
                catalog.schemas[name].set_priority(priority)

        table_by_oid = {}
        cursor.execute("""
            SELECT c.oid, c.relnamespace, c.relname
            FROM pg_catalog.pg_class c
            WHERE c.relkind IN ('r', 'v') AND
                  HAS_TABLE_PRIVILEGE(c.oid, 'SELECT')
            ORDER BY c.relnamespace, c.relname
        """)
        for row in cursor.fetchnamed():
            if row.relnamespace not in schema_by_oid:
                continue
            name = row.relname
            if any(fnmatch.fnmatchcase(name, pattern)
                   for pattern in self.system_table_names):
                continue
            schema = schema_by_oid[row.relnamespace]
            table = schema.add_table(row.relname)
            table_by_oid[row.oid] = table

        cursor.execute("""
            SELECT t.oid, n.nspname, t.typname, t.typtype,
                   t.typbasetype, t.typlen, t.typtypmod, t.typdefault
            FROM pg_catalog.pg_type t
            JOIN pg_catalog.pg_namespace n ON (t.typnamespace = n.oid)
            ORDER BY n.oid, t.typname
        """)
        typrows_by_oid = dict((row.oid, row) for row in cursor.fetchnamed())

        # FIXME: respect `enumsortorder` if available
        cursor.execute("""
            SELECT e.enumtypid, e.enumlabel
            FROM pg_catalog.pg_enum e
            ORDER BY e.enumtypid, e.oid
        """)
        enumrows_by_typid = dict((key, list(group))
                                 for key, group
                                 in itertools.groupby(cursor.fetchnamed(),
                                                      lambda r: r.enumtypid))

        column_by_num = {}
        cursor.execute("""
            SELECT a.attrelid, a.attnum, a.attname, a.atttypid, a.atttypmod,
                   a.attnotnull, a.atthasdef, a.attisdropped
            FROM pg_catalog.pg_attribute a
            ORDER BY a.attrelid, a.attnum
        """)
        for row in cursor.fetchnamed():
            if row.attisdropped:
                continue
            if any(fnmatch.fnmatchcase(row.attname, pattern)
                   for pattern in self.system_column_names):
                continue
            if row.attrelid not in table_by_oid:
                continue
            table = table_by_oid[row.attrelid]
            name = row.attname
            modifier = row.atttypmod
            typrow = typrows_by_oid[row.atttypid]
            length = typrow.typlen
            if modifier == -1:
                modifier = typrow.typtypmod
            is_nullable = (not row.attnotnull)
            has_default = (row.atthasdef or typrow.typdefault is not None)
            introspect_domain = IntrospectPGSQLDomain(typrow.nspname,
                                                      typrow.typname,
                                                      length, modifier)
            domain = introspect_domain()
            while isinstance(domain, PGOpaqueDomain) and typrow.typtype == 'd':
                typrow = typrows_by_oid[typrow.typbasetype]
                if modifier == -1:
                    modifier = typrow.typtypmod
                introspect_domain = IntrospectPGSQLDomain(typrow.nspname,
                                                          typrow.typname,
                                                          length, modifier)
                domain = introspect_domain()
            if (isinstance(domain, PGOpaqueDomain) and typrow.typtype == 'e'
                                        and typrow.oid in enumrows_by_typid):
                enumrows = enumrows_by_typid[typrow.oid]
                labels = [enumrow.enumlabel
                          for enumrow in enumrows]
                domain = PGEnumDomain(typrow.nspname, typrow.typname,
                                      labels=labels)
            column = table.add_column(name, domain, is_nullable, has_default)
            column_by_num[row.attrelid, row.attnum] = column

        cursor.execute("""
            SELECT c.contype, c.confmatchtype,
                   c.conrelid, c.conkey, c.confrelid, c.confkey
            FROM pg_catalog.pg_constraint c
            WHERE c.contype IN ('p', 'u', 'f')
            ORDER BY c.oid
        """)
        for row in cursor.fetchnamed():
            if row.conrelid not in table_by_oid:
                continue
            table = table_by_oid[row.conrelid]
            if not all((row.conrelid, num) in column_by_num
                       for num in row.conkey):
                continue
            columns = [column_by_num[row.conrelid, num]
                       for num in row.conkey]
            if row.contype in ('p', 'u'):
                is_primary = (row.contype == 'p')
                table.add_unique_key(columns, is_primary)
            elif row.contype == 'f':
                if row.confrelid not in table_by_oid:
                    continue
                target_table = table_by_oid[row.confrelid]
                if not all((row.confrelid, num) in column_by_num
                           for num in row.confkey):
                    continue
                target_columns = [column_by_num[row.confrelid, num]
                                  for num in row.confkey]
                is_partial = (len(target_columns) > 1 and
                              any(column.is_nullable
                                  for column in target_columns) and
                              row.confmatchtype == 'u')
                table.add_foreign_key(columns, target_table, target_columns,
                                      is_partial)

        connection.release()
        return catalog


class IntrospectPGSQLDomain(Protocol):

    @classmethod
    def dispatch(component, schema_name, name, *args, **kwds):
        return (schema_name.encode('utf-8'), name.encode('utf-8'))

    @classmethod
    def matches(component, dispatch_key):
        return (dispatch_key in component.names)

    def __init__(self, schema_name, name, length, modifier):
        self.schema_name = schema_name
        self.name = name
        self.length = length
        self.modifier = modifier

    def __call__(self):
        return PGOpaqueDomain(self.schema_name, self.name)


class IntrospectPGSQLBooleanDomain(IntrospectPGSQLDomain):

    named(('pg_catalog', 'bool'))

    def __call__(self):
        return PGBooleanDomain(self.schema_name, self.name)


class IntrospectPGSQLIntegerDomain(IntrospectPGSQLDomain):

    named(('pg_catalog', 'int2'),
          ('pg_catalog', 'int4'),
          ('pg_catalog', 'int8'))

    def __call__(self):
        return PGIntegerDomain(self.schema_name, self.name,
                               size=8*self.length)


class IntrospectPGSQLFloatDomain(IntrospectPGSQLDomain):

    named(('pg_catalog', 'float4'), ('pg_catalog', 'float8'))

    def __call__(self):
        return PGFloatDomain(self.schema_name, self.name,
                              size=8*self.length)


class IntrospectPGSQLDecimalDomain(IntrospectPGSQLDomain):

    named(('pg_catalog', 'numeric'))

    def __call__(self):
        precision = None
        scale = None
        if self.modifier != -1:
            precision = ((self.modifier-4) >> 0x10) & 0xFFFF
            scale = (self.modifier-4) & 0xFFFF
        return PGDecimalDomain(self.schema_name, self.name,
                               precision=precision, scale=scale)


class IntrospectPGSQLCharDomain(IntrospectPGSQLDomain):

    named(('pg_catalog', 'bpchar'))

    def __call__(self):
        length = self.modifier-4 if self.modifier != -1 else None
        return PGCharDomain(self.schema_name, self.name, length=length)


class IntrospectPGSQLVarCharDomain(IntrospectPGSQLDomain):

    named(('pg_catalog', 'varchar'))

    def __call__(self):
        length = self.modifier-4 if self.modifier != -1 else None
        return PGVarCharDomain(self.schema_name, self.name, length=length)


class IntrospectPGSQLTextDomain(IntrospectPGSQLDomain):

    named(('pg_catalog', 'text'))

    def __call__(self):
        return PGTextDomain(self.schema_name, self.name)


class IntrospectPGSQLDateDomain(IntrospectPGSQLDomain):

    named(('pg_catalog', 'date'))

    def __call__(self):
        return PGDateDomain(self.schema_name, self.name)


class IntrospectPGSQLTimeDomain(IntrospectPGSQLDomain):

    named(('pg_catalog', 'time'), ('pg_catalog', 'timetz'))

    def __call__(self):
        return PGTimeDomain(self.schema_name, self.name)


class IntrospectPGSQLDateTimeDomain(IntrospectPGSQLDomain):

    named(('pg_catalog', 'timestamp'), ('pg_catalog', 'timestamptz'))

    def __call__(self):
        return PGDateTimeDomain(self.schema_name, self.name)


