#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from htsql.core.adapter import Protocol, call
from htsql.core.introspect import Introspect
from htsql.core.entity import make_catalog
from htsql.core.domain import (BooleanDomain, IntegerDomain,
                               FloatDomain, StringDomain, DateDomain,
                               TimeDomain, DateTimeDomain, OpaqueDomain)
from htsql.core.connect import connect


class IntrospectSQLite(Introspect):

    @staticmethod
    def escape_name(name):
        return '"%s"' % name.encode('utf-8').replace('"', '""')

    def __call__(self):
        connection = connect()
        cursor = connection.cursor()

        catalog = make_catalog()

        schema = catalog.add_schema(u'')

        cursor.execute("""
            SELECT *
            FROM sqlite_master
            WHERE type = 'table'
            ORDER BY name
        """)
        for row in cursor.fetchnamed():
            schema.add_table(row.name)

        for table in schema.tables:
            cursor.execute("""PRAGMA table_info(%s)"""
                           % self.escape_name(table.name))
            primary_key_columns = []
            for row in cursor.fetchnamed():
                name = row.name
                domain = IntrospectSQLiteDomain.__invoke__(row.type)
                is_nullable = (not row.notnull)
                has_default = (row.dflt_value is not None)
                column = table.add_column(name, domain,
                                          is_nullable, has_default)
                if row.pk:
                    primary_key_columns.append(column)
            if primary_key_columns:
                # SQLite does not enforce NOT NULL on PRIMARY KEY columns.
                if any(column.is_nullable for column in primary_key_columns):
                    table.add_unique_key(primary_key_columns)
                else:
                    table.add_primary_key(primary_key_columns)

        for table in schema.tables:
            cursor.execute("""PRAGMA index_list(%s)"""
                           % self.escape_name(table.name))
            for index_row in cursor.fetchnamed():
                if not index_row.unique:
                    continue
                cursor.execute("""PRAGMA index_info(%s)"""
                               % self.escape_name(index_row.name))
                columns = []
                for row in cursor.fetchnamed():
                    columns.append(table.columns[row.name])
                table.add_unique_key(columns)

        for table in schema.tables:
            ids = set()
            columns_by_id = {}
            target_by_id = {}
            target_columns_by_id = {}
            cursor.execute("""PRAGMA foreign_key_list(%s)"""
                           % self.escape_name(table.name))
            for row in cursor.fetchnamed():
                if row.id not in ids:
                    ids.add(row.id)
                    columns_by_id[row.id] = []
                    target_name = row.table
                    # Workaround against extra quoting in
                    # PRAGMA foreign_key_list; column `table`.
                    # See `http://www.sqlite.org/cvstrac/tktview?tn=3800`
                    # and `http://www.sqlite.org/src/ci/600482d161`.
                    # The bug is fixed in SQLite 3.6.14.
                    if (target_name.startswith(u'"') and
                            target_name.endswith(u'"')):
                        target_name = target_name[1:-1].replace(u'""', u'"')
                    target_by_id[row.id] = schema.tables[target_name]
                    target_columns_by_id[row.id] = []
                target = target_by_id[row.id]
                column = table.columns[row.from_]
                target_column = target.columns[row.to]
                columns_by_id[row.id].append(column)
                target_columns_by_id[row.id].append(target_column)
            for id in sorted(ids):
                columns = columns_by_id[id]
                target = target_by_id[id]
                target_columns = target_columns_by_id[id]
                table.add_foreign_key(columns, target, target_columns)

        connection.release()
        return catalog


class IntrospectSQLiteDomain(Protocol):

    @classmethod
    def __dispatch__(interface, name, *args, **kwds):
        return name.lower().encode('utf-8')

    @classmethod
    def __matches__(component, dispatch_key):
        assert isinstance(dispatch_key, str)
        return any(name in dispatch_key for name in component.__names__)

    def __init__(self, name):
        self.name = name

    def __call__(self):
        return OpaqueDomain()


class IntrospectSQLiteIntegerDomain(IntrospectSQLiteDomain):

    call('int')

    def __call__(self):
        return IntegerDomain()


class IntrospectSQLiteStringDomain(IntrospectSQLiteDomain):

    call('char', 'clob', 'text')

    def __call__(self):
        return StringDomain()


class IntrospectSQLiteFloatDomain(IntrospectSQLiteDomain):

    call('real', 'floa', 'doub')

    def __call__(self):
        return FloatDomain()


class IntrospectSQLiteBooleanDomain(IntrospectSQLiteDomain):

    call('bool')

    def __call__(self):
        return BooleanDomain()


class IntrospectSQLiteDateTimeDomain(IntrospectSQLiteDomain):

    call('date', 'time')

    def __call__(self):
        key = self.name.encode('utf-8').lower()
        if 'datetime' in key or 'timestamp' in key:
            return DateTimeDomain()
        if 'date' in key:
            return DateDomain()
        if 'time' in key:
            return TimeDomain()


