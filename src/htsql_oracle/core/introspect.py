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
import re
import itertools


class IntrospectOracle(Introspect):

    system_owner_names = ['SYS', 'SYSTEM', 'OUTLN', 'DIP', 'TSMSYS', 'DBSNMP',
                          'CTXSYS', 'XDB', 'ANONYMOUS', 'MDSYS', 'HR',
                          'FLOWS_FILES', 'FLOWS_020100']

    def __call__(self):
        connection = connect()
        cursor = connection.cursor()

        catalog = make_catalog()

        if self.system_owner_names:
            ignored_owners = ("(%s)"
                              % ", ".join("'%s'" % name
                                          for name in self.system_owner_names))
        else:
            ignored_owners = "('$')"

        cursor.execute("""
            SELECT username
            FROM all_users
            WHERE username NOT IN %(ignored_owners)s
            ORDER BY 1
        """ % vars())
        for row in cursor.fetchnamed():
            if '$' in row.username:
                continue
            catalog.add_schema(row.username)

        cursor.execute("""
            SELECT USER FROM DUAL
        """)
        current_user = cursor.fetchone()[0]
        if current_user in catalog.schemas:
            catalog.schemas[current_user].set_priority(1)

        cursor.execute("""
            SELECT owner, table_name
            FROM all_catalog
            WHERE owner NOT IN %(ignored_owners)s AND
                  table_type IN ('TABLE', 'VIEW')
            ORDER BY 1, 2
        """ % vars())
        for row in cursor.fetchnamed():
            if '$' in row.table_name:
                continue
            if row.owner not in catalog.schemas:
                continue
            schema = catalog.schemas[row.owner]
            schema.add_table(row.table_name)

        cursor.execute("""
            SELECT owner, constraint_name, table_name, search_condition
            FROM all_constraints
            WHERE owner NOT IN %(ignored_owners)s AND
                  constraint_type = 'C'
            ORDER BY 1, 3, 2
        """ % vars())
        checkrows_by_table = \
                dict((key, list(group))
                     for key, group in itertools.groupby(cursor.fetchnamed(),
                                            lambda r: (r.owner, r.table_name)))

        cursor.execute("""
            SELECT owner, table_name, column_id, column_name,
                   data_type, data_length, data_precision, data_scale,
                   nullable, data_default
            FROM all_tab_columns
            WHERE owner NOT IN %(ignored_owners)s
            ORDER BY 1, 2, 3
        """ % vars())
        for row in cursor.fetchnamed():
            if '$' in row.column_name:
                continue
            if row.owner not in catalog.schemas:
                continue
            schema = catalog.schemas[row.owner]
            if row.table_name not in schema.tables:
                continue
            table = schema.tables[row.table_name]
            name = row.column_name
            check = None
            check_key = (row.owner, row.table_name)
            if check_key in checkrows_by_table:
                for checkrow in checkrows_by_table[check_key]:
                    condition = checkrow.search_condition
                    if (condition.lower().startswith(name.lower()+' ') or
                        condition.lower().startswith('"'+name.lower()+'" ')):
                        check = condition
                        break
            domain = IntrospectOracleDomain.__invoke__(row.data_type,
                                                       row.data_length,
                                                       row.data_precision,
                                                       row.data_scale,
                                                       check)
            is_nullable = (row.nullable == 'Y')
            has_default = (row.data_default is not None)
            table.add_column(name, domain, is_nullable, has_default)

        cursor.execute("""
            SELECT owner, constraint_name, constraint_type,
                   table_name, r_owner, r_constraint_name
            FROM all_constraints
            WHERE owner NOT IN %(ignored_owners)s AND
                  constraint_type IN ('P', 'U', 'R') AND
                  status = 'ENABLED' AND validated = 'VALIDATED'
            ORDER BY 1, 2
        """ % vars())
        constraint_rows = cursor.fetchnamed()
        constraint_row_by_constraint = \
                dict(((row.owner, row.constraint_name), row)
                     for row in constraint_rows)
        cursor.execute("""
            SELECT owner, constraint_name, position, column_name
            FROM all_cons_columns
            WHERE owner NOT IN %(ignored_owners)s
            ORDER BY 1, 2, 3
        """ % vars())
        column_rows_by_constraint = \
                dict((key, list(group))
                     for key, group in itertools.groupby(cursor.fetchnamed(),
                         lambda r: (r.owner, r.constraint_name)))
        for row in constraint_rows:
            key = (row.owner, row.constraint_name)
            if key not in column_rows_by_constraint:
                continue
            column_rows = column_rows_by_constraint[key]
            if row.owner not in catalog.schemas:
                continue
            schema = catalog.schemas[row.owner]
            if row.table_name not in schema.tables:
                continue
            table = schema.tables[row.table_name]
            if not all(column_row.column_name in table.columns
                       for column_row in column_rows):
                continue
            columns = [table.columns[column_row.column_name]
                       for column_row in column_rows]
            if row.constraint_type in ('P', 'U'):
                is_primary = (row.constraint_type == 'P')
                table.add_unique_key(columns, is_primary)
            elif row.constraint_type == 'R':
                target_key = (row.r_owner, row.r_constraint_name)
                if target_key not in constraint_row_by_constraint:
                    continue
                if target_key not in column_rows_by_constraint:
                    continue
                target_row = constraint_row_by_constraint[target_key]
                target_column_rows = column_rows_by_constraint[target_key]
                if target_row.owner not in catalog.schemas:
                    continue
                target_schema = catalog.schemas[target_row.owner]
                if target_row.table_name not in target_schema.tables:
                    continue
                target_table = target_schema.tables[target_row.table_name]
                if not all(column_row.column_name in target_table.columns
                           for column_row in target_column_rows):
                    continue
                target_columns = [target_table.columns[column_row.column_name]
                                  for column_row in target_column_rows]
                table.add_foreign_key(columns, target_table, target_columns)

        connection.release()
        return catalog


class IntrospectOracleDomain(Protocol):

    @classmethod
    def __dispatch__(self, data_type, *args, **kwds):
        return data_type.encode('utf-8')

    def __init__(self, data_type, length, precision, scale, check):
        self.data_type = data_type
        self.length = length
        self.precision = precision
        self.scale = scale
        self.check = check

    def __call__(self):
        return OpaqueDomain()


class IntrospectOracleCharDomain(IntrospectOracleDomain):

    call('CHAR', 'NCHAR')

    def __call__(self):
        return StringDomain(length=self.length, is_varying=False)


class IntrospectOracleVarCharDomain(IntrospectOracleDomain):

    call('VARCHAR2', 'NVARCHAR2', 'CLOB', 'NCLOB', 'LONG')

    def __call__(self):
        return StringDomain(length=self.length, is_varying=True)


class IntrospectOracleNumberDomain(IntrospectOracleDomain):

    call('NUMBER')

    boolean_pattern = r"""
        ^ [\w"]+ \s+ IN \s+ \( (?: 0 \s* , \s* 1 | 1 \s* , \s* 0 ) \) $
    """
    boolean_regexp = re.compile(boolean_pattern, re.X|re.I)

    def __call__(self):
        if (self.precision, self.scale) == (1, 0):
            if (self.check is not None and
                    self.boolean_regexp.match(self.check)):
                return BooleanDomain()
        if (self.precision, self.scale) == (38, 0):
            return IntegerDomain()
        return DecimalDomain(precision=self.precision, scale=self.scale)


class IntrospectOracleFloatDomain(IntrospectOracleDomain):

    call('BINARY_FLOAT', 'BINARY_DOUBLE')

    def __call__(self):
        return FloatDomain()


class IntrospectOracleDateTimeDomain(IntrospectOracleDomain):

    call('DATE', 'TIMESTAMP')

    def __call__(self):
        return DateTimeDomain()


