#
# Copyright (c) 2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#
"""
:mod:`htsql_tweak.sqlalchemy.introspect`
========================================

This module implements the introspection adapter for SQLAlchemy MetaData.
"""

from htsql.context import context
from htsql.adapter import Adapter, weigh, adapts, adapts_many
from htsql.introspect import Introspect
from htsql.entity import make_catalog
from htsql.domain import (BooleanDomain, IntegerDomain, FloatDomain,
                          DecimalDomain, StringDomain, DateDomain, TimeDomain,
                          DateTimeDomain, OpaqueDomain)
from sqlalchemy import types
from sqlalchemy.schema import (PrimaryKeyConstraint, ForeignKeyConstraint,
                               UniqueConstraint)


class SQLAlchemyIntrospect(Introspect):
    """ override normal introspection with SQLAlchemy's MetaData """

    weigh(1.0)

    def __init__(self):
        super(SQLAlchemyIntrospect, self).__init__()

    def __call__(self):
        metadata = context.app.tweak.sqlalchemy.metadata
        if not metadata:
            return super(SQLAlchemyIntrospect, self).__call__()

        catalog = make_catalog()

        for table_record in metadata.sorted_tables:
            schema_name = table_record.schema or ''
            if schema_name not in catalog.schemas:
                catalog.add_schema(schema_name)
            schema = catalog.schemas[schema_name]
            table = schema.add_table(table_record.name)

            for column_record in table_record.columns:
                introspect_domain = IntrospectSADomain(column_record.type)
                domain = introspect_domain()
                is_nullable = column_record.nullable
                has_default = (column_record.server_default is not None)
                table.add_column(column_record.name, domain,
                                 is_nullable, has_default)

        for table_record in metadata.sorted_tables:
            schema_name = table_record.schema or ''
            schema = catalog.schemas[schema_name]
            table = schema.tables[table_record.name]

            for key_record in table_record.constraints:
                if isinstance(key_record, (PrimaryKeyConstraint,
                                           UniqueConstraint)):
                    names = [column_record.name
                             if not isinstance(column_record, str)
                             else column_record
                             for column_record in key_record.columns]
                    if not all(name in table.columns for name in names):
                        continue
                    columns = [table.columns[name] for name in names]
                    is_primary = isinstance(key_record, PrimaryKeyConstraint)
                    table.add_unique_key(columns, is_primary)
                elif isinstance(key_record, ForeignKeyConstraint):
                    names = [column_record.name
                             if not isinstance(column_record, str)
                             else column_record
                             for column_record in key_record.columns]
                    if not all(name in table.columns for name in names):
                        continue
                    columns = [table.columns[name] for name in names]
                    target_records = [element.column
                                      for element in key_record.elements]
                    target_table_record = target_records[0].table
                    target_schema_name = target_table_record.schema or ''
                    if target_schema_name not in catalog.schemas:
                        continue
                    target_schema = catalog.schemas[target_schema_name]
                    if target_table_record.name not in target_schema.tables:
                        continue
                    target_table = \
                            target_schema.tables[target_table_record.name]
                    target_names = [target_record.name
                                    for target_record in target_records]
                    if not all(name in target_table.columns
                               for name in target_names):
                        continue
                    target_columns = [target_table.columns[name]
                                      for name in target_names]
                    table.add_foreign_key(columns, target_table, target_columns)

        return catalog


class IntrospectSADomain(Adapter):

    adapts(types.TypeEngine)

    def __init__(self, type):
        self.type = type

    def __call__(self):
        return OpaqueDomain()


class IntrospectSABooleanDomain(IntrospectSADomain):

    adapts(types.Boolean)

    def __call__(self):
        return BooleanDomain()


class IntrospectSAIntegerDomain(IntrospectSADomain):

    adapts(types.Integer)

    def __call__(self):
        return IntegerDomain()


class IntrospectSAStringDomain(IntrospectSADomain):

    adapts(types.String)

    def __call__(self):
        return StringDomain(self.type.length, True)


class IntrospectSACharDomain(IntrospectSADomain):

    adapts_many(types.CHAR, types.NCHAR)

    def __call__(self):
        return StringDomain(self.type.length, False)


class IntrospectSAFloatDomain(IntrospectSADomain):

    adapts(types.Float)

    def __call__(self):
        return FloatDomain()


class IntrospectSADecimalDomain(IntrospectSADomain):

    adapts(types.Numeric)

    def __call__(self):
        return DecimalDomain(self.type.precision, self.type.scale)


class IntrospectSADateDomain(IntrospectSADomain):

    adapts(types.Date)

    def __call__(self):
        return DateDomain()


class IntrospectSATimeDomain(IntrospectSADomain):

    adapts(types.Time)

    def __call__(self):
        return TimeDomain()


class IntrospectSADateTimeDomain(IntrospectSADomain):

    adapts(types.DateTime)

    def __call__(self):
        return DateTimeDomain()


