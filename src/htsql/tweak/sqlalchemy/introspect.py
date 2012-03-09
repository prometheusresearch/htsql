#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ...core.context import context
from ...core.adapter import Adapter, rank, adapt, adapt_many
from ...core.introspect import Introspect
from ...core.entity import make_catalog
from ...core.domain import (BooleanDomain, IntegerDomain, FloatDomain,
                            DecimalDomain, StringDomain, DateDomain,
                            TimeDomain, DateTimeDomain, OpaqueDomain)
from sqlalchemy import types
from sqlalchemy.schema import (PrimaryKeyConstraint, ForeignKeyConstraint,
                               UniqueConstraint)


def decode(name, quote=None):
    if not name:
        name = ""
    if not quote and name == name.lower():
        if context.app.htsql.db.engine == 'oracle':
            name = name.upper()
    if isinstance(name, str):
        name = name.decode('utf-8')
    return name


class SQLAlchemyIntrospect(Introspect):

    rank(1.0)

    def __init__(self):
        super(SQLAlchemyIntrospect, self).__init__()

    def __call__(self):
        metadata = context.app.tweak.sqlalchemy.metadata
        if not metadata:
            return super(SQLAlchemyIntrospect, self).__call__()

        catalog = make_catalog()

        for table_record in metadata.sorted_tables:
            schema_name = decode(table_record.schema,
                                 table_record.quote_schema)
            if schema_name not in catalog.schemas:
                catalog.add_schema(schema_name)
            schema = catalog.schemas[schema_name]
            name = decode(table_record.name, table_record.quote)
            table = schema.add_table(name)

            for column_record in table_record.columns:
                name = decode(column_record.name, column_record.quote)
                domain = IntrospectSADomain.__invoke__(column_record.type)
                is_nullable = column_record.nullable
                has_default = (column_record.server_default is not None)
                table.add_column(name, domain, is_nullable, has_default)

        for table_record in metadata.sorted_tables:
            schema_name = decode(table_record.schema,
                                 table_record.quote_schema)
            schema = catalog.schemas[schema_name]
            name = decode(table_record.name, table_record.quote)
            table = schema.tables[name]

            for key_record in table_record.constraints:
                if isinstance(key_record, (PrimaryKeyConstraint,
                                           UniqueConstraint)):
                    names = [decode(column_record.name, column_record.quote)
                             for column_record in key_record.columns]
                    if not all(name in table.columns for name in names):
                        continue
                    columns = [table.columns[name] for name in names]
                    is_primary = isinstance(key_record, PrimaryKeyConstraint)
                    table.add_unique_key(columns, is_primary)
                elif isinstance(key_record, ForeignKeyConstraint):
                    column_records = [table_record.columns[column_record]
                                      if isinstance(column_record, basestring)
                                      else column_record
                                      for column_record in key_record.columns]
                    names = [decode(column_record.name, column_record.quote)
                             for column_record in column_records]
                    if not all(name in table.columns for name in names):
                        continue
                    columns = [table.columns[name] for name in names]
                    target_records = [element.column
                                      for element in key_record.elements]
                    target_table_record = target_records[0].table
                    target_schema_name = decode(target_table_record.schema,
                                            target_table_record.quote_schema)
                    if target_schema_name not in catalog.schemas:
                        continue
                    target_schema = catalog.schemas[target_schema_name]
                    target_table_name = decode(target_table_record.name,
                                               target_table_record.quote)
                    if target_table_name not in target_schema.tables:
                        continue
                    target_table = target_schema.tables[target_table_name]
                    target_names = [decode(target_record.name,
                                           target_record.quote)
                                    for target_record in target_records]
                    if not all(name in target_table.columns
                               for name in target_names):
                        continue
                    target_columns = [target_table.columns[name]
                                      for name in target_names]
                    table.add_foreign_key(columns, target_table, target_columns)

        return catalog


class IntrospectSADomain(Adapter):

    adapt(types.TypeEngine)

    def __init__(self, type):
        self.type = type

    def __call__(self):
        return OpaqueDomain()


class IntrospectSABooleanDomain(IntrospectSADomain):

    adapt(types.Boolean)

    def __call__(self):
        return BooleanDomain()


class IntrospectSAIntegerDomain(IntrospectSADomain):

    adapt(types.Integer)

    def __call__(self):
        return IntegerDomain()


class IntrospectSAStringDomain(IntrospectSADomain):

    adapt(types.String)

    def __call__(self):
        return StringDomain(self.type.length, True)


class IntrospectSACharDomain(IntrospectSADomain):

    adapt_many(types.CHAR, types.NCHAR)

    def __call__(self):
        return StringDomain(self.type.length, False)


class IntrospectSAFloatDomain(IntrospectSADomain):

    adapt(types.Float)

    def __call__(self):
        return FloatDomain()


class IntrospectSADecimalDomain(IntrospectSADomain):

    adapt(types.Numeric)

    def __call__(self):
        return DecimalDomain(self.type.precision, self.type.scale)


class IntrospectSADateDomain(IntrospectSADomain):

    adapt(types.Date)

    def __call__(self):
        return DateDomain()


class IntrospectSATimeDomain(IntrospectSADomain):

    adapt(types.Time)

    def __call__(self):
        return TimeDomain()


class IntrospectSADateTimeDomain(IntrospectSADomain):

    adapt(types.DateTime)

    def __call__(self):
        return DateTimeDomain()


