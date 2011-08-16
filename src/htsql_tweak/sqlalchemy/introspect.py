#
# Copyright (c) 2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#
"""
:mod:`htsql_tweak.sqlalchemy.introspect`
========================================

This module implements the introspection adapter for SQLAlchemy MetaData.
"""

from htsql.introspect import Introspect
from htsql.entity import (CatalogEntity, SchemaEntity, TableEntity,
                          ColumnEntity, UniqueKeyEntity, PrimaryKeyEntity,
                          ForeignKeyEntity)
from htsql.domain import (BooleanDomain, IntegerDomain, TimeDomain,
                          FloatDomain, StringDomain, DateDomain,
                          DateTimeDomain, OpaqueDomain)
from htsql.connect import Connect
from htsql.util import Record

from htsql.context import context
from htsql.adapter import weigh

from sqlalchemy import types
from sqlalchemy.schema import (PrimaryKeyConstraint, ForeignKeyConstraint,
                               CheckConstraint, UniqueConstraint) 

class SQLAlchemyIntrospect(Introspect):
    """ override normal introspection with SQLAlchemy's MetaData """

    weigh(1.0)

    def __init__(self):
        super(SQLAlchemyIntrospect, self).__init__()
 
    def __call__(self):
        metadata = context.app.tweak.sqlalchemy.metadata
        if metadata:
            return self.convert_catalog(metadata)
        return super(SQLAlchemyIntrospect, self).__call__()
 
    def permit_schema(self, schema_name):
        return True

    def permit_table(self, schema_name, table_name):
        return True

    def permit_column(self, schema_name, table_name, column_name):
        return True
    
    def permit_columns(self, schema_name, table_name, column_names):
        return all(self.permit_column(schema_name, table_name, column_name)
                           for column_name in column_names)

    def convert_domain(self, sqa_type):
        for (test, ifso) in ((types.Boolean, BooleanDomain),
                             (types.Integer, IntegerDomain),
                             (types.DateTime, DateTimeDomain),
                             (types.Date, DateDomain),
                             (types.Time, TimeDomain)):
            if isinstance(sqa_type, test):
                return ifso()
        if isinstance(sqa_type, types.String):
           is_varying = True
           if isinstance(sqa_type, (types.CHAR, types.NCHAR)):
               is_varying = False
           if isinstance(sqa_type, types.TEXT):
               assert sqa_type.length is None
           return StringDomain(sqa_type.length, is_varying)
        if isinstance(sqa_type, types.Float):
           return FloatDomain(sqa_type.precision)
        if isinstance(sqa_type, types.Numeric):
           return DecimalDomain(sqa_type.precision, sqa_type.scale)
        return OpaqueDomain()

    def convert_columns(self, schema, table):
        columns = []
        for column in table.columns:
            if not self.permit_column(schema, table.name, column.name) and \
               not column.primary_key:
                continue
            domain = self.convert_domain(column.type)
            has_default = column.server_default is not None
            columns.append(ColumnEntity(schema, table.name, 
                                        column.name, domain,
                                        column.nullable, has_default))
        return columns

    def convert_unique_keys(self, schema, table):
        unique_keys = []
        for cons in table.constraints:
            if isinstance(cons, PrimaryKeyConstraint):
                column_names = []
                for column in cons.columns:
                    column_names.append(column.name)
                unique_keys.append(PrimaryKeyEntity(schema, table.name, 
                                                    column_names))
                continue
            if isinstance(cons, UniqueConstraint):
                column_names = []
                for column in cons.columns:
                    column_names.append(column.name)
                if not self.permit_columns(schema, table.name, column_names):
                    continue
                unique_keys.append(UniqueKeyEntity(schema, table.name, 
                                                   column_names))
                continue
        return unique_keys
            
    def convert_foreign_keys(self, schema, table):
        foreign_keys = []
        for cons in table.constraints:
            if not isinstance(cons, ForeignKeyConstraint):
                continue
            source_names = []
            for column in cons.columns:
                if type(column) == str:
                    source_names.append(column)
                else:
                    source_names.append(column.name)
            target_columns = [e.column for e in cons.elements]
            target_table = target_columns[0].table
            target_schema = target_table.schema or '_'
            target_names = [column.name for column in target_columns]
            if not self.permit_schema(target_schema):
                continue
            if not self.permit_table(target_schema, target_table.name):
                continue
            if not self.permit_columns(schema, table.name, source_names):
                continue
            if not self.permit_columns(target_schema, target_table.name, 
                                       target_names):
                continue
            foreign_key = ForeignKeyEntity(schema, table.name, source_names,
                             target_schema, target_table.name, target_names)
            foreign_keys.append(foreign_key)
        return foreign_keys

    def convert_tables(self, metadata):
        tables = []
        for table in metadata.sorted_tables:
            schema_name = table.schema or '_'
            if not self.permit_schema(schema_name):
                continue
            if not self.permit_table(schema_name, table.name):
                continue
            columns = self.convert_columns(schema_name, table)
            unique_keys = self.convert_unique_keys(schema_name, table)
            foreign_keys = self.convert_foreign_keys(schema_name, table)
            table = TableEntity(schema_name, table.name, columns, 
                                unique_keys, foreign_keys)
            tables.append(table)
        return tables

    def convert_catalog(self, metadata):
        buckets = {}
        tables = self.convert_tables(metadata)
        for table in tables:
            bucket = buckets.setdefault(table.schema_name, [])
            bucket.append(table)
        schemas = []
        for (schema_name, tables) in buckets.items():
             schemas.append(SchemaEntity(schema_name, tables))
        return CatalogEntity(schemas)
            
