#
# Copyright (c) 2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.context import context
from htsql.adapter import Protocol, weigh, named
from htsql.introspect import Introspect
from htsql.entity import make_catalog
from htsql.domain import (BooleanDomain, IntegerDomain, FloatDomain,
                          DecimalDomain, StringDomain, DateDomain, TimeDomain,
                          DateTimeDomain, OpaqueDomain)


class DjangoIntrospect(Introspect):

    weigh(1.0)

    def __call__(self):
        from django.db import connections, models
        from django.db.utils import DEFAULT_DB_ALIAS
        from django.conf import settings
        is_upper = (settings.DATABASES[DEFAULT_DB_ALIAS]['ENGINE'] in
                                ['django.db.backends.oracle'])
        connection = connections[DEFAULT_DB_ALIAS]
        catalog = make_catalog()
        tables = connection.introspection.table_names()
        seen_models = connection.introspection.installed_models(tables)
        all_models = []
        for app in models.get_apps():
            for model in models.get_models(app, include_auto_created=True):
                if model not in seen_models:
                    continue
                all_models.append(model)
        schema = catalog.add_schema(u"")
        relations = []
        table_by_model = {}
        column_by_field = {}
        for model in all_models:
            meta = model._meta
            name = meta.db_table
            if is_upper:
                name = name.upper()
            if isinstance(name, str):
                name = name.decode('utf-8')
            table = schema.add_table(name)
            table_by_model[model] = table
            for field in meta.local_fields:
                introspect_domain = IntrospectDjangoDomain(field)
                domain = introspect_domain()
                if domain is None:
                    continue
                name = field.column
                if is_upper:
                    name = name.upper()
                if isinstance(name, str):
                    name = name.decode('utf-8')
                is_nullable = bool(field.null)
                column = table.add_column(name, domain, is_nullable)
                column_by_field[field] = column
                if field.primary_key:
                    table.add_primary_key([column])
                if field.unique:
                    table.add_unique_key([column])
                if field.rel is not None:
                    relations.append(field)
        for field in relations:
            table = table_by_model[field.model]
            column = column_by_field[field]
            target_field = field.rel.get_related_field()
            if target_field not in column_by_field:
                continue
            target_table = table_by_model[target_field.model]
            target_column = column_by_field[target_field]
            table.add_foreign_key([column], target_table, [target_column])
        return catalog


class IntrospectDjangoDomain(Protocol):

    @classmethod
    def dispatch(cls, field):
        return field.__class__.__name__

    def __init__(self, field):
        self.field = field

    def __call__(self):
        return OpaqueDomain()


class IntrospectDjangoBooleanDomain(IntrospectDjangoDomain):

    named('BooleanField', 'NullBooleanField')

    def __call__(self):
        return BooleanDomain()


class IntrospectDjangoIntegerDomain(IntrospectDjangoDomain):

    named('AutoField', 'IntegerField', 'ForeignKey')

    def __call__(self):
        return IntegerDomain()


class IntrospectDjangoFloatDomain(IntrospectDjangoDomain):

    named('FloatField')

    def __call__(self):
        return FloatDomain()


class IntrospectDjangoDecimalDomain(IntrospectDjangoDomain):

    named('DecimalField')

    def __call__(self):
        return DecimalDomain()


class IntrospectDjangoStringDomain(IntrospectDjangoDomain):

    named('CharField', 'FilePathField', 'TextField')

    def __call__(self):
        return StringDomain()


class IntrospectDjangoDateDomain(IntrospectDjangoDomain):

    named('DateField')

    def __call__(self):
        return DateDomain()


class IntrospectDjangoTimeDomain(IntrospectDjangoDomain):

    named('TimeField')

    def __call__(self):
        return TimeDomain()


class IntrospectDjangoDateTimeDomain(IntrospectDjangoDomain):

    named('DateTimeField')

    def __call__(self):
        return DateTimeDomain()


class IntrospectDjangoNotDomain(IntrospectDjangoDomain):

    named('ManyToManyField')

    def __call__(self):
        return None


