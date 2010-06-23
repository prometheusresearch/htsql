#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.entity`
===================

This module implements the HTSQL catalog and catalog entities.
"""


from .util import listof
from .domain import Domain


class Entity(object):

    def __str__(self):
        return self.__class__.__name__.lower()

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self)


class NamedEntity(Entity):

    def __init__(self, name):
        self.name = name

    def __str__(self):
        return self.name

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self)


class EntitySet(object):

    def __init__(self, entities):
        assert isinstance(entities, listof(NamedEntity))
        self.entities = entities
        self.entity_by_name = dict((entity.name, entity)
                                   for entity in entities)

    def __contains__(self, name):
        return (name in self.entity_by_name)

    def __getitem__(self, name):
        return self.entity_by_name[name]

    def __iter__(self):
        return iter(self.entities)

    def __len__(self):
        return len(self.entities)

    def get(self, name, default=None):
        return self.entity_by_name.get(name, default)

    def keys(self):
        return [entity.name for entity in self.entities]

    def values(self):
        return self.entities[:]

    def items(self):
        return [(entity.name, entity) for entity in self.entities]


class CatalogEntity(Entity):

    def __init__(self, schemas):
        assert isinstance(schemas, listof(SchemaEntity))
        self.schemas = EntitySet(schemas)


class SchemaEntity(NamedEntity):

    def __init__(self, name, tables):
        assert isinstance(tables, listof(TableEntity))
        assert all(table.schema_name == name for table in tables)
        super(SchemaEntity, self).__init__(name)
        self.tables = EntitySet(tables)


class TableEntity(NamedEntity):

    is_view = False

    def __init__(self, schema_name, name, columns, unique_keys, foreign_keys):
        assert isinstance(columns, listof(ColumnEntity))
        assert all((column.schema_name, column.table_name)
                        == (schema_name, name) for column in columns)
        assert isinstance(unique_keys, listof(UniqueKeyEntity))
        assert all((uk.origin_schema_name, uk.origin_name)
                        == (schema_name, name) for uk in unique_keys)
        assert isinstance(foreign_keys, listof(ForeignKeyEntity))
        assert all((fk.origin_schema_name, fk.origin_name)
                        == (schema_name, name) for fk in foreign_keys)
        super(TableEntity, self).__init__(name)
        self.schema_name = schema_name
        self.columns = EntitySet(columns)
        self.unique_keys = unique_keys
        self.primary_key = None
        primary_keys = [uk for uk in unique_keys if uk.is_primary]
        assert len(primary_keys) <= 1
        if primary_keys:
            self.primary_key = primary_keys[0]
        self.foreign_keys = foreign_keys

    def __str__(self):
        return "%s.%s" % (self.schema_name, self.name)


class ViewEntity(TableEntity):

    is_view = True


class ColumnEntity(NamedEntity):

    def __init__(self, schema_name, table_name, name,
                 domain, is_nullable=False, has_default=False):
        assert isinstance(domain, Domain)
        assert isinstance(is_nullable, bool)
        assert isinstance(has_default, bool)
        super(ColumnEntity, self).__init__(name)
        self.schema_name = schema_name
        self.table_name = table_name
        self.domain = domain
        self.is_nullable = is_nullable
        self.has_default = has_default

    def __str__(self):
        return "%s.%s.%s" % (self.schema_name, self.table_name, self.name)


class UniqueKeyEntity(Entity):

    is_primary = False

    def __init__(self, origin_schema_name, origin_name, origin_column_names):
        assert isinstance(origin_schema_name, str)
        assert isinstance(origin_name, str)
        assert isinstance(origin_column_names, listof(str))

        self.origin_schema_name = origin_schema_name
        self.origin_name = origin_name
        self.origin_column_names = origin_column_names

    def __str__(self):
        return "%s.%s(%s)" % (self.origin_schema_name, self.origin_name,
                              ",".join(self.origin_column_names))


class PrimaryKeyEntity(UniqueKeyEntity):

    is_primary = True


class ForeignKeyEntity(Entity):

    def __init__(self, origin_schema_name, origin_name, origin_column_names,
                 target_schema_name, target_name, target_column_names):
        assert isinstance(origin_schema_name, str)
        assert isinstance(origin_name, str)
        assert isinstance(origin_column_names, listof(str))
        assert isinstance(target_schema_name, str)
        assert isinstance(target_name, str)
        assert isinstance(target_column_names, listof(str))

        self.origin_schema_name = origin_schema_name
        self.origin_name = origin_name
        self.origin_column_names = origin_column_names
        self.target_schema_name = target_schema_name
        self.target_name = target_name
        self.target_column_names = target_column_names

    def __str__(self):
        return "%s.%s(%s) -> %s.%s(%s)" \
                % (self.origin_schema_name, self.origin_name,
                   ",".join(self.origin_column_names),
                   self.target_schema_name, self.target_name,
                   ",".join(self.target_column_names))


class Join(object):

    is_direct = False
    is_reverse = False

    def __init__(self, origin, target):
        assert isinstance(origin, TableEntity)
        assert isinstance(target, TableEntity)
        self.origin = origin
        self.target = target


class DirectJoin(Join):

    is_direct = True

    def __init__(self, origin, target, foreign_key):
        assert isinstance(foreign_key, ForeignKeyEntity)
        super(DirectJoin, self).__init__(origin, target)
        self.foreign_key = foreign_key
        self.origin_columns = [origin.columns[name]
                               for name in foreign_key.origin_column_names]
        self.target_columns = [target.columns[name]
                               for name in foreign_key.target_column_names]
        self.is_expanding = all(not column.is_nullable
                                for column in self.origin_columns)
        self.is_contracting = True


class ReverseJoin(Join):

    is_reverse = True

    def __init__(self, origin, target, foreign_key):
        assert isinstance(foreign_key, ForeignKeyEntity)
        super(ReverseJoin, self).__init__(origin, target)
        self.foreign_key = foreign_key
        self.origin_columns = [target.columns[name]
                               for name in foreign_key.target_column_names]
        self.target_columns = [origin.columns[name]
                               for name in foreign_key.origin_column_names]
        self.is_expanding = False
        self.is_contracting = False
        for uk in target.unique_keys:
            if all(column.name in uk.origin_column_names
                   for column in target.columns):
                self.is_contracting = True


#
# TODO: bound entities...
#


class cached_property(object):

    def __init__(self, getter):
        self.getter = getter

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self
        value = self.getter(obj)
        obj.__dict__[self.getter.__name__] = value
        return value


class BoundCatalog(object):

    def __init__(self, catalog):
        self.catalog = catalog

    @cached_property
    def schemas(self):
        return BoundEntitySet(self, self.catalog.schemas)


class BoundSchema(object):

    def __init__(self, catalog, schema):
        self.catalog = catalog
        self.schema = schema


class BoundTable(object):

    def __init__(self, schema, table):
        self.schema = schema
        self.table = table
        self.columns = BoundEntitySet(self, self.table.columns)


