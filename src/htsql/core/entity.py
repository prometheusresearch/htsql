#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql.core.entity`
========================

This module implements the HTSQL catalog and catalog entities.
"""


from .util import listof, Printable, Comparable
from .domain import Domain
import weakref


class Entity(Printable):

    is_frozen = True

    def __init__(self, owner):
        assert not self.is_frozen
        assert isinstance(owner, weakref.ref) and isinstance(owner(), Entity)
        self.owner = owner

    def freeze(self):
        pass

    def __unicode__(self):
        return u"[%s]" % id(self)

    def __str__(self):
        return unicode(self).encode('utf-8')


class MutableEntity(Entity):

    is_frozen = False

    def remove(self):
        self.__dict__.clear()
        self.__class__ = RemovedEntity


class NamedEntity(Entity):

    def __init__(self, owner, name):
        assert isinstance(name, unicode)
        super(NamedEntity, self).__init__(owner)
        self.name = name

    def __unicode__(self):
        if self.name:
            return self.name
        else:
            return u"<default>"


class RemovedEntity(Printable):

    def __unicode__(self):
        return u"<removed>"

    def __str__(self):
        return "<removed>"


class EntitySet(Printable):

    is_frozen = True

    def __init__(self):
        assert not self.is_frozen
        self.entities = []
        self.index_by_name = {}

    def __contains__(self, name):
        return (name in self.index_by_name)

    def __getitem__(self, name):
        return self.entities[self.index_by_name[name]]

    def __iter__(self):
        return iter(self.entities)

    def __len__(self):
        return len(self.entities)

    def get(self, name, default=None):
        index = self.index_by_name
        if index is not None:
            return self.entities[index]
        return default

    def keys(self):
        return [entity.name for entity in self.entities]

    def values(self):
        return self.entities[:]

    def items(self):
        return [(entity.name, entity) for entity in self.entities]

    def __unicode__(self):
        return u"[%s]" % u", ".join(entity.name for entity in self.entities)

    def __str__(self):
        return unicode(self).encode('utf-8')


class MutableEntitySet(EntitySet):

    is_frozen = False

    def add(self, entity):
        assert isinstance(entity, NamedEntity)
        if entity.name in self.index_by_name:
            raise KeyError(entity.name)
        self.index_by_name[entity.name] = len(self.entities)
        self.entities.append(entity)

    def remove(self, entity):
        assert isinstance(entity, NamedEntity)
        assert entity.name in self.index_by_name
        idx = self.index_by_name[entity.name]
        assert self.entities[idx] is entity
        del self.entities[idx]
        del self.index_by_name[entity.name]
        for entity in self.entities[idx:]:
            self.index_by_name[entity.name] -= 1

    def freeze(self):
        for entity in self.entities:
            entity.freeze()
        self.__class__ = EntitySet


class CatalogEntity(Entity):
    pass


class MutableCatalogEntity(CatalogEntity, MutableEntity):

    def __init__(self):
        super(MutableCatalogEntity, self).__init__(weakref.ref(self))
        self.schemas = MutableEntitySet()

    def add_schema(self, name, priority=0):
        return MutableSchemaEntity(self, name, priority)

    def freeze(self):
        self.schemas.freeze()
        self.__class__ = CatalogEntity

    def remove(self):
        for schema in reversed(self.schemas):
            schema.remove()
        self.__dict__.clear()
        self.__class__ = RemovedEntity


class SchemaEntity(NamedEntity):

    @property
    def catalog(self):
        return self.owner()


class MutableSchemaEntity(SchemaEntity, MutableEntity):

    def __init__(self, catalog, name, priority):
        assert isinstance(catalog, MutableCatalogEntity)
        assert name not in catalog.schemas
        assert isinstance(priority, int)
        super(MutableSchemaEntity, self).__init__(weakref.ref(catalog), name)
        self.tables = MutableEntitySet()
        self.priority = priority
        catalog.schemas.add(self)

    def set_priority(self, priority):
        assert isinstance(priority, int)
        self.priority = priority
        return self

    def add_table(self, name):
        return MutableTableEntity(self, name)

    def freeze(self):
        self.tables.freeze()
        self.__class__ = SchemaEntity

    def remove(self):
        for table in reversed(list(self.tables)):
            table.remove()
        self.catalog.schemas.remove(self)
        self.__dict__.clear()
        self.__class__ = RemovedEntity


class TableEntity(NamedEntity):

    @property
    def schema(self):
        return self.owner()

    def __unicode__(self):
        if not self.schema.name:
            return self.name
        return u"%s.%s" % (self.schema, self.name)


class MutableTableEntity(TableEntity, MutableEntity):

    def __init__(self, schema, name):
        assert isinstance(schema, MutableSchemaEntity)
        assert name not in schema.tables
        assert len(name) > 0
        super(MutableTableEntity, self).__init__(weakref.ref(schema), name)
        self.columns = MutableEntitySet()
        self.primary_key = None
        self.unique_keys = []
        self.foreign_keys = []
        self.referring_foreign_keys = []
        schema.tables.add(self)

    def add_column(self, name, domain, is_nullable=True, has_default=False):
        return MutableColumnEntity(self, name, domain,
                                   is_nullable, has_default)

    def add_unique_key(self, columns, is_primary=False, is_partial=False):
        return MutableUniqueKeyEntity(self, columns, is_primary, is_partial)

    def add_primary_key(self, columns):
        return MutableUniqueKeyEntity(self, columns, True, False)

    def add_foreign_key(self, columns, target, target_columns,
                        is_partial=False):
        return MutableForeignKeyEntity(self, columns, target, target_columns,
                                       is_partial)

    def freeze(self):
        self.columns.freeze()
        for unique_key in self.unique_keys:
            unique_key.freeze()
        for foreign_key in self.foreign_keys:
            foreign_key.freeze()
        self.__class__ = TableEntity

    def remove(self):
        for unique_key in list(self.unique_keys):
            unique_key.remove()
        for foreign_key in list(self.foreign_keys):
            foreign_key.remove()
        for foreign_key in list(self.referring_foreign_keys):
            foreign_key.remove()
        for column in reversed(list(self.columns)):
            column.remove()
        self.schema.tables.remove(self)
        self.__dict__.clear()
        self.__class__ = RemovedEntity


class ColumnEntity(NamedEntity, MutableEntity):

    @property
    def table(self):
        return self.owner()

    @property
    def unique_keys(self):
        return [unique_key
                for unique_key in self.table.unique_keys
                if self in unique_key.origin_columns]

    @property
    def foreign_keys(self):
        return [foreign_key
                for foreign_key in self.table.foreign_keys
                if self in foreign_key.origin_columns]

    @property
    def referring_foreign_keys(self):
        return [foreign_key
                for foreign_key in self.table.referring_foreign_keys
                if self in foreign_key.target_columns]

    def __unicode__(self):
        return u"%s.%s" % (self.table, self.name)


class MutableColumnEntity(ColumnEntity, MutableEntity):

    def __init__(self, table, name, domain, is_nullable, has_default):
        assert isinstance(table, MutableTableEntity)
        assert name not in table.columns
        assert len(name) > 0
        assert isinstance(domain, Domain)
        assert isinstance(is_nullable, bool)
        assert isinstance(has_default, bool)
        super(MutableColumnEntity, self).__init__(weakref.ref(table), name)
        self.domain = domain
        self.is_nullable = is_nullable
        self.has_default = has_default
        table.columns.add(self)

    def set_domain(self, domain):
        assert isinstance(domain, Domain)
        self.domain = domain
        return self

    def set_is_nullable(self, is_nullable):
        assert isinstance(is_nullable, bool)
        if is_nullable and self.table.primary_key is not None:
            assert self not in self.table.primary_key.origin_columns
        self.is_nullable = is_nullable
        return self

    def set_has_default(self, has_default):
        assert isinstance(has_default, bool)
        self.has_default = has_default
        return self

    def freeze(self):
        self.__class__ = ColumnEntity

    def remove(self):
        for unique_key in self.unique_keys:
            unique_key.remove()
        for foreign_key in self.foreign_keys:
            foreign_key.remove()
        for foreign_key in self.referring_foreign_keys:
            foreign_key.remove()
        self.table.columns.remove(self)
        self.__dict__.clear()
        self.__class__ = RemovedEntity


class UniqueKeyEntity(Entity):

    @property
    def origin(self):
        return self.owner()

    def __unicode__(self):
        return u"%s(%s)" % (self.origin,
                            u",".join(column.name
                                      for column in self.origin_columns))


class MutableUniqueKeyEntity(UniqueKeyEntity, MutableEntity):

    def __init__(self, origin, origin_columns, is_primary, is_partial):
        assert isinstance(origin, MutableTableEntity)
        assert isinstance(origin_columns, listof(MutableColumnEntity))
        assert len(origin_columns) > 0
        assert all(column.table is origin for column in origin_columns)
        assert isinstance(is_primary, bool)
        assert isinstance(is_partial, bool)
        if is_primary:
            assert not is_partial
            assert origin.primary_key is None
            assert all(not column.is_nullable for column in origin_columns)
        super(MutableUniqueKeyEntity, self).__init__(weakref.ref(origin))
        self.origin_columns = origin_columns
        self.is_primary = is_primary
        self.is_partial = is_partial
        origin.unique_keys.append(self)
        if is_primary:
            origin.primary_key = self

    def set_is_primary(self, is_primary):
        assert isinstance(is_primary, bool)
        if is_primary == self.is_primary:
            return self
        if is_primary:
            assert not self.is_partial
            assert [not column.is_nullable for column in self.origin_columns]
            assert self.origin.primary_key is None
            self.origin.primary_key = self
        else:
            self.origin.primary_key = None
        self.is_primary = is_primary
        return self

    def set_is_partial(self, is_partial):
        assert isinstance(is_partial, bool)
        if is_partial == self.is_partial:
            return self
        if is_partial:
            assert not self.is_primary
        self.is_partial = is_partial
        return self

    def freeze(self):
        self.__class__ = UniqueKeyEntity

    def remove(self):
        self.origin.unique_keys.remove(self)
        if self.is_primary:
            self.origin.primary_key = None
        self.__dict__.clear()
        self.__class__ = RemovedEntity


class ForeignKeyEntity(Entity):

    @property
    def origin(self):
        return self.owner()

    @property
    def target(self):
        return self.coowner()

    def __unicode__(self):
        return (u"%s(%s) -> %s(%s)"
                % (self.origin,
                   u",".join(column.name for column in self.origin_columns),
                   self.target,
                   u",".join(column.name for column in self.target_columns)))


class MutableForeignKeyEntity(ForeignKeyEntity, MutableEntity):

    def __init__(self, origin, origin_columns, target, target_columns,
                 is_partial):
        assert isinstance(origin, MutableTableEntity)
        assert isinstance(origin_columns, listof(MutableColumnEntity))
        assert len(origin_columns) > 0
        assert all(column.table is origin for column in origin_columns)
        assert isinstance(target, MutableTableEntity)
        assert origin.schema.catalog is target.schema.catalog
        assert isinstance(target_columns, listof(MutableColumnEntity))
        assert len(target_columns) == len(origin_columns)
        assert all(column.table is target for column in target_columns)
        assert isinstance(is_partial, bool)
        super(MutableForeignKeyEntity, self).__init__(weakref.ref(origin))
        self.origin_columns = origin_columns
        self.coowner = weakref.ref(target)
        self.target_columns = target_columns
        self.is_partial = is_partial
        origin.foreign_keys.append(self)
        target.referring_foreign_keys.append(self)

    def set_is_partial(self, is_partial):
        assert isinstance(is_partial, bool)
        self.is_partial = is_partial
        return self

    def freeze(self):
        self.__class__ = ForeignKeyEntity

    def remove(self):
        self.origin.foreign_keys.remove(self)
        self.target.referring_foreign_keys.remove(self)
        self.__dict__.clear()
        self.__class__ = RemovedEntity


class Join(Printable, Comparable):
    """
    Represents a join condition between two tables.

    This is an abstract case class with two subclasses: :class:`DirectJoin`
    and :class:`ReverseJoin`.

    Class attributes:

    `is_direct` (Boolean)
        Indicates that the join follows a foreign key
        (set for an instance of :class:`DirectJoin`).

    `is_reverse` (Boolean)
        Indicates that the join follows the opposite direction
        to a foreign key (set for an instance of :class:`ReverseJoin`).

    Attributes:

    `origin` (:class:`TableEntity`)
        The origin table of the join.

    `target` (:class:`TableEntity`)
        The target table of the join.

    `is_expanding` (Boolean)
        Indicates that for each row of the origin table there is
        at least one row of the target table that satisfies
        the join condition.

    `is_contracting` (Boolean)
        Indicates that for each row of the origin table there is
        no more than one row of the target table that satisfies
        the join condition.
    """
    # FIXME: do joins belong to `entity.py`?

    is_direct = False
    is_reverse = False

    def __init__(self, origin, target, origin_columns, target_columns,
                 is_expanding, is_contracting):
        # Sanity check on the arguments.
        assert isinstance(origin, TableEntity)
        assert isinstance(target, TableEntity)
        assert isinstance(origin_columns, listof(ColumnEntity))
        assert isinstance(target_columns, listof(ColumnEntity))
        assert isinstance(is_expanding, bool)
        assert isinstance(is_contracting, bool)

        self.origin = origin
        self.target = target
        self.origin_columns = origin_columns
        self.target_columns = target_columns
        self.is_expanding = is_expanding
        self.is_contracting = is_contracting

    def reverse(self):
        raise NotImplementedError()

    def __unicode__(self):
        # Generate a string of the form:
        #   schema.table -> schema.table
        return u"%s -> %s" % (self.origin, self.target)

    def __str__(self):
        return unicode(self).encode('utf-8')


class DirectJoin(Join):
    """
    Represents a join condition corresponding to a foreign key.

    `foreign_key` (:class:`ForeignKeyEntity`)
        The foreign key that generates the join condition.
    """

    is_direct = True

    def __init__(self, foreign_key):
        # Sanity check on the arguments.
        assert isinstance(foreign_key, ForeignKeyEntity)

        # The origin and target tables.
        origin = foreign_key.origin
        target = foreign_key.target

        # The columns that form the join condition.
        origin_columns = foreign_key.origin_columns
        target_columns = foreign_key.target_columns

        # If all referencing columns are `NOT NULL` and the key is total,
        # the target row always exists.
        is_expanding = not (foreign_key.is_partial or
                            any(column.is_nullable
                                for column in foreign_key.origin_columns))
        # Normally, the foreign key always refers to a unique key of
        # the target table; so the join should always be contracting.
        is_contracting = any(all(column in foreign_key.target_columns
                                 for column in unique_key.origin_columns)
                             for unique_key in foreign_key.target.unique_keys)

        super(DirectJoin, self).__init__(origin, target,
                                         origin_columns, target_columns,
                                         is_expanding, is_contracting)
        self.foreign_key = foreign_key

    def __basis__(self):
        return (self.foreign_key,)

    def reverse(self):
        return ReverseJoin(self.foreign_key)


class ReverseJoin(Join):
    """
    Represents a join condition that joins tables in the opposite direction
    to some foreign key.

    `foreign_key` (:class:`ForeignKeyEntity`)
        The foreign key that generates the join condition.
    """

    is_reverse = True

    def __init__(self, foreign_key):
        # Sanity check on the arguments.
        assert isinstance(foreign_key, ForeignKeyEntity)

        # The origin and target tables.
        origin = foreign_key.target
        target = foreign_key.origin

        # The columns that form the join condition.
        origin_columns = foreign_key.target_columns
        target_columns = foreign_key.origin_columns

        # Unset since we do not know if all rows in the target table
        # of a foreign key are referenced.
        is_expanding = False
        # Set if the foreign key is one-to-one.  It is so if and only if
        # the referencing columns form a unique key.
        is_contracting = any(all(column in foreign_key.origin_columns
                                 for column in unique_key.origin_columns)
                             for unique_key in foreign_key.origin.unique_keys)

        super(ReverseJoin, self).__init__(origin, target,
                                          origin_columns, target_columns,
                                          is_expanding, is_contracting)
        self.foreign_key = foreign_key

    def __basis__(self):
        return (self.foreign_key,)

    def reverse(self):
        return DirectJoin(self.foreign_key)


def make_catalog():
    return MutableCatalogEntity()


