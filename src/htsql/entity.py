#
# Copyright (c) 2006-2011, Prometheus Research, LLC
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
    """
    Represents a database metadata object.
    """

    def __str__(self):
        return self.__class__.__name__.lower()

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self)


class NamedEntity(Entity):
    """
    Represents a database object with a name.

    `name` (a string)
        The object name
    """

    def __init__(self, name):
        # Sanity check on the argument.
        assert isinstance(name, str) and len(name) > 0
        self.name = name

    def __str__(self):
        return self.name

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self)


class EntitySet(object):
    """
    Implements an ordered collection of named entities.

    :class:`EntitySet` provides a read-only mapping interface to a collection
    of named entities.  The only deviation from the mapping interface is that
    iteration generates entities in the original order (instead of entity
    names in an arbitrary order).

    `entities`
        A list of named entities.  Entity names must be unique.
    """

    def __init__(self, entities):
        # Sanity check on the argument.
        assert isinstance(entities, listof(NamedEntity))

        # An ordered list of entities.
        self.entities = entities
        # A mapping: name -> entity.
        self.entity_by_name = dict((entity.name, entity)
                                   for entity in entities)

        # Verify that the names are unique.
        assert len(self.entity_by_name) == len(self.entities)

    def __contains__(self, name):
        """
        Checks if the collection contains an entity with the given name.
        """
        return (name in self.entity_by_name)

    def __getitem__(self, name):
        """
        Returns an entity with the given name.

        Raises :exc:`KeyError` if there is no entity with the given name
        in the collection.
        """
        return self.entity_by_name[name]

    def __iter__(self):
        """
        Generates entities in the original order.
        """
        return iter(self.entities)

    def __len__(self):
        """
        Returns the number of entities in the collection.
        """
        return len(self.entities)

    def get(self, name, default=None):
        """
        Returns an entity with the given name.

        If the collection does not contain an entity with the given name,
        returns the `default` value.
        """
        return self.entity_by_name.get(name, default)

    def keys(self):
        """
        Returns a list of entity names in the original order.
        """
        return [entity.name for entity in self.entities]

    def values(self):
        """
        Returns a list of entities in the original order.
        """
        return self.entities[:]

    def items(self):
        """
        Returns a list of pairs ``(name, entity)`` in the original order.
        """
        return [(entity.name, entity) for entity in self.entities]


class CatalogEntity(Entity):
    """
    Encapsulates database metadata.

    `schemas` (a list of :class:`SchemaEntity`)
        A list of schemas.
    """

    def __init__(self, schemas):
        # Sanity check on the argument.
        assert isinstance(schemas, listof(SchemaEntity))
        # An ordered mapping: name -> schema.
        self.schemas = EntitySet(schemas)


class SchemaEntity(NamedEntity):
    """
    Represents a database schema.

    `name` (a string)
        The schema name.

    `tables` (a list of :class:`TableEntity`)
        A list of tables in the schema.
    """

    def __init__(self, name, tables):
        # Sanity check on the arguments.
        assert isinstance(tables, listof(TableEntity))
        assert all(table.schema_name == name for table in tables)

        super(SchemaEntity, self).__init__(name)
        # An ordered mapping: name -> table.
        self.tables = EntitySet(tables)


class TableEntity(NamedEntity):
    """
    Represents a database table or a view.

    `schema_name` (a string)
        The name of the schema to which the table belongs.

    `name` (a string)
        The table name.

    `columns` (a list of :class:`ColumnEntity`)
        A list of columns of the table.

    `unique_keys` (a list of :class:`UniqueKeyEntity`)
        A list of unique key constraints applied to the table.

    `foreign_keys` (a list of :class:`ForeignKeyEntity`)
        A list of foreign key constraints applied to the table.
    """

    def __init__(self, schema_name, name, columns, unique_keys, foreign_keys):
        # Sanity check on the arguments.
        assert isinstance(schema_name, str)
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
        # An ordered mapping: name -> column.
        self.columns = EntitySet(columns)
        self.unique_keys = unique_keys
        # Find the primary key if it exists.
        self.primary_key = None
        primary_keys = [uk for uk in unique_keys if uk.is_primary]
        assert len(primary_keys) <= 1
        if primary_keys:
            self.primary_key = primary_keys[0]
        self.foreign_keys = foreign_keys

    def __str__(self):
        return "%s.%s" % (self.schema_name, self.name)


class ColumnEntity(NamedEntity):
    """
    Represents a column of a table.

    `schema_name`, `table_name` (strings)
        The schema name and the name of the table to which the column belongs.

    `name` (a string)
        The name of the column.

    `domain` (:class:`htsql.domain.Domain`)
        The column type.

    `is_nullable` (Boolean)
        Indicates if the column admits ``NULL`` values.

    `has_default` (Boolean)
        Indicates if the column has some (explicitly set) default value.
    """

    def __init__(self, schema_name, table_name, name,
                 domain, is_nullable=False, has_default=False):
        # Sanity check on the arguments.
        assert isinstance(schema_name, str)
        assert isinstance(table_name, str)
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
    """
    Represents a unique key constraint.

    Class attributes:

    `is_primary` (Boolean)
        Indicates if the constraint represents a primary key.

    The constructor accepts the following parameters:

    `origin_schema_name`, `origin_name` (strings)
        The schema name and the name of the table to which the constraint
        is applied.

    `origin_column_names` (a list of strings)
        The names of the columns which the constraint comprises.
    """

    is_primary = False

    def __init__(self, origin_schema_name, origin_name, origin_column_names):
        # Sanity check on the arguments.
        assert isinstance(origin_schema_name, str)
        assert isinstance(origin_name, str)
        assert isinstance(origin_column_names, listof(str))

        self.origin_schema_name = origin_schema_name
        self.origin_name = origin_name
        self.origin_column_names = origin_column_names

    def __str__(self):
        # Generate a string of the form:
        #   schema.table(column,...)
        return "%s.%s(%s)" % (self.origin_schema_name, self.origin_name,
                              ",".join(self.origin_column_names))


class PrimaryKeyEntity(UniqueKeyEntity):
    """
    Represents a primary key constraint.
    """

    is_primary = True


class ForeignKeyEntity(Entity):
    """
    Represents a foreign key constraint.

    `origin_schema_name`, `origin_name` (strings)
        The schema name and the name of the table to which the constraint
        is applied.

    `origin_column_names` (a list of strings)
        The names of the columns which the constraint comprises.

    `target_schema_name`, `target_name` (strings)
        The schema name and the name of the referenced table.

    `target_column_names` (a list of strings)
        The names of the columns which the constraint refers to.
    """

    def __init__(self, origin_schema_name, origin_name, origin_column_names,
                 target_schema_name, target_name, target_column_names):
        # Sanity check on the arguments.
        assert isinstance(origin_schema_name, str)
        assert isinstance(origin_name, str)
        assert isinstance(origin_column_names, listof(str))
        assert isinstance(target_schema_name, str)
        assert isinstance(target_name, str)
        assert isinstance(target_column_names, listof(str))
        assert len(origin_column_names) == len(target_column_names) > 0

        self.origin_schema_name = origin_schema_name
        self.origin_name = origin_name
        self.origin_column_names = origin_column_names
        self.target_schema_name = target_schema_name
        self.target_name = target_name
        self.target_column_names = target_column_names

    def __str__(self):
        # Generate a string of the form:
        #   schema.table(column,...) -> schema.table(column,...)
        return "%s.%s(%s) -> %s.%s(%s)" \
                % (self.origin_schema_name, self.origin_name,
                   ",".join(self.origin_column_names),
                   self.target_schema_name, self.target_name,
                   ",".join(self.target_column_names))


class Join(object):
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

    def __init__(self, origin, target, is_expanding, is_contracting):
        # Sanity check on the arguments.
        assert isinstance(origin, TableEntity)
        assert isinstance(target, TableEntity)
        assert isinstance(is_expanding, bool)
        assert isinstance(is_contracting, bool)

        self.origin = origin
        self.target = target
        self.is_expanding = is_expanding
        self.is_contracting = is_contracting

    def __str__(self):
        # Generate a string of the form:
        #   schema.table -> schema.table
        return "%s -> %s" % (self.origin, self.target)

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self)

    def __hash__(self):
        # Since joins are used in comparison operations of code and space
        # objects, we need to override hash and equality operators to
        # provide comparison by value.  Override in subclasses.
        raise NotImplementedError()

    def __eq__(self, other):
        # Since joins are used in comparison operations of code and space
        # objects, we need to override hash and equality operators to
        # provide comparison by value.  Override in subclasses.
        raise NotImplementedError()

    def __ne__(self, other):
        # Since we override `==`, we also need to override `!=`.
        return not (self == other)


class DirectJoin(Join):
    """
    Represents a join condition corresponding to a foreign key.

    `origin` (:class:`TableEntity`)
        The origin table of the join.

    `target` (:class:`TableEntity`)
        The target table of the join.

    `foreign_key` (:class:`ForeignKeyEntity`)
        The foreign key that generates the join condition.  Note
        that the origin and the target of the key must coincide with
        the `origin` and `target` parameters.
    """

    is_direct = True

    def __init__(self, origin, target, foreign_key):
        # Sanity check on the arguments.
        assert isinstance(origin, TableEntity)
        assert isinstance(target, TableEntity)
        assert isinstance(foreign_key, ForeignKeyEntity)
        assert ((origin.schema_name, origin.name) ==
                (foreign_key.origin_schema_name, foreign_key.origin_name))
        assert ((target.schema_name, target.name) ==
                (foreign_key.target_schema_name, foreign_key.target_name))

        # The columns that form the join condition.
        self.origin_columns = [origin.columns[name]
                               for name in foreign_key.origin_column_names]
        self.target_columns = [target.columns[name]
                               for name in foreign_key.target_column_names]

        # If all referencing columns are `NOT NULL`, the target row
        # always exists.
        is_expanding = all(not column.is_nullable
                           for column in self.origin_columns)
        # For a join condition corresponding to a foreign key, there is always
        # no more than one row in the target table.
        is_contracting = True

        super(DirectJoin, self).__init__(origin, target,
                                         is_expanding, is_contracting)
        self.foreign_key = foreign_key

    def __hash__(self):
        # Provide comparison by value.
        return hash(self.foreign_key)

    def __eq__(self, other):
        # Provide comparison by value.
        return (isinstance(other, DirectJoin) and
                self.foreign_key == other.foreign_key)


class ReverseJoin(Join):
    """
    Represents a join condition that joins tables in the opposite direction
    to some foreign key.

    `origin` (:class:`TableEntity`)
        The origin table of the join.

    `target` (:class:`TableEntity`)
        The target table of the join.

    `foreign_key` (:class:`ForeignKeyEntity`)
        The foreign key that generates the join condition.  Note
        that the origin and the target of the key must coincide with
        the `target` and `origin` parameters respectively.
    """

    is_reverse = True

    def __init__(self, origin, target, foreign_key):
        # Sanity check on the arguments.
        assert isinstance(origin, TableEntity)
        assert isinstance(target, TableEntity)
        assert isinstance(foreign_key, ForeignKeyEntity)
        assert ((origin.schema_name, origin.name) ==
                (foreign_key.target_schema_name, foreign_key.target_name))
        assert ((target.schema_name, target.name) ==
                (foreign_key.origin_schema_name, foreign_key.origin_name))
        assert isinstance(foreign_key, ForeignKeyEntity)

        # The columns that form the join condition.
        self.origin_columns = [origin.columns[name]
                               for name in foreign_key.target_column_names]
        self.target_columns = [target.columns[name]
                               for name in foreign_key.origin_column_names]

        # Unset since we do not know if all rows in the target table
        # of a foreign key are referenced.
        is_expanding = False
        # Set if the foreign key is one-to-one.  It is so if and only if
        # the referencing columns form a unique key.
        is_contracting = False
        for uk in target.unique_keys:
            if all(column_name in foreign_key.origin_column_names
                   for column_name in uk.origin_column_names):
                is_contracting = True

        super(ReverseJoin, self).__init__(origin, target,
                                          is_expanding, is_contracting)
        self.foreign_key = foreign_key

    def __hash__(self):
        # Provide comparison by value.
        return hash(self.foreign_key)

    def __eq__(self, other):
        # Provide comparison by value.
        return (isinstance(other, ReverseJoin) and
                self.foreign_key == other.foreign_key)


