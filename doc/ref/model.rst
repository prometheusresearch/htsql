**************
  Data Model
**************

.. contents:: Table of Contents
   :depth: 1
   :local:

HTSQL is not a full-fledged database system.  As opposed to regular data
stores, it does not include a storage layer, but relies on a SQL
database server to physically store and retrieve data.

HTSQL is designed to work on top of existing relational databases and
does not impose any restrictions on how information is modeled and
stored there.  At the same time, HTSQL works best when the data in the
database is highly normalized.

Even though HTSQL wraps a relational database, it does not expose the
relational model directly to the users.  Instead it derives *HTSQL data
model* from the underlying database and uses this model when presenting
data to the users and interpreting user queries.  HTSQL data model is
very close to traditional `network data model`_ utilized by CODASYL and
various OODBMS and ORM systems.

.. _network data model: http://en.wikipedia.org/wiki/Network_model

In the next sections, we describe HTSQL data model and how it is
inferred from the relational model of the underlying database.


Model and Instances
===================

When describing how information is represented by HTSQL, we
differentiate between *a database model* and *a database instance*.

A database model specifies the structure of the database: what types of
business entities are represented and how the entities may relate to
each other.  A database instance is the actual data in the database
and must satisfy the constraints imposed by the model.  The difference
between a model and an instance is the difference between the shape
of data and data itself.

Let's consider the model of a student enrollment system in a fictional
university.  This model may contain schools, programs administered
by a school, departments associated with a school, and courses offered
by a department.  A concrete instance of this model may contain
a school of *Engineering* with an associated department of *Computer
Science*, which offers a *Database Theory* course, etc.:

.. diagram:: ../dia/model-and-instance.tex
   :align: center


Model Graph
===========

HTSQL represents a database model as a directed graph, or a collection
of nodes connected by arrows.

A model graph may contain several types of nodes:

* A *domain* node represents a scalar data type such as boolean,
  integer, string, date.
* A *class* node represents a collection of homogeneous business
  entities.  For instance, a student enrollment database may contain
  classes for school, program, department, and course entities.
* Each model graph contains one instance of a *unit* node.  The unit
  serves as an origin node when we construct paths in the model graph.

Arrows in the model graph are categorized by the type of nodes they
connect:

* There is exactly one arrow connecting the unit node to each class
  node.  We call it a *class* arrow.  It represents the set of all
  entities of the target class.
* An arrow connecting a class node to a domain node is called an
  *attribute* arrow and represents an entity attribute.
* An arrow between two class nodes denotes a relationship between
  entities of the respective classes and is called a *link* arrow.

Each arrow has a *name*.  The arrow name must be unique among all arrows
with the same origin node.

The following diagram shows a fragment of the model graph for the
student enrollment database (most of the domain nodes and attribute
arrows are omitted for clarity).

.. diagram:: ../dia/sample-model.tex
   :align: center

A *navigation* in the model graph is a path, or a sequence of arrows,
where the first arrow starts at the unit node and the target of each
arrow coincides with the origin of the next arrow.  We denote a
navigation using arrow names separated by a period (``.``).  For
example, in the diagram above, the selected navigation is denoted by
``school.department.name``.

A navigation expresses a respective HTSQL query:

.. htsql:: /school.department.name
   :cut: 3

.. note::

   We will routinely use the navigational notation to refer to various
   components of the model graph.  Since there is a one-to-one
   correspondence between class arrows and class nodes, we can use the
   arrow name to refer to the target class; for example, we say "class
   ``school``" referring to the target of the class arrow called
   ``school``.  In the same manner, we will often say "link
   ``school.department``" or "attribute ``department.name``" referring
   to the last component of the navigation.


Instances
=========

As we focus from the database model to a specific instance, nodes
are populated with values and entities, and each arrow splits into
connections between individual node elements.

A domain node is populated with all values of the respective type.
Thus, boolean domain acquires two values: ``true`` and ``false``,
integer domain is filled with all integer numbers, and so on.

A class node becomes a set of entities of the respective class; e.g.
``school`` class becomes a set of university schools, ``department`` a
set of departments, etc.

The unit node contains a single value, which is called a *unit*
and denoted by ``@``.

.. diagram:: ../dia/sample-instance-1.tex
   :align: center

.. note::

   In HTSQL, we can only observe entity attributes, but not the entities
   themselves.  When we need to refer to a specific entity in writing,
   we enclose in brackets the value of an entity attribute which
   uniquely identifies the entity.  For example, attribute
   ``school.code`` uniquely identifies ``school`` entities, therefore we
   may say that ``[eng]``, ``[ns]``, ``[sc]`` are respectively entities
   representing schools of *Engineering*, of *Natural Sciences*, and
   of *Continuing Studies*.

An arrow between two nodes splits into a binary relation between
elements of these nodes:

* An arrow from the unit node to a class node connects the unit
  to every entity in the target class.
* An arrow between two class nodes connects each entity of the
  origin class to all related entities in the target class.
* An arrow from a class node to a domain node connects each
  entity with the respective attribute value.

The following diagram visualizes the navigation
``school.department.name`` on a specific database instance.

.. diagram:: ../dia/sample-instance-2.tex
   :align: center


Arrow Constraints
=================

Arrows may enforce constraints on connections between elements.  We
recognize the following constraints: singularity, totality and
uniqueness.

Note that arrow constraints are defined on the database model and
applied to all instances of the model.

Singular and Plural Arrows
--------------------------

An arrow is called *singular* if any element of the origin node is
connected to no more than one element of the target node.  Otherwise,
the arrow is called *plural*.

* All attribute arrows are singular.
* All class arrows are plural.
* A link arrow, which connects two class nodes, may be singular or
  plural.  For example, link ``department.school`` is singular because
  each department may be associated with just one school, but the
  *reverse* link ``school.department`` is plural since a school may
  include more than one department.

The following diagram visualises a singular link ``program.school``
and a plural link ``school.department``.

.. diagram:: ../dia/singular-links.tex
   :align: center

Total and Partial Arrows
------------------------

An arrow is called *total* if each element of the origin node is
connected to at least one element of the target node.  Otherwise, the
arrow is called *partial*.

* A class arrow is always partial.  It represents the fact that in some
  database instances the class may contain no entities.
* Links and attributes could be total or partial.  For example,
  attribute ``school.name`` is total since each school must have a name,
  but attribute ``school.campus`` is partial since some schools do not
  belong to any campus.

The following diagram shows a total link ``program.school`` and a
partial attribute ``school.campus``.

.. diagram:: ../dia/total-links.tex
   :align: center

Unique and Non-unique Arrows
----------------------------

An arrow is called *unique* if any element of the target node is
connected to no more than one element of the origin node.  Otherwise,
the arrow is *non-unique*.

* A class arrow is non-unique since the class may contain more than one
  entity.
* Links and attributes could be unique and non-unique.  For example,
  attribute ``school.name`` is unique since we require that each school
  has a distinct name, but attribute ``school.campus`` is non-unique
  since several schools may share the same campus.

The following diagram shows a unique attribute ``department.name`` and a
non-unique link ``department.school``.

.. diagram:: ../dia/unique-links.tex
   :align: center


Correspondence to the Relational Model
======================================

In this section, we explain how underlying relation database model
is translated to HTSQL data model.

For the most part, translation of relational structure to HTSQL model is
straightforward.  SQL data types become domain nodes, SQL tables become
class nodes, table columns become attributes.  Links between classes are
inferred from FOREIGN KEY constraints.

HTSQL allows the administrator to restrict access to specific tables and
columns, configure additional database constraints, and rename links.
Here we describe how HTSQL creates a database model from the given SQL
database in the absence of any configuration.

A name in HTSQL is a sequence of letters, digits and ``_`` characters
which does not start with a digit.  When an HTSQL name is generated from
a SQL name which contains non-alphanumeric characters, those are
replaced with an underscore (``_``).

Classes
-------

Each SQL table induces a class node, which, in general, borrows its
name from the table.

Some SQL database servers support a notion of *schemas*, or collections
of tables.  Tables in the same schema must have unique names, but two or
more tables in different schemas may share the same name, in which case
HTSQL cannot use the name directly.  This naming conflict is resolved as
follows:

* If one of the schemas is marked as "default" for the purposes of name
  resolution, the name of the respective table is borrowed unadorned.
* For the remaining tables, the assigned HTSQL name has the form
  ``<schema>_<name>``.

Attributes
----------

Each table column induces a class attribute with the same name.

When the column has a ``FOREIGN KEY`` constraint, the column name is
also used to refer to the respective link.  In this case, whether the
name refers to an attribute or a link is determined from context;
compare

.. htsql:: /department.school_code
   :cut: 3

and

.. htsql:: /department.school_code.*
   :cut: 3

Links
-----

Each ``FOREIGN KEY`` constraint generates two links between respective
class nodes, one in the direction of the constraint, called *direct*,
and the other in the opposite direction, called *reverse*.

The names of the links are synthesized from the names of the tables and
names of the columns which form the constraint.  If the name of the
referring column ends with the name of the referred column (e.g.
``department.school_code`` refers to ``school.code``), we call the
beginning of the referring column a *prefix* (in this case, ``school``).
The prefix is stripped from any underscore characters.

The link names are generated according to the following rules.  The
adopted name is the first one which doesn't conflict with other arrows
with the same origin class.

1. When the link is direct and the prefix exists, use the prefix.
2. Use the name of the target table.  When there are more than one
   link to the same target, prefer the one for which the referring
   column is a primary key.
3. If the link is reverse and the prefix exists, use the name of the
   form ``<target>_via_<prefix>``.
4. If the link is reverse, use the name of the form
   ``<target>_via_<column>``.

Constraints
-----------

Column constraints are trivially translated to properties of the
respective attribute arrows.

* A ``NOT NULL`` constraint on a column means, in HTSQL terms, that the
  respective attribute is total.
* A ``UNIQUE`` constraint indicates that the attribute is unique.
* A ``PRIMARY KEY`` constraint indicates that the attribute is both
  total and unique.  The columns that form a primary key are also used
  for default ordering on the class.
* A direct link induced by a ``FOREIGN KEY`` constraint is always
  singular.  The reverse link is plural in general, but could be
  singular when the key column is ``UNIQUE``.

An Example
----------

Consider, for example, the following fragment of an SQL schema:

.. sourcecode:: sql

    CREATE SCHEMA ad;

    CREATE TABLE ad.school (
        code                VARCHAR(16) NOT NULL,
        name                VARCHAR(64) NOT NULL,
        campus              VARCHAR(5),
        CONSTRAINT school_pk
          PRIMARY KEY (code),
        CONSTRAINT school_name_uk
          UNIQUE (name),
        CONSTRAINT school_campus_ck
          CHECK (campus IN ('old', 'north', 'south'))
    );

    CREATE TABLE ad.department (
        code                VARCHAR(16) NOT NULL,
        name                VARCHAR(64) NOT NULL,
        school_code         VARCHAR(16),
        CONSTRAINT department_pk
          PRIMARY KEY (code),
        CONSTRAINT department_name_uk
          UNIQUE (name),
        CONSTRAINT department_school_fk
          FOREIGN KEY (school_code)
          REFERENCES ad.school(code)
    );

    CREATE TABLE ad.program (
        school_code         VARCHAR(16) NOT NULL,
        code                VARCHAR(16) NOT NULL,
        title               VARCHAR(64) NOT NULL,
        degree              CHAR(2),
        part_of_code        VARCHAR(16),
        CONSTRAINT program_pk
          PRIMARY KEY (school_code, code),
        CONSTRAINT program_title_uk
          UNIQUE (title),
        CONSTRAINT program_degree_ck
          CHECK (degree IN ('bs', 'pb', 'ma', 'ba', 'ct', 'ms', 'ph')),
        CONSTRAINT program_school_fk
          FOREIGN KEY (school_code)
          REFERENCES ad.school(code),
       CONSTRAINT program_part_of_fk
          FOREIGN KEY (school_code, part_of_code)
          REFERENCES ad.program(school_code, code)
    );

    CREATE TABLE ad.course (
        department_code     VARCHAR(16) NOT NULL,
        no                  INTEGER NOT NULL,
        title               VARCHAR(64) NOT NULL,
        credits             INTEGER,
        description         TEXT,
        CONSTRAINT course_pk
          PRIMARY KEY (department_code, no),
        CONSTRAINT course_title_uk
          UNIQUE (title),
        CONSTRAINT course_dept_fk
          FOREIGN KEY (department_code)
          REFERENCES ad.department(code)
    );

In this schema, four tables ``ad.school``, ``ad.department``,
``ad.program``, ``ad.course`` generate four classes:

.. htsql:: /school
   :cut: 3

.. htsql:: /department
   :cut: 3

.. htsql:: /program
   :cut: 3

.. htsql:: /course
   :cut: 3

Foreign key constraints ``department_school_fk``, ``program_school_fk``,
``course_dept_fk`` generate three direct and three reverse links:

.. htsql:: /department.school
   :hide:

.. htsql:: /school.department
   :hide:

.. htsql:: /program.school
   :hide:

.. htsql:: /school.program
   :hide:

.. htsql:: /course.department
   :hide:

.. htsql:: /department.course
   :hide:

A foreign key ``program_part_of_fk`` induces two self-referential links
on ``program``:

.. htsql:: /program.part_of
   :hide:

.. htsql:: /program.program_via_part_of
   :hide:


.. vim: set spell spelllang=en textwidth=72:
