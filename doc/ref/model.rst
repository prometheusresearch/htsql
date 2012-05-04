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


Classes and Links
=================

HTSQL represents a database model in form of a directed graph; the nodes
of the graph are called *classes* and the arcs are called *links*.

We distinguish several types of classes:

* A *value class* represents a scalar data type such as `boolean`,
  `integer`, `string`, `date`.
* An *entity class* represents a collection of homogeneous entities
  modeled by the database.  For instance, a student enrollment database
  may contain entity classes such as `school`, `program`, `department`,
  `course`.
* Each database model contains one instance of a *unit class*.  It
  serves as an origin node when we construct paths in the model graph.

A link represents a relationship between two classes.

* Each entity class has a link from the unit class.  This link
  represents the set of all entities from the class.
* A link connecting an entity class to a value class represents an
  entity attribute.
* A link between two entity classes denotes a relation between the
  respective entities.

Each link has a name, which must be unique among all links sharing the
same origin class.  We use a dot-separated sequence of link names to
identify a path in the model graph that starts at the unit class.
Thus ``school.department.name`` identifies a path with three links:
``school`` connecting the unit class to an entity class, ``department``
connecting two entity classes and ``name`` connecting an entity class to
a value class.

We will routinely use the dotted notation to refer to the last link or
the target class in the path.  For example, we write ``school`` to
indicate the `school` class, ``department.name`` to indicate link
``name`` from `department` class to `string` class.

The following diagram shows a fragment of the model graph for our
student enrollment database (most of the value classes and attribute
links are omitted for clarity).

.. diagram:: ../dia/sample-model.tex
   :align: center

The marked path on the diagram represents the query:

.. htsql:: /school.department.name
   :cut: 3


Entities and Relations
======================

As we focus from the database model to a specific instance, classes
are populated with values and entities, and each link splits into
connections between individual class elements.

A value class is populated with all values of the respective type.
Thus, `boolean` class acquires two values: ``true`` and ``false``,
`integer` class is filled with all integer numbers, and so on.

An entity class becomes a set of homogeneous business entities; e.g.
`school` class becomes a set of university schools, `department` a set
of departments, etc.

In HTSQL, individual entities are not observable, only entity attributes
are.  When we need to refer to a specific entity in writing, we use the
value of some entity attribute that can uniquely identify it, enclosed
in brackets.  For example, attribute `school.code` uniquely identifies
`school` entities, therefore we may say that ``[eng]``, ``[la]``,
``[ns]`` are respectively entities representing schools of
*Engineering*, of *Arts and Humanities*, and of *Natural Sciences*.

The unit class contains a single value, which is called *unit*
and denoted by ``@``.

.. diagram:: ../dia/sample-instance-1.tex
   :align: center

A link between two classes splits into a binary relation between
elements of these classes:

* A link from the unit class to an entity class connects the unit
  to every entity in the entity class.
* A link between two entity classes connects each entity of the
  origin class to all related entities from the target class.
* A link from an entity class to a value class connects each
  entity with the respective attribute value.

The following diagram demonstrates how the path
``school.department.name`` looks for some specific database instance.

.. diagram:: ../dia/sample-instance-2.tex
   :align: center


Link Constraints
================

Links may enforce constraints on connections between elements.  We
recognize the following constraints: singularity, totality and
uniqueness.

Note that links constraints are defined on the database model
and applied to all instances of the model.

Singular and Plural Links
-------------------------

A link is called *singular* if any element of the origin class is
connected to no more than one element of the target class.  Otherwise,
the link is called *plural*.

* All attribute links are singular.
* Any link from the unit class to an entity class is plural.
* A link between two entity classes may be singular or plural.  For
  example, link ``department.school`` is singular because each
  department may be associated with just one school, but the *reverse*
  link ``school.department`` is plural since a school may include more
  than one department.

The following diagram visualises a singular link ``school.campus`` and
a plural link ``school.department``.

.. diagram:: ../dia/singular-links.tex
   :align: center

Total and Partial Links
-----------------------

A link is called *total* if each element of the origin class is
connected to at least one element of the target class.  Otherwise, the
link is called *partial*.

For example, we require every program to be associated with some school,
so link `program.school` is total.  At the same time, not every program
is a part of another program, therefore link `program.part_of` is
partial.

.. diagram:: ../dia/total-links.tex
   :align: center

Unique and Non-unique Links
---------------------------

A link is called *unique* if any element of the target class is
connected to no more than one element of the origin class.  Otherwise,
the link is *non-unique*.

Attribute `department.name` is unique since different department
entities must have different names, but link `department.school` is
non-unique as different departments are allowed to be associated with
the same school.

.. diagram:: ../dia/unique-links.tex
   :align: center


Correspondence to the Relational Model
======================================

In this section, we explain how underlying relation database model
is translated to HTSQL data model.

For the most part, translation of relational structure to HTSQL model is
straightforward.  SQL data types become value classes, SQL tables become
entity classes, table columns become class attributes.  Links between
entity classes are inferred from FOREIGN KEY constraints.

HTSQL allows the administrator to restrict access to specific tables and
columns, configure additional database constraints, and rename link
names.  In the following sections we describe how HTSQL describes
database model in the absence of any configuration.

A name in HTSQL is a sequence of letters, digits and ``_`` characters
which doesn't start with a digit.  When an HTSQL name is generated from
a SQL name which contains non-alphanumeric characters, those are
replaced with an underscore (``_``).

Entity Names
------------

Each SQL table induces an entity class, which, in general, borrows its
name from the table.

Some SQL database servers support a notion of *schemas*, namespaces for
tables, which may cause a naming conflict when two or more different
schemas have tables with the same name.  This conflict is resolved as
follows:

* If one of the schemas is marked as "default" for the purposes of name
  resolution, the name of the respective table is borrowed unadorned.
* For the remaining tables, the assigned name has the form
  ``<schema>_<name>``.

Attribute Names
---------------

Each table column induces an entity attribute with the same name.

When the column is a ``FOREIGN KEY`` constraint, the column name is also
used to refer to the respective entity link.  The usage is determined
from the context; compare

.. htsql:: /department.school_code
   :cut: 3

and

.. htsql:: /department.school_code.*
   :cut: 3

Entity Links
------------

Each ``FOREIGN KEY`` constraint generates two links between respective
entity classes, one in the direction of the constraint, called
*direct*, and the other in the opposite direction, called *reverse*.

The names of the links are synthesized from the names of the tables and
names of the columns which form the constraint.  If the name of the
referring column ends with the name of the referred column (e.g.
``department.school_code`` and ``school.code``), we call the beginning
of the referring column a *prefix* (in this case, ``school``).

The link names are generated according to the following rules.  The
adopted name is the first one which doesn't conflict with other link
names with the same origin class.

1. When the link is direct and the prefix exists, use the prefix.
2. Use the name of the target table.  In case there are more than one
   link to the same target, prefer the one for which the referring
   column is a primary key.
3. If the link is reverse and the prefix exists, use the name of the
   form ``<target>_via_<prefix>``.
4. If the link is reverse, use the name of the form
   ``<target>_via_<column>``.

Link Constraints
----------------

Column constraints are trivially translated to properties of the
respective attribute links.

* A ``NOT NULL`` constraint on a column means, in HTSQL term, that the
  respective attribute is total.
* A ``UNIQUE`` constraint indicates that the attribute is unique.
* A ``PRIMARY KEY`` constraint indicates that the attribute is both
  total and unique.  The columns that form a primary key are also used
  for default ordering on the entity class.
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
``ad.program``, ``ad.course`` generate four entity classes:

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
