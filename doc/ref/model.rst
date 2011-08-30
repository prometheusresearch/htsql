**************
  Data Model
**************

HTSQL is not a full-fledged database system.  As opposed to regular data
stores, it does not include a storage layer, but relies on a relational
database server to physically store and retrieve data.

HTSQL is designed to work on top of existing relational databases and
does not impose any restrictions on how information is modeled and
stored there.  At the same time, HTSQL works best when the data in the
database is highly normalized.

Even though HTSQL wraps a relational database, it does not expose the
relational model directly to the users.  Instead it derives *HTSQL data
model* from the underlying database and uses this model when presenting
data to the users and interpreting user queries.  HTSQL data model is
very close to traditional `network data model`_ utilized by CODASYL, and
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
a school of *Engineering* with associated departments of *Computer
Science*, *Electrical Engineering*, etc.:

.. diagram:: ../dia/model-and-instance.tex
   :align: center


Classes and Links
=================

HTSQL structures the data with *classes* and *links*, which together
form *a model graph*.  Classes, which are the nodes in the model graph,
represents types of entities.  Links, which are the arcs in the model
graph, describe relations between entities.  Both classes and links
have a name.

Among classes we distinguish *domain classes* and *record classes*.
Domain classes represent scalar data types such as `boolean`, `integer`,
`string`, `date`.  Record classes represent types of business entities
modeled by the database.  A student enrollment system in our example
would have record classes such as `school`, `program`, `department`,
`course`.

Links are classified by the type of classes they connect.  A link from a
record class to a domain class indicates that records of this class have
an attribute, which type is specified by the domain class.  For example,
`school` class may have a link called `name` to `string` class, which
indicates that each *school* record has a string attribute *name*.

A link between two record classes indicates that records of these
classes are related to each other.  For example, `department` class
has a link to `school` class, which indicates that each *department*
record may be associated with some *school* record.

.. diagram:: ../dia/sample-model.tex
   :align: center

Since different links may have the same name, we will use dotted
notation `class.link` to indicate links.  Here, `class` is the name of a
class, `link` is the name of a link originating from the class.  Thus,
`school.name` and `department.school` are links on the diagram.


Records and Relations
=====================

As we focus from the database model to a specific instance, classes
are populated with values and records, and links are expanded to
relations between individual items.

On the instance level, a domain class is transformed into a set of all
values of the respective type.  Thus, `boolean` class contains two
values: ``true`` and ``false``, `integer` class contains all integer
numbers, and so on.

A record class becomes a set of records representing business entities
of this class.

It is convenient to depict an entity as a collection of attribute
values, hence the word "record".  Even though it is permitted for two
different records to have the same set of attribute values, in practice,
there often exists an attribute or a group of attributes which could
uniquely identify a record.  We use the value of such an attribute
enclosed in brackets to denote records in writing.  Thus, an instance of
class `school` may contain records ``[eng]``, ``[la]``, ``[ns]``
representing respectively schools of *Engineering*, of *Arts and
Humanities*, and of *Natural Sciences*, assuming that we use attribute
`school.code` to uniquely identify records.

.. diagram:: ../dia/sample-instance-1.tex
   :align: center

A link between two classes is unwound into connections between elements
of these classes.  If in the database model a link represents an entity
attribute, in a specific instance a link connects records to attribute
values.  A link between two record classes would connect records of
these classes.

For example, link `school.name` connects a school record ``[eng]`` to a
string value ``'School of Engineering'``.  The record ``[eng]`` is also
connected to department records ``[comp]`` and ``[ee]`` indicating that
*Department of Computer Science* and *Department of Electrical
Engineering* belong to *School of Engineering*.

.. diagram:: ../dia/sample-instance-2.tex
   :align: center

Some links may enforce constraints on connections between elements.
We classify these constraints as follows:

A link is called *singular* if any element of the origin class is
connected to no more than one element of the target class.  Otherwise,
the link is called *plural*.

For example, all links representing attributes are singular; link
`department.school` is also singular because each department may be
associated with just one school, but the *reverse* link
`school.department` is plural since a school may contain more than one
department.

.. diagram:: ../dia/singular-links.tex
   :align: center

A link is called *total* if any element of the origin class is connected
to at least one element of the target class.  Otherwise, the link is
called *partial*.

For example, we require that every school entity has a code, therefore
attribute `school.code` is total.  We also permit a department
to lack an associated school, which means link `department.school`
is partial.

.. diagram:: ../dia/total-links.tex
   :align: center

A link is called *unique* if any element of the target class is
connected to no more than one element of the origin class.  Otherwise,
the link is *non-unique*.

Attribute `school.name` is unique since different school entities must
have different names, but link `department.school` is non-unique as
different departments are allowed to be associated with the same school.

.. diagram:: ../dia/unique-links.tex
   :align: center

Note that links constraints are defined on the database model
and applied to all instances of the model.


Correspondence to the Relational Model
======================================

In this section, we explain how underlying relation database model
is translated to HTSQL data model.

For the most part, translation of relational structure to HTSQL model
is straightforward.  SQL data types become domain classes, SQL tables
become record classes, table columns are class attributes.

Column constraints are trivially translated to properties of the
respective attributes.  ``NOT NULL`` constraint on a table column means,
in HTSQL terms, that the respective class attribute is total.
``UNIQUE`` constraint on a column indicates that the respective
attribute link is unique.  ``PRIMARY KEY`` constraint indicates that the
attribute link is both total and unique.

The link structure of the model graph is provided by foreign key
constraints.  Specifically, a foreign key creates a singular link
from the referring class to the referred class.

Consider, for example, the following fragment of an SQL schema:

.. sourcecode:: sql

   CREATE TABLE ad.school (
       code                VARCHAR(16) NOT NULL,
       name                VARCHAR(64) NOT NULL,
       campus              VARCHAR(5),
       CONSTRAINT school_pk
         PRIMARY KEY (code),
       CONSTRAINT name_uk
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

HTSQL model of this schema consists of two classes, `school` and
`department`, each with three attributes: `code`, `name`, `campus`
and `code`, `name`, `school_code` respectively.  Additionally,
the foreign key constraint ``department_school_fk`` generates
a singular link from class `department` to class `school` and a
reverse plural link from class `school` to class `department`.


.. vim: set spell spelllang=en textwidth=72:
