*************************
  Introduction to HTSQL
*************************


What is HTSQL?
==============

HTSQL is a query language and web service for relational databases.

HTSQL is a Web Service
----------------------

.. vsplit::

   .. sourcecode:: text

      GET /school HTTP/1.1

   .. image:: img/show_school.png
      :alt: output of /school query
      :target: http://demo.htsql.org/school

HTSQL is a query language for the web.  Queries are URLs that can be
directly typed into a browser; the output could be returned in a variety
of formats including HTML, CSV, JSON, etc.

HTSQL is a Relational Database Gateway
--------------------------------------

.. vsplit::

   .. sourcecode:: htsql

      /school

   .. sourcecode:: sql

      SELECT "school"."code",
             "school"."name",
             "school"."campus"
      FROM "ad"."school" AS "school"
      ORDER BY 1 ASC

HTSQL wraps an existing relational database and translates incoming
queries into SQL.  The current version of HTSQL supports *SQLite*,
*PostgreSQL*, *MySQL*, *Oracle*, and *Microsoft SQL Server*.

HTSQL is an Advanced Query Language
-----------------------------------

.. vsplit::

   .. sourcecode:: htsql

      /school{name,
              count(program),
              count(department)}

   .. sourcecode:: sql

      SELECT "school"."name", COALESCE("program"."count", 0), COALESCE("department"."count", 0)
      FROM "ad"."school" AS "school"
      LEFT OUTER JOIN (SELECT COUNT(TRUE) AS "count", "program"."school_code" FROM "ad"."program" AS "program" GROUP BY 2) AS "program" ON ("school"."code" = "program"."school_code")
      LEFT OUTER JOIN (SELECT COUNT(TRUE) AS "count", "department"."school_code" FROM "ad"."department" AS "department" GROUP BY 2) AS "department" ON ("school"."code" = "department"."school_code")
      ORDER BY "school"."code" ASC

HTSQL is a compact, high-level navigational query language designed
for data analysts and web developers.


Why not SQL?
============

Relational algebra is frequently inadequate for encoding business 
inquiries --- elementary set operations do not correspond to 
meaningful data transformations.  The SQL language itself is tedious,
verbose, and provides poor means of abstraction.  Yet, the relational
database is an excellent tool for data modeling, storage and retrieval.

HTSQL reimagines what it means to query a database.  The combination of
a *navigational model* with *data flows* enables expressions that
naturally reflect business inquiries.  The HTSQL translator uses SQL as
a target assembly language, which allows us to fix the query model and
language while keeping current investment in relational systems.

To demonstrate this point, we walk through a set of business inquires
expressed over a fictitious university schema.

.. diagram:: dia/administrative-directory-small-schema.tex
   :align: center

This data model has two top-level tables, ``school`` and ``department``,
where ``department`` has an optional link to ``school``.  Subordinate
tables, ``course`` and ``program``, have mandatory links to their
parents.

SQL Conflates Rows & Columns
----------------------------

    *"For each department, please show the department name and the
    corresponding school's campus."*

This business inquiry clearly separates the requested rows (*each
department*) and columns (*department name* and *corresponding school's
campus*), but this separation is lost when the query is encoded in SQL:

.. sourcecode:: sql

    SELECT d.name, s.campus
    FROM ad.department AS d
    LEFT JOIN ad.school AS s
           ON (d.school_code = s.code);

In this SQL query, the ``FROM`` clause not only picks target rows, but
also includes extra tables required to produce output columns.  This
conflation makes it difficult to determine business entities represented
by each row of the output.

.. htsql::
   :cut: 4
   :hide:

    /department{name, school.campus}

The HTSQL translation separates the row definition from the column
selection.  The linking is implicit, and correct.  The encoded query can
be read aloud as a verbal inquiry.


SQL Conflates Filters & Links
-----------------------------

    *"For each department, return the department's name and number of
    courses having more than 2 credit hours."*

This business inquiry returns *department* records, and for each record
summarizes associated courses meeting a particular criteria.

.. sourcecode:: sql

    SELECT d.name, COUNT(SELECT TRUE FROM ad.course AS c
                         WHERE c.department_code = d.code
                           AND c.credits > 2)
    FROM ad.department AS d;

For this SQL encoding, the ``WHERE`` clause of the subquery conflates
the linking of ``course`` to ``department`` with the filter criteria.

.. sourcecode:: sql

    SELECT d.name, COUNT(c)
    FROM ad.department AS d
    LEFT JOIN ad.course AS c
           ON (c.department_code = d.code
               AND c.credits > 2)
    GROUP BY d.name;

In a common optimization, the correlated subquery is replaced with a
``GROUP BY`` projection.  This encoding further obfuscates the business
inquiry by conflating in two ways --- row/column and link/filter.

.. htsql::
   :cut: 4
   :hide:

    /department{name, count(course?credits>2)}

The HTSQL translation keeps the filter criteria separate from linking
and the row definition separate from output columns.  The query adheres
the form of the original business inquiry.


Conflating Projection with Aggregation
--------------------------------------

    *"How many departments by campus?"*

This business inquiry asks for rows corresponding to each campus, and
for each row, the number of correlated departments.  In the schema,
there isn't a ``campus`` table, so we have to take *distinct* values of
``campus`` column from the ``school`` table.  This operation is called
*projection*.

.. sourcecode:: sql

    SELECT s.campus, COUNT(d)
    FROM ad.school AS s
    LEFT JOIN ad.department AS d
      ON (s.code = d.school_code)
    WHERE s.campus IS NOT NULL
    GROUP BY s.campus;

For this SQL encoding, the ``GROUP BY`` clause combines two operations:
projection and evaluating the aggregate ``COUNT()``.  This conflation
causes a reader of the query some effort determining what sort of rows
are returned and how the aggregate is related to those rows.

.. htsql::
   :cut: 4
   :hide:

    /school^campus {campus, count(school.department)}

In the HTSQL query, we start with an explicit projection (the ``^``
operator), then we select correlated columns.  This way, the aggregation
is indicated separately as part of the column selector rather than being
conflated with the row definition.


SQL Lacks Means of Encapsulation
--------------------------------

    *"For each department, return the department name and the number of
    offered 100's, 200's, 300's and 400's courses."*

In this business inquiry, we are asked to evaluate the same statistic 
across multiple ranges.

.. sourcecode:: sql

    SELECT d.name,
           COUNT(CASE WHEN c.no BETWEEN 100 AND 199 THEN TRUE END),
           COUNT(CASE WHEN c.no BETWEEN 200 AND 299 THEN TRUE END),
           COUNT(CASE WHEN c.no BETWEEN 300 AND 399 THEN TRUE END),
           COUNT(CASE WHEN c.no BETWEEN 400 AND 499 THEN TRUE END)
    FROM ad.department AS d
    LEFT JOIN ad.course AS c
           ON (c.department_code = d.code)
    GROUP BY d.name;

This query is tedious to write and error prone to maintain since SQL
provides no way to factor the repetitive expression ``COUNT(...)``.

.. htsql::
   :cut: 4
   :hide:

    /department.define(
         count_courses($level) := count(course?no>=$level*100
                                              &no<($level+1)*100))
      {name, count_courses(1),
             count_courses(2),
             count_courses(3),
             count_courses(4)}

The HTSQL translation avoids this duplication by defining a calculated
attribute ``count_courses($level)`` on the ``department`` table and
then evaluating it for each course level.


In SQL, Modest Complexity is Painful
------------------------------------

    *"For each school with a degree program, return the school's name,
    and the average number of high-credit (>3) courses its departments
    have."*

This business inquiry asks us to do the following:

* pick records from the ``school`` table

* keep only those with an associated degree program

* for each school record, compute average of:

  - for each associated department, count:

    - associated courses with credits>3


.. sourcecode:: sql

    SELECT s.name, o.avg_over_3
    FROM ad.school AS s
    JOIN ad.program AS p ON (p.school_code = s.code)
    LEFT JOIN (
        SELECT d.school_code, AVG(COALESCE(i.over_3,0)) AS avg_over_3
        FROM ad.department d
        LEFT JOIN (
            SELECT c.department_code, COUNT(c) AS over_3
            FROM ad.course AS c WHERE c.credits > 3
            GROUP BY c.department_code
        ) AS i ON (i.department_code = d.code)
        GROUP BY d.school_code
    ) AS o ON (o.school_code = s.code)
    GROUP BY s.name, o.avg_over_3;


Not only is this SQL encoding is hard to read, it took several passes to
get right --- without the ``COALESCE`` you get results that look
correct, but aren't.

.. htsql::
   :cut: 4
   :hide:

     /school?exists(program)
       {name, avg(department.count(course?credits>3))}

Each syntactic component of the HTSQL query is self-contained; when
assembled, they form a cohesive translation of the business inquiry.


HTSQL in a Nutshell
===================

HTSQL was designed from the ground up as a self-serve reporting tool
for data analysts.  With HTSQL, the easy stuff is truly easy; and,
the complex stuff is easy too.

In this section we introduce the fundamentals of HTSQL syntax and
semantics.  For a more incremental approach, please read the
:doc:`tutorial`.


Scalar Expressions
------------------

Literal values:

.. htsql:: /{3.14159, 'Hello World!'}

Algebraic expressions:

.. htsql:: /(3+4)*6

Predicate expressions:

.. htsql:: /(7<13)&(1=0|1!=0)


Navigation
----------

A table name by itself produces all records from that table:

.. htsql:: /school
   :cut: 4

In the scope of ``school`` table, ``department`` is a link to
associated records from ``department`` table.  The following query
returns ``department`` records via navigation though ``school``:

.. htsql:: /school.department
   :cut: 4

This query works as follows:

* ``school`` generates all records from ``school`` table;
* for each ``school`` record, ``department`` generates
  associated ``department`` records;


Filtering
---------

Sieve operator produces records satisfying the specified condition:

.. htsql:: /school?campus='south'

Sorting operator reorders records:

.. htsql:: /school.sort(campus)
   :cut: 4

Truncating operator takes a slice from the record sequence:

.. htsql:: /school.limit(2)


Selection & Definition
----------------------

Selection specifies output columns:

.. htsql:: /school{name, campus}
   :cut: 4

Title decorator defines the title of an output column:

.. htsql:: /school{name, count(department) :as '# of Dept'}
   :cut: 4

Calculated attributes factor out repeating expressions:

.. htsql::

   /school.define(num_dept := count(department))
          {code, num_dept}?num_dept>3

References carry over values across nested scopes:

.. htsql::
   :cut: 4

   /define($avg_credits := avg(course.credits))
    .course{title, credits}?credits>$avg_credits


Aggregation
-----------

Aggregates convert plural expressions to singular values.

Scalar aggregates:

.. htsql:: /count(department)

Nested aggregates:

.. htsql:: /avg(school.count(department))

Various aggregation operations:

.. htsql::
   :cut: 4

   /department{name, count(course),
                     max(course.credits),
                     sum(course.credits),
                     avg(course.credits)}?exists(course)


Projection
----------

Projection operator returns distinct values.  This example returns
distinct ``campus`` values from the ``school`` table:

.. htsql:: /school^campus

In the scope of the projection, ``school`` refers to all records from
``school`` table having the same value of ``campus`` attribute:

.. htsql:: /school^campus {campus, count(school)}


Linking
-------

Even though HTSQL provides automatic links inferred from foreign key
constraints, arbitrary linking is also allowed:

.. htsql:: /school.({code} -> department{school_code})
   :cut: 4

This query uses a linking operator to replicate an automatic link:

.. htsql:: /school.department
   :cut: 4
   :hide:

Forking operator links a table to itself by the given expression:

.. htsql::

   /school{name, campus}
          ?count(department)>avg(fork(campus).count(department))

This query returns schools with the number of departments above average
among all schools in the same campus.  Using a linking operator, this
query could be written as:

.. htsql::
   :hide:

   /school{name, campus}
          ?count(department)>avg((campus -> school).count(department))


What's up Next?
===============

We intend to add to HTSQL many more features in the future.

Usability
---------

Currently, the HTSQL processor is not quite user friendly.  In the next
major release we will focus on filling these gaps:

* helpful error messages 
* ability to list tables & columns
* syntax highlighting & completion
* installers & deployment documentation 

Hierarchical Output
-------------------

HTSQL should not be limited to tabular output.

.. sourcecode:: htsql

   /school{name,
           /program{title},
           /department{name}}

This query is to generate a tree-shaped output: for each school, it
produces the school name, a list of titles of associated programs, 
and a list of names of associated departments.

Analytical Processing
---------------------

HTSQL should support OLAP cube operations.

.. sourcecode:: htsql

   /rollup(school^campus){campus, count(school.department)}

This query is to produce the number of departments per school's campus
followed by a total value for all campuses.

Recursive Queries
-----------------

HTSQL should be able to construct hierarchies from parent-child
relationships.

.. sourcecode:: htsql

   /program{title, /recurse(part_of){title}}

This query is to return programs together with a list of all
dependent subprograms.

