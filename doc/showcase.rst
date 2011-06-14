******************
  HTSQL Showcase
******************

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
directly typed into a browser.  


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

HTSQL wraps an existing relational database.  Queries are translated
into SQL.

HTSQL is an Advanced Query Language
-----------------------------------

.. vsplit::

   .. sourcecode:: htsql

      /school{name, count(program),
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

Relational algebra has proven to be inadequate for encoding business
inquiries -- elementary set operations simply do not correspond to
meaningful data transformations.  The SQL language itself is tedious,
verbose, and provides poor means of abstraction.  Yet, the relational
database has proven to be an excellent tool for data modeling, storage
and retrival.

HTSQL reimagines what it means to query a database.  The combination of
a *navigational model* with *data flows* enables expressions that
naturally reflect business inquiries.  The HTSQL translator uses SQL as
a target assembly language, which allows us to fix the query model and
language, while keeping current investment in relational systems.

To demonstrate this point, we walk through a set of business inquires
expressed over a fictitious university schema.

.. diagram:: dia/administrative-directory-small-schema.tex
   :align: center

This data model has two top-level tables, ``school`` and ``department``,
where ``department`` has an optional link to ``school``.  Subordinate
tables, ``course`` and ``program``, have mandatory links to their parents.

SQL conflates Rows & Columns
----------------------------

  "Please list departments; for each department,
  show the corresponding school's campus." 

.. sourcecode:: sql

     SELECT d.code, d.name, s.campus
     FROM ad.department AS d
     LEFT JOIN ad.school AS s
            ON (d.school_code = s.code);

The business inquiry asks for a specific set of rows, and then
correlated columns.  The SQL encoding returns a subset of a cross
product making it difficult to ensure what each row represents. 
The ``FROM`` clause doesn't just pick rows, it also plays and auxiliary
role in choosing columns.

.. sourcecode:: htsql

    /department{code, name, school.campus}

The navigational translation separates the row definition from the
column selection.  The linking is implicit, and correct.  The encoded
query can be read aloud as a verbal inquiry.


SQL Conflates Filters & Links
-----------------------------

  "For each department, return the department's
  name and number of courses having more than
  3 credit hours."

.. sourcecode:: sql

     SELECT d.name, COUNT(SELECT TRUE FROM ad.course AS c
                          WHERE c.department_code = d.code
                            AND c.credits > 3  )
     FROM ad.department AS d;

For the SQL encoding of this inquiry we use a subquery to avoid row and
column conflation.  However, ``WHERE`` clause in the subquery conflates
logic filter with the glue linking department and course.

.. sourcecode:: sql

     SELECT d.name, count(c)
     FROM ad.department AS d
     LEFT JOIN ad.course AS c
            ON (c.department_code = d.code
                AND c.credits > 3)
     GROUP BY d.name;

In a common optimization of this query, we replace the correlated
subquery with a ``GROUP BY`` projection.  This gives us both row/column
and link/filter conflation, further obfuscating the business inquiry.

.. sourcecode:: htsql

     /department{name, count(course?credits>3)}

The navigational translation keeps the business logic separate from the
link and the row definition separate from output columns.  The encoded
query corresponds to the original inquiry.


Conflating Projection with Aggregation
--------------------------------------

  "How many departments by campus?"

.. sourcecode:: sql

   SELECT s.campus, COUNT(d)
   FROM ad.school AS s 
   LEFT JOIN ad.department AS d
     ON (s.code = d.school_code)
   WHERE s.campus IS NOT NULL
   GROUP by s.campus;

In the schema there isn't a ``campus`` table, you have to take
*distinct* values from the school table.  In this SQL query its not
clear if the ``GROUP BY`` is used only to produce an aggregate, you have
to examine primary key columns to know for sure.

.. sourcecode:: htsql

   /(school^campus) {campus, count(school.department)}

In a navigational approach, you first construct the projection
explicitly (using ``^`` operator).  Then, you select from it. 
In this way the aggregation is indicated separately as part of the
column selector rather than being confused with the row definition.


For SQL, Modest Complexity is Painful
-------------------------------------

  "For each school with a degree program, return 
  the school's name, and the average number of 
  high-credit (>3) courses its departments have."

.. sourcecode:: sql

   SELECT s.name, o.avg_over_3 FROM ad.school AS s
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

Not only is this query hard to read, it took several passes to get
correct -- without the ``COALESCE`` you get results that look correct,
but arn't.

.. sourcecode:: htsql

     /(school?exists(program))
       {name, avg(department.count(course?credits>3))} 


How do I use HTSQL?
===================

HTSQL can be used with any number of higher-level tools.

HTSQL is a Reporting Toolkit
----------------------------

.. vsplit::

   .. sourcecode:: html

      <body>
      <h3>Select a School</h3>
      <select id="school" 
        data-htsql="/school{code, name}"></select>
      <div style="width: 500px; height: 350px;"
        data-htsql="/program{title, count(student)}
                    ?school_code=$school&count(student)>0"
        data-ref="school" data-type="pie" data-widget="chart"
        data-title="Percent of Students by Program"></div>
      <h3>Departments</h3>
      <p>Filter by name: <input id="department_name"/></p>
      <table id="department" data-hide-column-0="yes"
        data-htsql="/department{code, name, school.name}
                    ?school_code=$school&name~$department_name"
        data-ref="school department_name"></table>
      <p>
        The selected department:
        <em data-htsql="/department{name}?code=$department"
            data-ref="department"></em> <br/>
        The number of courses in the selected department:
        <strong data-htsql="/department{count(course)}
                            ?code=$department"
                data-ref="department"></strong>
      </p>
      <h3>Courses</h3>
      <table id="course" 
        data-htsql="/course?department_code=$department"
        data-ref="department"></table>
      </body>

   .. image:: img/htraf_screenshot.png
      :alt: The HTRAF demo
      :target: http://htraf.htsql.org/

The dashboard above (using the JQuery-based HTRAF toolkit) shows a 3-level
drill down (``school``, ``department`` and ``course``) for a university
schema.  The live demo for this dashboard is at http://htraf.htsql.org/.


HTSQL is a Communication Tool
-----------------------------

HTSQL reduces the number of meetings in your organization::

   From: "Anne (data analyst)" <ann@example.com>
   To: "Dave (product manager)" <dave@example.com>
   Cc: jess@example.com, tim@example.com, jose@example.com
   Subject: do we need to meet?

   Does the HTSQL below return, for each school, the average
   number of courses offered in its departments? If so, then
   Tim and I don't need to meet with you tomorrow.

   http://demo.htsql.org/school{name,avg(department.count(course))}

   - A

HTSQL is a common language usable by software developers, data analysts,
database administrators, and even business users.


Show me this HTSQL!
===================

HTSQL was designed from the ground up as a self-serve reporting tool
for data analysts.  With HTSQL, the easy stuff is truly easy; and,
the complex stuff is easy too.

Database Introspection
----------------------

On startup, HTSQL examines tables, primary keys, and foreign keys
to construct a navigational graph of your database.  For example:

.. diagram:: dia/administrative-directory-small-schema.tex
   :align: center

This university schema is used in the examples below.  The data model
has two top-level tables, ``school`` and ``department``, where
``department`` has an optional link to ``school``.  Subordinate tables,
having mandatory foreign key references, are ``course`` and ``program``.

Choosing a Table
----------------

HTSQL queries typically start with a table.

.. vsplit::

   .. sourcecode:: htsql

      /department

   .. sourcecode:: sql

    SELECT "department"."code",
           "department"."name",
           "department"."school_code"
    FROM "ad"."department" AS "department"
    ORDER BY 1 ASC

`This query`__ returns all departments.

__ http://demo.htsql.org/department

Selecting Columns
-----------------

Output columns are selected with curly brackets ``{}``; the ``:as``
decorator sets the title.

.. vsplit::

   .. sourcecode:: htsql

      /department{school.name, name}

   .. sourcecode:: sql

      SELECT "school"."name",
             "department"."name"
      FROM "ad"."department" AS "department"
      LEFT OUTER JOIN "ad"."school" AS "school"
      ON ("department"."school_code" = "school"."code")
      ORDER BY "department"."code" ASC

`This query`__ returns, for each department, the name of the
associated school and the name of the department.

__ http://demo.htsql.org
        /department{school.name, name}

Filtering Rows
--------------

HTSQL lets you filter results with arbitrary predicates.

.. vsplit::

   .. sourcecode:: htsql

      /course?credits>3
             &department.school.code='eng'

   .. sourcecode:: sql

      SELECT "course"."department_code",
             "course"."no",
             "course"."title",
             "course"."credits",
             "course"."description"
      FROM "ad"."course" AS "course"
      INNER JOIN "ad"."department" AS "department"
      ON ("course"."department_code" = "department"."code")
      LEFT OUTER JOIN "ad"."school" AS "school"
      ON ("department"."school_code" = "school"."code")
      WHERE ("course"."credits" > 3)
        AND ("school"."code" = 'eng')
      ORDER BY 1 ASC, 2 ASC

`This query`__ returns courses from the school of
engineering having more than 3 credits.

__ http://demo.htsql.org
        /course?department.school='eng'&credits>3

Paging and Sorting
------------------

Table operations such as sorting and paging could be freely combined.

.. vsplit::

   .. sourcecode:: htsql

      /course.sort(credits).limit(10,20)

   .. sourcecode:: sql

      SELECT "course"."department_code",
             "course"."no",
             "course"."title",
             "course"."credits",
             "course"."description"
      FROM "ad"."course" AS "course"
      ORDER BY 4 ASC NULLS FIRST, 1 ASC, 2 ASC
      LIMIT 10 OFFSET 20

`This query`__ returns courses 21 to 30 in the course
catalog as sorted by number of credits.

__ http://demo.htsql.org
        /course.sort(credits).limit(10,20)

Aggregating Data
----------------

In HTSQL, aggregates aren't a reason to run to the DBA.

.. vsplit::

   .. sourcecode:: htsql

      /school{name,
              avg(department.count(course))}
             ?exists(program.degree='ms')

   .. sourcecode:: sql

      SELECT "school"."name",
             "department"."avg"
      FROM "ad"."school" AS "school"
      LEFT OUTER JOIN (
        SELECT AVG(CAST(COALESCE("course"."count", 0)
                        AS NUMERIC)) AS "avg",
               "department"."school_code"
        FROM "ad"."department" AS "department"
        LEFT OUTER JOIN (
          SELECT COUNT(TRUE) AS "count",
                 "course"."department_code"
          FROM "ad"."course" AS "course"
          GROUP BY 2
        ) AS "course"
        ON ("department"."code" = "course"."department_code")
        GROUP BY 2
      ) AS "department"
      ON ("school"."code" = "department"."school_code")
      WHERE EXISTS(
        SELECT TRUE
        FROM "ad"."program" AS "program"
        WHERE ("school"."code" = "program"."school_code")
          AND ("program"."degree" = 'ms')
      )
      ORDER BY "school"."code" ASC

`This query`__ returns, for each school having a
MS program, the average number of courses offered
across its departments.

__ http://demo.htsql.org
        /school{name,avg(department.count(course))}
                ?exists(program.degree='ms')


What's up Next?
===============

Over the next few months we'll be adding more features (some
of them are already implemented in our internal 1.X branch).

Projections
-----------

HTSQL will support complex grouping operations.

.. vsplit::

   .. sourcecode:: htsql

      /(program^degree){degree,
                        count(program)}

   .. sourcecode:: sql

      SELECT degree, COUNT(TRUE)
      FROM ad.program
      WHERE degree IS NOT NULL
      GROUP BY 1
      ORDER BY 1;

`This query`__ returns the number of programs per degree.

__ http://demo.htsql.org
        /(program^degree){degree,count(program)}

Hierarchical Output
-------------------

HTSQL is not to be limited to tabular output.

.. vsplit::

   .. sourcecode:: htsql

      /school{name,
              /program{title},
              /department{name}}

   .. sourcecode:: sql

      SELECT name, code
      FROM ad.school
      ORDER BY code;

      SELECT s.code, p.title
      FROM ad.school AS s
      INNER JOIN ad.program AS p
      ON (s.code = p.school)
      ORDER BY s.code, p.code;

      SELECT s.code, d.name
      FROM ad.school AS s
      INNER JOIN ad.department
      AS d ON (s.code = d.school)
      ORDER BY s.code,d.code;

This query will return all schools with associated programs and
departments.


More Backends
-------------

The current release of HTSQL supports PostgreSQL and SQLite.
Subsequent releases will add support for MySQL, Oracle and
Microsoft SQL Server.

The challenge here is providing consistent function definitions
and semantics that work across various SQL database systems.

