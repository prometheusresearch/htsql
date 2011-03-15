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

HTSQL is a query language for the web.  Queries are URLs_ that can be
directly typed into a browser.

.. _REST: http://en.wikipedia.org/wiki/Representational_State_Transfer
.. _HTTP: http://www.w3.org/Protocols/rfc2616/rfc2616.html
.. _Accept: http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html
.. _URLs: http://www.ietf.org/rfc/rfc3986.txt

HTSQL is a Relational Database Gateway
--------------------------------------

.. vsplit::

   .. sourcecode:: htsql

      /school

   .. sourcecode:: sql

      SELECT "school"."code",
             "school"."name"
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

      SELECT "school"."name",
             COALESCE("program"."count", 0),
             COALESCE("department"."count", 0)
      FROM "ad"."school" AS "school"
      LEFT OUTER JOIN (
        SELECT COUNT(TRUE) AS "count",
               "program"."school"
        FROM "ad"."program" AS "program"
        GROUP BY 2
      ) AS "program"
      ON ("school"."code" = "program"."school")
      LEFT OUTER JOIN (
        SELECT COUNT(TRUE) AS "count",
               "department"."school"
        FROM "ad"."department" AS "department"
        GROUP BY 2
      ) AS "department"
      ON ("school"."code" = "department"."school")
      ORDER BY "school"."code" ASC

HTSQL is a compact, high-level query language.  Often times,
short HTSQL queries are equivalent to much more complex SQL.


How do I use HTSQL?
===================

HTSQL can be used with any number of higher-level tools.

HTSQL Makes Dashboarding Easy
-----------------------------

.. vsplit::

   .. sourcecode:: html

      <body>
      <h3>Select a School</h3>
      <select id="school" 
        data-htsql="/school{code, name}"></select>
      <div style="width: 500px; height: 350px;"
        data-htsql="/program{title, count(student)}
                    ?school=$school&count(student)>0"
        data-type="pie"
        data-widget="chart"
        data-title="Percent of Students by Program"></div>
      <h3>Departments</h3>
      <p>Filter by name: <input id="department_name"/></p>
      <table id="department" data-hide-column-0="yes"
        data-htsql="/department{code, name, school.name}
                    ?school=$school&name~$department_name">
      </table>
      <p>
        The selected department:
        <em data-htsql="/department{name}?code=$department"></em> <br/>
        The number of courses in the selected department:
        <strong data-htsql="/department{count(course)}
                            ?code=$department"></strong>
      </p>
      <h3>Courses</h3>
      <table id="course" 
        data-htsql="/course?department=$department">
      </table>
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
to construct a navigational graph of your database.  For example::

         +------------+               +------------+
    /---+| DEPARTMENT |>-------------o|   SCHOOL   |+---\
    |.   +------------+        .      +------------+   .|
    |  .                     .                       .  |
    |   department       department        school may   |
    |   offers           may be part       offer some   |
    |   courses          of school         programs     |
    |                                                   |
    |    +------------+               +------------+    |
    \---<|   COURSE   |               |  PROGRAM   |>---/
         +------------+               +------------+

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
           "department"."school"
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
      ON ("department"."school" = "school"."code")
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
             &department.school='eng'

   .. sourcecode:: sql

       SELECT "course"."department",
              "course"."no",
              "course"."title",
              "course"."credits",
              "course"."description"
       FROM "ad"."course" AS "course"
       INNER JOIN "ad"."department" AS "department"
       ON ("course"."department" = "department"."code")
       WHERE ("course"."credits" > 3)
         AND ("department"."school" = 'eng')
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

      SELECT "course"."department",
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
               "department"."school"
        FROM "ad"."department" AS "department"
        LEFT OUTER JOIN (
          SELECT COUNT(TRUE) AS "count",
                 "course"."department"
          FROM "ad"."course" AS "course"
          GROUP BY 2
        ) AS "course"
        ON ("department"."code" = "course"."department")
        GROUP BY 2
      ) AS "department"
      ON ("school"."code" = "department"."school")
      WHERE EXISTS(
        SELECT TRUE
        FROM "ad"."program" AS "program"
        WHERE ("school"."code" = "program"."school")
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

HTSQL supports complex grouping operations.

.. vsplit::

   .. sourcecode:: htsql

      /program{^degree, count(^)}

   .. sourcecode:: sql

      SELECT degree, COUNT(TRUE)
      FROM ad.program
      GROUP BY 1
      ORDER BY 1;

`This query`__ returns the number of programs per degree.

__ http://demo.htsql.org
        /(program^degree){*,count(^)}

Hierarchical Output
-------------------

HTSQL is not limited to tabular output.

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

`This query`__ returns programs and departments
in each school.

__ http://demo.htsql.com
        /school{name}/(program{title};department{name})

More Backends
-------------

The current release of HTSQL supports PostgreSQL and SQLite.
Subsequent releases will add support for MySQL, Oracle and
Microsoft SQL Server.

The challenge here is providing consistent function definitions
and semantics that work across various SQL database systems.

