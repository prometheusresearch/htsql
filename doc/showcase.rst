*******************
  HTSQL Showcase
*******************

What is HTSQL?
==============

HTSQL is a query language and web service for relational databases.

HTSQL is a Web Service
----------------------

.. container:: vsplit

   .. sourcecode:: htsql

      /school

   .. image:: img/show_school.png
      :alt: output of /school query
      :target: http://demo.htsql.org/school

HTSQL is a REST_ query language for the web.  Queries are URLs_ that can
be directly typed into a browser.  The default output format of the
HTTP_ request depends upon the user-agent and its Accept_ header.

.. _REST: http://en.wikipedia.org/wiki/Representational_State_Transfer
.. _HTTP: http://www.w3.org/Protocols/rfc2616/rfc2616.html
.. _Accept: http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html
.. _URLs: http://www.ietf.org/rfc/rfc3986.txt

HTSQL is a Relational Database Gateway
--------------------------------------

.. container:: vsplit

   .. sourcecode:: htsql

      /school

   .. sourcecode:: sql

      SELECT code, name
      FROM ad.school
      ORDER BY code;

HTSQL wraps SQL databases.  On startup HTSQL introspects structure of
the database.  At runtime, each request is then translated into SQL and
executed.

HTSQL is an Advanced Query Language
-----------------------------------

.. container:: vsplit

   .. sourcecode:: htsql

      /school{name, count(program), 
         count(department)}

   .. sourcecode:: sql

      SELECT s.name, COALESCE(p.cnt, 0), COALESCE(d.cnt, 0)
      FROM ad.school AS s
      LEFT OUTER JOIN
           (SELECT COUNT(TRUE) AS cnt, p.school
            FROM ad.program AS p
            GROUP BY 2) AS p ON (s.code = p.school)
      LEFT OUTER JOIN
           (SELECT COUNT(TRUE) AS cnt, d.school
            FROM ad.department AS d
            GROUP BY 2) AS d ON (s.code = d.school)
      ORDER BY s.code;

HTSQL is a high-level query language that compiles into SQL as if it
were a database assembler.

HTSQL is a Communication Tool
-----------------------------

HTSQL reduces the number of meetings in your organization. The
following may happen to you as well::

   From: "Anne (data analyst)" <ann@example.com>
   To: "Dave (product manager)" <dave@example.com> 
   Cc: jess@example.com, tim@example.com, jose@example.com
   Subject: do we need to meet?
  
   Does the HTSQL below return, for each school, the average 
   number of courses offered in its departments? If so, then 
   Tim and I don't need to meet with you tomorrow.

   http://demo.htsql.org
   /school{name,avg(department.count(course))}

   - A

HTSQL is a common language usable by software developers, data analysts,
database administrators, and even business users.


Show me HTSQL
=============

HTSQL was designed from the ground up as a self-serve reporting tool for
data analysts.  With HTSQL the easy stuff is truly easy; oh, yea, and
complex stuff is easy too.

For the examples below, the following "university catalog" schema is
used.  It has two top-level tables, ``school`` and ``department``; where
department has an optional link to school.  Subordinate tables, having
mandatory foreign key references are ``course`` and ``program``::

         +-------------+              +--------+
    /---m| DEPARTMENT  |>-------------| SCHOOL |m----\
    |.   +-------------+       .      +--------+    .|
    | .                       .                    . |
    |  .                     .                    .  |
    |   department       department      a school    |
    |   offers           may be part     has one or  |
    |   courses          of school       programs    |
    |                                                |
    |                                                |
    |    +-------------+              +---------+    |
    \---<| COURSE      |              | PROGRAM |>---/
         +-------------+              +---------+


Choosing a Table
----------------

HTSQL queries typically start with the driving table.

.. container:: vsplit

   .. sourcecode:: htsql

      /department

   .. sourcecode:: sql

    SELECT "department"."code",
           "department"."name",
           "department"."school"
    FROM "ad"."department" AS "department"
    ORDER BY 1 ASC

This query (Q1_) all departments.

.. _Q1: http://demo.htsql.org/department

Selecting Columns
-----------------

Output columns are selected with curly brackets ``{}``; the ``:as``
decorator sets the title.  

.. container:: vsplit

   .. sourcecode:: htsql

      /department{school.name :as 'School', 
                  name :as 'Department'}

   .. sourcecode:: sql

      SELECT "school"."name" AS "School",
             "department"."name" AS "Department"
      FROM "ad"."department" AS "department"
           LEFT OUTER JOIN "ad"."school" AS "school"
           ON ("department"."school" = "school"."code")
      ORDER BY "department"."code" ASC

This query (Q2_) returns, for each department, the name of the
associated school and the name of the department.

.. _Q2: 
     http://demo.htsql.org
     /department{school.name :as 'School', name :as 'Department'}

Filtering Rows
--------------

HTSQL lets you filter results with arbitrary predicates.

.. container:: vsplit

   .. sourcecode:: htsql

      /course?credits>3
       &department.school='egn'

   .. sourcecode:: sql

       SELECT "course"."department",
              "course"."number",
              "course"."title",
              "course"."credits",
              "course"."description"
       FROM "ad"."course" AS "course"
            INNER JOIN "ad"."department" AS "department"
            ON ("course"."department" = "department"."code")
       WHERE ("course"."credits" > 3)
         AND ("department"."school" = 'egn')
       ORDER BY 1 ASC, 2 ASC

This query (Q3_) returns courses from the school of 
engineering having more than 3 credits.

.. _Q3: 
     http://demo.htsql.org
     /course?department.school='egn'&credits>3

Paging and Sorting
------------------

HTSQL has a composable table expression mechanism for things like
sorting and paging.

.. container:: vsplit

   .. sourcecode:: htsql

      /course.sort(credits)
             .limit(10,20)

   .. sourcecode:: sql

      SELECT "course"."department",
             "course"."number",
             "course"."title",
             "course"."credits",
             "course"."description"
      FROM "ad"."course" AS "course"
      ORDER BY 4 ASC NULLS FIRST, 1 ASC, 2 ASC
      LIMIT 10 OFFSET 20

This query (Q4_) returns page 3 of the course catalog as
sorted by number of credits.

.. _Q4: 
     http://demo.htsql.org
     /course.sort(credits).limit(10,20)

Aggregating Data
----------------

In HTSQL, aggregates aren't a reason to run to the DBA.

.. container:: vsplit

   .. sourcecode:: htsql

      /school{name,
          avg(department.count(course))
      }?exists(program.degree='ms')

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

This query (Q5_) returns, for each school having a 
MS program, the average number of courses offered 
in its departments.

.. _Q5: 
     http://demo.htsql.org
     /school{name,avg(department.count(course))}?
          exists(program.degree='ms')


How do I use HTSQL?
===================

HTSQL is a tool that can be used with any number of higher-level
tools.

HTSQL Makes Dashboarding Easy
-----------------------------

.. image:: img/htraf_screenshot.png
   :alt: The HTRAF demo
   :align: right
   :target: http://htraf.htsql.org/

.. sourcecode:: html

    <body>
        <h3>Select School</h3>
        <select id="school"
                data-source="/school{code, name}"/>
        <div class="chart"
             data-source="/program{title, count(student)}
                          ?school=$school&count(student)>0" 
             data-display="chart"
             data-chart-title="Percent of Students by Program"/>

        <h3>Departments</h3>
        <p>Filter by name: <input id="department_name"/></p>
        <table id="department"
               data-hide-first-column="yes" 
               data-source="/department{code, name, school.name}
                            ?school=$school&name~$department_name"/>
        <p>
            The selected department: 
            <em data-source="/department{name}?code=$department"/>
            <br/>
            The number of courses in selected department:
            <strong data-source="/department{count(course)}
                                 ?code=$department"/>
        </p>

        <h3>Courses</h3>
        <table id="course" 
               data-source="/course?department=$department"/>
    </body>

The dashboard above (using the JQuery HTRAF toolkit) shows a 3-level
drill down (``school``, ``department`` and ``course``) for a university
schema.  The live demo for this dashboard is at http://htraf.htsql.org/. 


What's up Next?
===============


*Programs and departments in each school*
-----------------------------------------

**HTSQL** (C1_, using 1.0, different syntax)::

    /school{name, /program{title}, /department{name}}

This query produces a *tree* output::

    [
     ["School of Art and Design",
      ["Post Baccalaureate in Art History", ...],
      ["Art History", "Studio Art"]],
     ["School of Business",
      ["Graduate Certificate in Accounting", ...],
      ["Accounting", "Capital Markets", "Corporate Finance"]],
     ...
    ]

You need at least 3 **SQL** statements to produce the same result::

    SELECT name, code
    FROM ad.school
    ORDER BY code;

    SELECT s.code, p.title
    FROM ad.school AS s
    INNER JOIN
         ad.program AS p ON (s.code = p.school)
    ORDER BY s.code, p.code;

    SELECT s.code, d.name
    FROM ad.school AS s
    INNER JOIN
         ad.department AS d ON (s.code = d.school)
    ORDER BY s.code, d.code;

.. _C1: http://demo.htsql.com/school{name}/(program{title};department{name})


*The number of programs per degree*
-----------------------------------

**HTSQL** (C2_, using 1.0)::

    /program{degree^, count()}

**SQL**::

    SELECT degree, COUNT(TRUE)
    FROM ad.program
    GROUP BY 1
    ORDER BY 1;

.. _C2: http://demo.htsql.com/program{degree^,count()}


*The number&list of schools and the top 2 departments by the number of programs*
--------------------------------------------------------------------------------

**HTSQL** (C3_, using 1.0, different syntax, w/o ``top()``)::

    /school{count(program)^, count(), /name, /department.top(2)}

**SQL**: *Ahhh!*

.. _C3: http://demo.htsql.com/school{count(program)^,count()}/({name};department)


