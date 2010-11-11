============
  Examples
============

HTSQL provides outstanding clarity without sacrificing rigor.  Not only
is working with HTSQL more productive than SQL, but things are possible
that may have otherwise exceeded a user's mental capacity. 

Let's assume we have a data model, with schools, departments, programs
and courses.  Here it is::

         +-------------+       +--------+
    /---m| DEPARTMENT  |>-----o| SCHOOL |m----\
    |.   +-------------+  .    +--------+    .|
    | .                  .                  . |
    |   department   department    a school   |
    |   offers       may be part   has one or |
    |   courses      of school     programs   |
    |                                         |
    |    +-------------+       +---------+    |
    \---<| COURSE      |       | PROGRAM |>---/
         +-------------+       +---------+


List all schools
----------------

An **HTSQL** query::

    /school

An equivalent **SQL** query::

    SELECT code, name
    FROM ad.school
    ORDER BY code;


Programs ordered by the title
-----------------------------

**HTSQL**::

    /program{title+}

**SQL**::

    SELECT title
    FROM ad.program
    ORDER BY title, school, code;


All courses missing a description
---------------------------------

**HTSQL**::

    /course?!description

**SQL**::

    SELECT department, number, title, credits, description
    FROM ad.course
    WHERE NULLIF(description, '') IS NULL
    ORDER BY 1, 2;


Departments in schools having "art" in its name
-----------------------------------------------

**HTSQL**::

    /department?school.name~'art'

**SQL**::

    SELECT d.code, d.name, d.school
    FROM ad.department AS d
    LEFT OUTER JOIN
         ad.school AS s ON (d.school = s.code)
    WHERE s.name ILIKE '%art%'
    ORDER BY 1;


The number of schools
---------------------

**HTSQL**::

    /count(school)

**SQL**::

    SELECT COUNT(TRUE)
    FROM ad.school;


Schools with programs
---------------------

**HTSQL**::

    /school?exists(program)

**SQL**::

    SELECT s.code, s.name
    FROM ad.school AS s
    WHERE EXISTS(SELECT TRUE
                 FROM ad.program AS p
                 WHERE s.code = p.school)
    ORDER BY 1;


The number of schools with programs
-----------------------------------

**HTSQL**::

    /count(school?exists(program))

**SQL**::

    SELECT COUNT(TRUE)
    FROM ad.school AS s
    WHERE EXISTS(SELECT TRUE
                 FROM ad.program AS p
                 WHERE (s.code = p.school));


Number of programs and departments per school
---------------------------------------------

**HTSQL**::

    /school{name, count(program), count(department)}

**SQL**::

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


Average number of courses offered by departments in each school
---------------------------------------------------------------

**HTSQL**::

    /school{name, avg(department.count(course))}

**SQL**::

    SELECT s.name, d.av
    FROM ad.school AS s
    LEFT OUTER JOIN
         (SELECT AVG(CAST(COALESCE(c.cnt, 0) AS NUMERIC)) AS av, d.school
          FROM ad.department AS d
          LEFT OUTER JOIN
               (SELECT COUNT(TRUE) AS cnt, c.department
               FROM ad.course AS c
               GROUP BY 2) AS c ON (d.code = c.department)
          GROUP BY 2) AS d ON (s.code = d.school)
    ORDER BY s.code;


.. warning::

   The following examples do not work in 2.0; will be available in 2.1+.


*Programs and departments in each school*
-----------------------------------------

**HTSQL**::

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


*The number of programs per degree*
-----------------------------------

**HTSQL**::

    /program{degree^, count()}

**SQL**::

    SELECT degree, COUNT(TRUE)
    FROM ad.program
    GROUP BY 1
    ORDER BY 1;


*The number&list of schools and the top 2 departments by the number of programs*
--------------------------------------------------------------------------------

**HTSQL**::

    /school{count(program)^, count(), /name, /department.top(2)}

**SQL**: *Ahhh!*


