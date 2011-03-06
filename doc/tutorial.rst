*************************
  Introduction to HTSQL
*************************

*A query language for the accidental programmer*

HTSQL makes accessing data as easy as browsing the web.  An HTSQL
processor translates web requests into relational database queries and
returns the results in a form ready for display or processing.
Information in a database can then be directly accessed from a browser,
a mobile application, statistics tool, or a rich Internet application.
Like any web resource, an HTSQL service can be secured via encryption
and standard authentication mechanisms -- either on your own private
intranet or publicly on the Internet.

HTSQL users are data experts.  They may be business users, but they can
also be technical users who value data transparency and direct access.
Business users can use HTSQL to quickly search the database and create
reports without the help of IT.  Programmers can use it as data access
layer for web applications.  HTSQL can be installed by DBAs to provide
easy, safe database access for power users.

HTSQL is a schema-driven URI-to-SQL translator that takes a request over
HTTP, converts it to a set of SQL queries, executes these queries in a
single transaction, and returns the results in a format (CSV, HTML,
JSON, etc.) requested by the user agent::

  /----------------\                   /------------------------\
  | USER AGENT     |                   |   HTSQL WEB SERVICE    |
  *----------------*  HTTP Request     *------------------------*
  |                | >---------------> -.                       |
  | * Web Browsers |  URI, headers,    | \      .---> Generated |
  |   HTML, TEXT   |  post/put body    |  v    /      SQL Query |
  |                |                   |  HTSQL          |      |
  | * Applications |                   |  PROCESSOR      v      |
  |   JSON, XML    |  HTTP Response    | /   ^.       SECURED   |
  |                | <---------------< -.      \      DATABASE  |
  | * Spreadsheets |  status, header,  |     Query       .      |
  |   CSV, XML     |  csv/html/json    |     Results <---/      |
  |                |  result body      |                        |
  \----------------/                   \------------------------/

The HTSQL query processor does heavy lifting for you.  Using
relationships between tables as permitted links, the HTSQL processor
translates graph-oriented web requests into corresponding relational
queries.  This translation can be especially involved for sophisticated
requests having projections and aggregates.  For complex cases, an
equivalent hand-written SQL query is tedious to write and non-obvious
without extensive training.  By doing graph to relational mapping on
your behalf, HTSQL permits your time to be spent exploring information
instead of debugging.

The HTSQL language is easy to use.  We've designed HTSQL to be broadly
usable by semi-technical domain experts, or what we call *accidental
programmers*.  We've field tested the toolset with business analysts,
medical researchers, statisticians, and web application developers. By
using a formalized directed graph as the underpinning of the query
algebra and by using a URI-inspired syntax over HTTP, we've obtained a
careful balance between clarity and functionality.

We hope you like it.


Getting Started
===============

The following examples show output from the HTSQL command-line system,
which is plain text.  HTSQL can output HTML, CSV, XML and many other
formats.  This makes it suitable not only for direct queries, but as a
data access layer for application development.

We'll use a fictional university that maintains a database for its
student enrollment system.  There are four tables that describe the
business units of the university and their relationship to the
courses offered::

  +--------------------+              +---------------------+
  | DEPARTMENT         |              | SCHOOL              |
  +--------------------+              +---------------------+
  | code            PK |--\       /--o| code             PK |----\
  | school          FK |>-|------/    | name          NN,UK |    |
  | name         NN,UK |  |    .      +---------------------+    |
  +--------------------+  |     .                              . |
                        . |  departments                      .  |
       a department    .  |  may belong                      .   |
       offers zero or .   |  to at most        a school          |
       more courses       |  one school        administers zero  |
                          |                    or more programs  |
  +--------------------+  |                                      |
  | COURSE             |  |           +---------------------+    |
  +--------------------+  |           | PROGRAM             |    |
  | department  FK,PK1 |>-/           +---------------------+    |
  | no             PK2 |              | school       PK1,FK |>---/
  | title           NN |              | code            PK2 |
  | credits         NN |              | title            NN |
  | description        |              | degree           CK |
  +--------------------+              +---------------------+

  PK - Primary Key   UK - Unique Key         FK - Foreign Key
  NN - Not Null      CK - Check Constraint

The university consists of schools, which administer one or more
degree-granting programs.  Departments are associated with a school
and offer courses.  Further on in the tutorial we will introduce
other tables such as student, instructor and enrollment.

Selecting Data
--------------

HTSQL requests typically begin with a table name.  You can browse the
contents of a table, search for specific data, and select the columns
you want to see in the results.

The most basic HTSQL request (A1_) returns everything from a table:

.. sourcecode:: htsql

   /school

.. _A1:  http://demo.htsql.org/school

The result set is a list of schools in the university, including all
columns, sorted by the primary key for the table:

====  =============================
code  name
====  =============================
art   School of Art and Design
bus   School of Business
edu   College of Education
egn   School of Engineering
la    School of Arts and Humanities
mus   School of Music & Dance
ns    School of Natural Sciences
ph    Public Honorariums
sc    School of Continuing Studies
====  =============================


Not all columns are useful for every context.  Use a *selector* to
choose columns for display (A2_):

.. htsql:: /program{school, code, title}
   :cut: 4

.. _A2: http://demo.htsql.org/program{school,code,title}

Add a plus (``+``) sign to the column name to sort the column in
ascending order.  Use a minus sign (``-``) for descending order.  For
example, this request (A3_) returns departments in descending order:

.. htsql:: /department{name-, school}
   :cut: 4

.. _A3:
    http://demo.htsql.org/department{name-,school}

Using two ordering indicators will sort on labeled columns as they
appear in the selector.  In the example below, we sort in ascending
order on ``department`` and then descending on ``credits`` (A4_):

.. htsql:: /course{department+, no, credits-, title}
   :cut: 3

.. _A4:
    http://demo.htsql.org
    /course{department+, no, credits-, title}

To display friendlier names for the columns, use ``:as`` to rename a
column's title (A5_):

.. htsql:: /course{department+ :as 'Dept Code', no :as 'No.',
                   credits-, title}
   :cut: 3

.. _A5:
    http://demo.htsql.org
    /course{department+%20:as%20'Dept%20Code',no%20:as%20'No.',
            credits-, title}

Selectors let you choose, rearrange, and sort columns of interest.  They
are an easy way to exclude data that isn't meaningful to your report.

Linking Data
------------

In our example schema, each ``program`` is administered by a ``school``.
Since the HTSQL processor knows about this relationship, it is possible
to link data accordingly (B1_):

.. htsql:: /program{school.name, title}
   :cut: 4

.. _B1:
    http://demo.htsql.org
    /program{school.name, title}

This request joins the ``program`` and ``school`` tables by the foreign
key from ``program{school}`` to ``school{code}``.  This is called a
*singular* relationship, since for every ``program``, there is exactly
one ``school``.

It is possible to join through multiple foreign keys; since ``course``
is offered by a ``department`` which belongs to a ``school``, we can
list courses including school and department name (B2_):

.. htsql:: /course{department.school.name, department.name, title}
   :cut: 4

.. _B2:
    http://demo.htsql.org
    /course{department.school.name, department.name, title}

This request can be shortened a bit by collapsing the duplicate mention
of ``department``; the resulting request is equivalent (B3_):

.. htsql:: /course{department{school.name, name}, title}
   :cut: 4
   :hide:

.. _B3:
    http://demo.htsql.org
    /course{department{school.name, name}, title}

For cases where you don't wish to specify each column explicitly, use
the wildcard ``*`` selector.  The request below returns all columns from
``department`` and all columns from its correlated ``school`` (B4_):

.. htsql:: /department{*,school.*}
   :cut: 4

.. _B4:
    http://demo.htsql.org
    /department{*,school.*}

Since the HTSQL processor knows about relationships between tables in
your relational database, joining tables in your reports is trivial.

Filtering Data
--------------

Predicate expressions in HTSQL follow the question mark ``?``.
For example, to return departments in the 'School of Engineering'
we write (C1_):

.. htsql:: /department?school='eng'
   :cut: 4

.. _C1:
    http://demo.htsql.org
    /department?school='eng'

The request above returns all rows in the ``department`` table where the
column ``school`` is equal to ``'eng'``.   In HTSQL, *literal* values are
single quoted, in this way we know ``'eng'`` isn't the name of a column.

Often times we want to compare a column against values from a list.  The
next example returns rows from the ``program`` table for the "Bachelors
of Arts" (``'ba'``) or "Bachelors of Science" (``'bs'``) degrees (C2_):

.. htsql:: /program?degree={'ba','bs'}
   :cut: 3

.. _C2:
    http://demo.htsql.org
    /program?degree={'ba','bs'}

Complex filters can be created using boolean connectors, such as the
conjunction (``&``) and alternation (``|``) operators .  The following
request returns programs in the "School of Business" that do not
grant a "Bachelor of Science" degree (C3_):

.. htsql:: /program?school='bus'&degree!='bs'
   :cut: 3

.. _C3:
    http://demo.htsql.org
    /program?school='bus'&degree!='bs'

Filters can be combined with selectors and links.  The following request
returns courses, listing only department number and title, having less
than 3 credits in the "School of Natural Science" (C4_):

.. htsql:: /course{department, no, title}
            ?credits<3&department.school='ns'
   :cut: 4

.. _C4:
    http://demo.htsql.org
    /course{department, no, title}
       ?credits<3&department.school='ns'

It is sometimes desirable to specify the filter before the selector.
Using a *table expression*, denoted by parenthesis, the previous request
is equivalent to (C5_):

.. htsql:: /(course?credits<3&department.school='ns')
            {department, no, title}
   :cut: 4
   :hide:

.. _C5:
    http://demo.htsql.org
    /(course?credits<3&department.school='ns')
      {department, no, title}

HTSQL supports a whole suite of functions and predicator operators.
Further, through the plug-in mechanism, custom data types, operators,
and functions may be integrated to support domain specific needs.

Formatters
----------

Once data is selected, linked and filtered, it is formatted for the
response.  By default, HTSQL uses the ``Accept`` header to negotiate the
output format with the user agent.  This can be overridden with a format
command, such as ``/:json``.  For example, results in JSON format (RFC
4627) can be requested as follows (D1_):

.. htsql:: /school/:json
   :plain:

.. _D1:
    http://demo.htsql.org
    /school/:json

Other formats include ``/:txt`` for plain-text formatting, ``/:html`` for
display in web browsers, and ``/:csv`` for data exchange.

Putting it All Together
-----------------------

The following request selects records from the ``course`` table,
filtered by all departments in the 'School of Business', sorted by
``course`` ``title``, including ``department``'s ``code`` and ``name``,
and returned as a "Comma-Separated Values" (RFC 4180) (E1_):

.. htsql:: /course{department{code,name},no,title+}?
            department.school='bus'/:csv
   :hide:

.. _E1:
    http://demo.htsql.org
    /course{department{code,name},no,title+}?
          department.school='bus'/:csv

HTSQL requests are powerful without being complex.  They are easy to
read and modify.  They adapt to changes in the database.  These
qualities increase the usability of databases by all types of users and
reduce the likelihood of costly errors.


Relating and Aggregating Data
=============================

HTSQL distinguishes between *singular* and *plural* relationships to
simplify query construction.  By a *singular* relationship we mean for
every record in one table, there is at most one record in a linked
table; by *plural* we mean there is perhaps more than one correlated
record.  To select a *plural* expression in a result set, an *aggregate*
function, such as ``sum``, ``count``, or ``exists`` must be used.  In
this way, what would be many values are converted into a single data cell
and integrated into a coherent result set.

By requiring aggregates for plural expressions, HTSQL reduces query
construction time and reduces errors.  When a query starts with a table,
rows returned are directly correlated to records in this table. Since
cross products or projections cannot be created accidentally, the
combined result set is always consistent and understandable.

Basic Linking
-------------

One-to-many relationships are the primary building block of relational
structures.  In our schema, each ``course`` is offered by a
``department`` with a mandatory foreign key.  For each course, there is
exactly one corresponding department.  In this case, the relationship is
singular in one direction and plural in the other.

If each row in your result set represents a ``course``, it is easy to
get correlated information for each course's department (RA1_):

.. htsql:: /course{department.name, title}
   :cut: 3

.. _RA1:
    http://demo.htsql.org
    /course{department.name,title}

It's possible to join *up* a hierarchy in this way, but not down. If
each row in your result set is a ``department``, then it is an error to
request ``course``'s ``credits`` since there could be many courses in a
given department (RA2_):

.. htsql:: /department{name, course.credits}
   :error:

.. _RA2:
    http://demo.htsql.org
    /department{name,course.credits}

In cases like this, an aggregate function, such as ``max`` is needed to
convert a plural expression into a singular value.  The following
example shows the maximum course credits by department (RA3_):

.. htsql:: /department{name, max(course.credits)}
   :cut: 3

.. _RA3:
    http://demo.htsql.org
    /department{name,max(course.credits)}

Conversely, you cannot use aggregates with singular expressions.  For
example, since ``school`` is singular relative to ``department``, it is
an error to count them (RA4_):

.. htsql:: /department{name, count(school)}
   :error:

.. _RA4:
    http://demo.htsql.org
    /department{name, count(school)}

For single row or *scalar* expressions, an aggregate is always needed
when referencing a table.  For example, the query below returns maximum
number of course credits across all departments (RA5_):

.. htsql:: /max(course.credits)

.. _RA5:
    http://demo.htsql.org
    /max(course.credits)


Aggregate Expressions
---------------------

Since ``school`` table has a *plural* (one to many) relationship
with ``program`` and ``department``, we can count them (RB1_):

.. htsql:: /school{name, count(program), count(department)}
   :cut: 4

.. _RB1:
    http://demo.htsql.org
    /school{name,count(program),count(department)}

Filters may be used within an aggregate expression.  For example, the
following returns the number of courses, by department, that are at
the 400 level or above (RB2_):

.. htsql:: /department{name, count(course?no>=400)}
   :cut: 4

.. _RB2:
    http://demo.htsql.org
    /department{name, count(course?no>=400)}

It's possible to nest aggregate expressions.  This request returns the
average number of courses each department offers (RB3_):

.. htsql:: /school{name, avg(department.count(course))}
   :cut: 4

.. _RB3:
    http://demo.htsql.org
    /school{name, avg(department.count(course))}

Filters and nested aggregates can be combined.  Here we count, for each
school, departments offering 4 or more credits (RB4_):

.. htsql:: /school{name, count(department?exists(course?credits>3))}
   :cut: 4

.. _RB4:
    http://demo.htsql.org
    /school{name, count(department?exists(course?credits>3))}

Filtering can be done on one column, with aggregation on another.  This
example shows average credits from only high-level courses (RB5_):

.. htsql:: /department{name, avg((course?no>400).credits)}
   :cut: 4

.. _RB5:
    http://demo.htsql.org
    /department{name, avg((course?no>400).credits)}

Numerical aggregates are supported.  These requests compute some useful
``course.credit`` statistics (RB6_, RB7_):

.. htsql:: /department{code, min(course.credits), max(course.credits)}
   :cut: 4

.. htsql:: /department{code, sum(course.credits), avg(course.credits)}
   :cut: 4

.. _RB6:
    http://demo.htsql.org
    /department{code, min(course.credits), max(course.credits)}

.. _RB7:
    http://demo.htsql.org
    /department{code, sum(course.credits), avg(course.credits)}

The ``every`` aggregate tests that a predicate is true for every row in
the correlated set.  This example returns ``department`` records that
either lack correlated ``course`` records or where every one of those
``course`` records have exactly ``3`` credits (RB8_):

.. htsql:: /department{name, avg(course.credits)}
            ?every(course.credits=3)
   :cut: 4
   :hide:

.. _RB8:
    http://demo.htsql.org
    /department{name, avg(course.credits)}
      ?every(course.credits=3)


Logical Expressions
===================

A *filter* refines results by including or excluding data by specific
criteria.  This section reviews comparison operators, boolean
expressions, and ``NULL`` handling.

Comparison Operators
--------------------

The quality operator (``=``) is overloaded to support various types.
For character strings, this depends upon the underlying database's
collation rules but typically is case-sensitive.  For example, to return
a ``course`` by ``title`` (PC1_):

.. htsql:: /course?title='Drawing'

.. _PC1:
    http://demo.htsql.org
    /course?title='Drawing'

If you're not sure of the exact course title, use the case-insensitive
*contains* operator (``~``).  The example below returns all ``course``
records that contain the substring ``'lab'`` (PC2_):

.. htsql:: /course?title~'lab'
   :cut: 4

.. _PC2:
    http://demo.htsql.org
    /course?title~'lab'

Use the *not-contains* operator (``!~``) to exclude all courses with
physics in the title (PC3_):

.. htsql:: /course?title!~'lab'
   :cut: 4
   :hide:

.. _PC3:
    http://demo.htsql.org
    /course?title!~'lab'

To exclude a specific class, use the *not-equals* operator (PC4_):


.. htsql:: /course?title!='Organic Chemistry Laboratory I'
   :cut: 4
   :hide:

.. _PC4:
    http://demo.htsql.org
    /course?title!='Organic Chemistry Laboratory I'


The *equality* (``=``) and *inequality* (``!=``) operators are
straightforward when used with numbers (PC5_):

.. htsql:: /course{department,no,title}?no=101
   :cut: 2

.. _PC5:
    http://demo.htsql.org
    /course{department,no,title}?no=101

The *in* operator (``={}``) can be thought of as equality over a set.
This example, we return courses that are in neither the "Art History"
nor the "Studio Art" department (PC6_):

.. htsql:: /course?department!={'arthis','stdart'}
   :cut: 4
   :hide:

.. _PC6:
    http://demo.htsql.org
    /course?department!={'arthis','stdart'}

Use the *greater-than* (``>``) operator to request courses with more
than 3 credits (PC7_):

.. htsql:: /course?credits>3
   :cut: 2

.. _PC7:
    http://demo.htsql.org
    /course?credits>3

Use the *greater-than-or-equal-to* (``>=``) operator request courses
that have three credits or more (PC8_):

.. htsql:: /course?credits>=3
   :cut: 4
   :hide:

.. _PC8:
    http://demo.htsql.org
    /course?credits>=3

Using comparison operators with strings tells HTSQL to compare them
alphabetically (once again, dependent upon database's collation).  For
example, the *greater-than* (``>``) operator can be used to request
departments whose ``code`` follows ``'me'`` in the alphabet (PC9_):

.. htsql:: /department?code>'me'
   :cut: 4

.. _PC9:
    http://demo.htsql.org
    /department?code>'me'


Boolean Expressions
-------------------

HTSQL uses function notation for constants such as ``true()``, ``false()``
and ``null()``.  For the text formatter, a ``NULL`` is shown as a blank,
while the empty string is presented as a double-quoted pair (PA1_):

.. htsql:: /{true(), false(), null(), ''}

.. _PA1:
    http://demo.htsql.org
    /{true(), false(), null()}

The ``is_null()`` function returns ``true()`` if it's operand is
``null()``.  In our schema, non-academic ``department`` records with
a ``NULL`` ``school`` can be listed (PA2_):

.. htsql:: /department{code, name}?is_null(school)

.. _PA2:
    http://demo.htsql.org
    /department{code, name}?is_null(school)

The *negation* operator (``!``) is ``true()`` when it's operand is
``false()``.   To skip non-academic ``department`` records (PA3_):

.. htsql:: /department{code, name}?!is_null(school)
   :cut: 4

.. _PA3:
    http://demo.htsql.org
    /department{code, name}?!is_null(school)

The *conjunction* (``&``) operator is ``true()`` only if both of its
operands are ``true()``.   This example asks for courses in the
``'Accounting'`` department having less than 3 credits (PA4_):

.. htsql:: /course?department='acc'&credits<3

.. _PA4:
    http://demo.htsql.org
    /course?department='acc'&credits<3

The *alternation* (``|``) operator is ``true()`` if either of its
operands is ``true()``.  For example, we could list courses having
anomalous number of credits (PA5_):

.. htsql:: /course?credits>4|credits<3
   :cut: 4

.. _PA5:
    http://demo.htsql.org
    /course?credits>4|credits<3

The precedence rules for boolean operators follow typical programming
convention; negation binds more tightly than conjunction, which binds
more tightly than alternation.  Parenthesis can be used to override this
default grouping rule or to better clarify intent.  The next example
returns courses that are in "Art History" or "Studio Art" departments
that have more than three credits (PA6_):

.. htsql:: /course?(department='arthis'|department='stdart')&credits>3
   :cut: 4

.. _PA6:
    http://demo.htsql.org
    /course?(department='arthis'|department='stdart')&credits>3

Without the parenthesis, the expression above would show all courses
from ``'arthis'`` regardless of credits (PA7_):

.. htsql:: /course?department='arthis'|department='stdart'&credits>3
   :cut: 3

.. _PA7:
    http://demo.htsql.org
    /course?department='arthis'|department='stdart'&credits>3

When a non-boolean is used in a logical expression, it is implicitly
cast as a *boolean*.  As part of this cast, tri-value logic is
flattened, ``null()`` is converted into ``false()``.  For strings, the
empty string (``''``) is also treated as ``false()``.  This conversion
rule shortens URLs and makes them more readable.

For example, this query returns only ``course`` records having a
``description`` (PA8_):

.. htsql:: /course?description
   :cut: 4
   :hide:

.. _PA8:
    http://demo.htsql.org
    /course?description

The predicate ``?description`` is treated as a short-hand for
``?(!is_null(description)&description!='')``.  The negated variant of
this shortcut is more illustrative (PA9_):

.. htsql:: /course{department,no,description}? !description

.. _PA9:
    http://demo.htsql.org
    /course{department,no,description}? !description


Types and Functions
===================

HTSQL supports *boolean*, *date*, *numeric*, and *string* data types, as
well as variants.  The pluggable type system can be used to augment the
core types provided.

Working with NULLs
------------------

HTSQL provides a rich function set for handling ``NULL`` expressions;
however, careful attention must be paid.  For starters, the standard
equality operator (``=``) is null-regular, that is, if either operand is
``null()`` the result is ``null()``.  The following request always
returns 0 rows (WN1_):

.. htsql:: /department?school=null()

.. _WN1:
    http://demo.htsql.org
    /department?school=null()

While you wouldn't directly write that query, it could be the final
result after parameter substitution for a templatized query such as
``/department?school=$var``.  For cases like this, use *total equality*
operator (``==``) which treats ``NULL`` values as equivalent (WN2_):

.. htsql:: /department?school==null()

.. _WN2:
    http://demo.htsql.org
    /department?school==null()

The ``!==`` operator lists distinct values, including records with
a ``NULL`` for the field tested (WN3_):

.. htsql:: /department?school!=='art'
   :cut: 5

.. _WN3:
    http://demo.htsql.org
    /department?school!=='art'




Odds & Ends
===========

There are a few more items that are important to know about, but for
which we don't document yet (but will before release candidate).

* untyped literals, ``/{1='1'}``
* single-quote escaping, ``/{'Bursar''s Office'}``
* percent-encoding, ``/{'%25'}``
* functions vs methods
* sort expression, ``/course.sort(credits)``
* limit/offset, ``/course.limit(5,20)``


