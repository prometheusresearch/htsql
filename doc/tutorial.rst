******************
  HTSQL Tutorial
******************

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
JSON, etc.) requested by the user agent:

.. diagram:: dia/htsql-web-service.tex
   :alt: HTSQL as a web service
   :align: center

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
courses offered:

.. diagram:: dia/administrative-directory-schema.tex
   :alt: Administrative Directory schema
   :align: center

The university consists of schools, which administer one or more
degree-granting programs.  Departments are associated with a school
and offer courses.  Further on in the tutorial we will introduce
other tables such as student, instructor and enrollment.


Basic Expressions
-----------------

HTSQL requests start with a forward-slash ``/``.  To return all rows and
columns from the school table, sorted by primary key, write:

.. htsql:: /school
   :cut: 3

.. hint::
   In this tutorial query results are shown truncated.  Click 
   on any example request to open it at demo.htsql.org and see 
   the complete output.

Scalar expressions, including arithmetic and boolean operations, can be
written directly:

.. htsql:: /(3+4)*6

HTSQL has many built-in functions.  For instance you could use the
function ``count()`` to get the number of rows in a table:

.. htsql:: /count(school)

HTSQL uses a regular and intuitive syntax for expressions ranging from
table selection to complex calculation.


Choosing Columns
----------------

Use a *selector*, marked with ``{`` curley braces ``}``, to specify 
more than one output column:

.. htsql:: /{count(school), count(program), count(department)}

When returning data from a table, use a selector to choose columns for
display:

.. htsql:: /program{school_code, code, title}
   :cut: 4

In addition to table attributes, you could select arbitrary expressions.
The following example displays, for each school record, the school's 
name and the number of associated departments:

.. htsql:: /school{name, count(department)}
   :cut: 4

To title an output column, use the ``:as`` decorator:

.. htsql:: /school{name, count(department) :as '%23 of Dept.'}
   :query: /school{name,%20count(department)%20:as%20'%23%20of%20Dept.'}
   :cut: 3

Since HTSQL is a web query language, there are two characters that have
special meaning: ``%`` is used to encode reserved and unprintable
characters as hexadecimal UTF-8 octets; ``#`` represents query fragments
that can be truncated by your browser.   Hence, these characters must be
percent-encoded in HTSQL queries: ``%`` is written ``%25``; ``#`` is
written ``%23``.  Depending upon the browser, other characters may be
percent-encoded, for example, the space `` `` may show up as ``%20``.


Linking Data
------------

In our example schema, each ``program`` is administered by a ``school``.
Since the HTSQL processor knows about this relationship, it is possible
to link data accordingly:

.. htsql:: /program{school.name, title}
   :cut: 3

It is possible to link data through several relationships.  Since ``course``
is offered by a ``department`` which belongs to a ``school``, we can write:

.. htsql:: /course{department.school.name, department.name, title}
   :cut: 4

This request can be shortened a bit by collapsing the duplicate mention
of ``department``; the resulting request is equivalent:

.. htsql:: /course{department{school.name, name}, title}
   :cut: 4
   :hide:

For cases where you don't wish to specify each column explicitly, use
the wildcard ``*`` selector.  The request below returns all columns from
``department`` and all columns from its correlated ``school``:

.. htsql:: /department{*, school.*}
   :cut: 4

Since the HTSQL processor knows about relationships between tables in
your relational database, linking tables in your reports is trivial.


Filtering Data
--------------

Use the filter operator ``?`` to show only data that satisfies some
criteria. For example, to return departments in the School of
Engineering we can write:

.. htsql:: /department?school_code='eng'
   :cut: 4

This request returns all records in the ``department`` table where the
column ``school_code`` is equal to ``'eng'``.  In HTSQL, *literal*
values are single quoted so that ``'eng'`` isn't confused with a column
name.

For a case-insensitive substring match, use the ``~`` operator:

.. htsql:: /program?title~'lit'
   :cut: 3

Often times we want to compare a column against values from a list.  The
next example returns rows from the ``program`` table for the "Bachelors
of Arts" (``'ba'``) or "Bachelors of Science" (``'bs'``) degrees:

.. htsql:: /program?degree={'ba','bs'}
   :cut: 3

Complex filters can be created using boolean connectors, such as the
conjunction (``&``), alternation (``|``), and negation (``!``)
operators.  The following request returns programs in the "School of
Business" that do not grant a "Bachelor of Science" degree:

.. htsql:: /program?school.code='bus'&degree!='bs'
   :cut: 3

Filters can be combined with selectors and links.  The following request
returns courses, listing only department number and title, having less
than 3 credits in the "School of Natural Science":

.. htsql:: /course{department_code, no, title}
            ?credits<3&department.school.code='ns'
   :cut: 4


Sorting & Truncating
--------------------

By default, with a simple table expression such as ``/school``, all rows
are returned in the order of the primary key columns.  To override the
sort order, you can use ``sort()`` function:

.. htsql:: /school.sort(name)
   :cut: 4

Sort direction can be specified explicitly using ``+`` for ascending and
``-`` for descending order.  Also, you can sort by multiple columns. The
following example sorts courses in ascending order by department and
then in descending order by number of credits:

.. htsql:: /course.sort(department_code+, credits-)
   :cut: 2

When sorting by a selected output column, you could use a shortcut
syntax which combines column selection and sorting:

.. htsql:: /course{department_code+, no, credits-, title}
   :cut: 4

To list a range of rows, the ``limit()`` function takes one or two
arguments.  The first argument is the number of rows to return, the
optional second argument is the starting offset.  The next example
returns 5 records from the program table, skipping first 10 rows:

.. htsql:: /program.limit(5,10)


Formatting Output
-----------------

By default, HTSQL tries to guess the desired output format depending
upon the browser or the tool used to make the request.  This can be
overridden with a format decorator, such as ``/:json``.  For example,
results in JSON format can be requested as follows:

.. htsql:: /school/:json
   :plain:

Other formats include ``/:txt`` for plain-text formatting, ``/:html``
for display in web browsers, and ``/:csv`` for data exchange. 


Putting it Together
-------------------

HTSQL is a composable language where individual query fragments can be
combined into more complex expressions.  For example, a selection on the
course table such as ``/course{department, no, title}`` and a filter on
the course table, ``/course?credits<3`` can be combined in either of the
following two forms:

.. htsql:: /course{department_code, no, title}?credits<3
   :cut: 3 

.. htsql:: /course?credits<3 {department_code, no, title}
   :cut: 3 

Note that the order in which selection and filter operators are applied
doesn't affect the output. You could also use a functional form:

.. htsql:: /course.filter(credits<3).select(department_code, no, title)
   :hide:
   :cut: 3 

For the following two equivalent examples, we combine 3 operators --
sorting, truncating, and selection:

.. htsql:: /course.sort(credits-).limit(10){department_code, no, credits}
   :cut: 3 

.. htsql:: /course{department_code, no, credits-}.limit(10)
   :cut: 3 

The relative position of sort and limit matter, switching the positions
will change the output:

.. htsql:: /course.limit(10).sort(credits-){department_code, no, credits}
   :cut: 3


The following example requests the top 5 departments from schools with
``'art'`` in their name, sorted in descending order by the number of
courses.  The output columns include the corresponding school name, the
name of the department itself, and the number of courses.  The output
format is "Comma-Separated Values" suitable for consumption by
spreadsheet or statistical analysis packages:

.. htsql::

   /department{school.name, name, count(course)-}
              .filter(school.name~'art').limit(5)/:csv

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
get correlated information for each course's department:

.. htsql:: /course{department.name, title}
   :cut: 3

It's possible to join *up* a hierarchy in this way, but not down. If
each row in your result set is a ``department``, then it is an error to
request ``course``'s ``credits`` since there could be many courses in a
given department:

.. htsql:: /department{name, course.credits}
   :error:

In cases like this, an aggregate function, such as ``max`` is needed to
convert a plural expression into a singular value.  The following
example shows the maximum course credits by department:

.. htsql:: /department{name, max(course.credits)}
   :cut: 3

Conversely, you cannot use aggregates with singular expressions.  For
example, since ``school`` is singular relative to ``department``, it is
an error to count them:

.. htsql:: /department{name, count(school)}
   :error:

For single row or *scalar* expressions, an aggregate is always needed
when referencing a table.  For example, the query below returns maximum
number of course credits across all departments:

.. htsql:: /max(course.credits)


Aggregate Expressions
---------------------

Since ``school`` table has a *plural* (one to many) relationship
with ``program`` and ``department``, we can count them:

.. htsql:: /school{name, count(program), count(department)}
   :cut: 4

Filters may be used within an aggregate expression.  For example, the
following returns the number of courses, by department, that are at
the 400 level or above:

.. htsql:: /department{name, count(course?no>=400)}
   :cut: 4

It's possible to nest aggregate expressions.  This request returns the
average number of courses each department offers:

.. htsql:: /school{name, avg(department.count(course))}
   :cut: 3

Filters and nested aggregates can be combined.  Here we count, for each
school, departments offering 4 or more credits:

.. htsql:: /school{name, count(department?exists(course?credits>3))}
   :cut: 3

Filtering can be done on one column, with aggregation on another.  This
example shows average credits from only high-level courses:

.. htsql:: /department{name, avg((course?no>400).credits)}
   :cut: 4

Numerical aggregates are supported.  These requests compute some useful
``course.credit`` statistics:

.. htsql:: /department{code, min(course.credits), max(course.credits)}
   :cut: 4

.. htsql:: /department{code, sum(course.credits), avg(course.credits)}
   :cut: 4

The ``every`` aggregate tests that a predicate is true for every row in
the correlated set.  This example returns ``department`` records that
either lack correlated ``course`` records or where every one of those
``course`` records have exactly ``3`` credits:

.. htsql:: /department{name, avg(course.credits)}
            ?every(course.credits=3)
   :cut: 3


Compositional Navigation
------------------------

Suppose you have an HTSQL query that returns the school of engineering.

.. htsql:: /school.filter(code='eng')
   :hide:

Now you'd like to return departments associated with this school.  This
could be written as:

.. htsql:: /department?school.code='eng'
   :cut: 4

However, if you want to re-use the existing (and working!) query
fragment, ``school.filter(code='eng')``, you could write:

.. htsql:: /school.filter(code='eng').department
   :cut: 4

Continuing this chain, you may choose the Department of Electrical
Engineering and then list associated courses.

.. htsql::
   :cut: 4

   /school.filter(code='eng')
   .department.filter(code='ee')
   .course

Drill-down navigation trims unrelated rows and preserves the order of
prior links. Consider the following two queries.

.. htsql:: /department
   :cut: 4

.. htsql:: /school.department
   :cut: 4

Although the latter query also returns records from the department
table, it differs from the former in two ways.  First, it skips
departments lacking an associated school.  Second, it orders the result
first by school code and then on department code.


Calculations & References
=========================


Calculated Attributes
---------------------

Suppose that you're returning schools along with the number of
associated departments, and we want to list only schools with
more than 3 departments.

.. htsql::
   :cut: 3

   /school{name, count(department)}? count(department)>3

In this query we have to repeat the expression ``count(department)``
twice; once to select the value for output, and the other as part of
filter criteria.  It is possible to avoid this duplication by defining a
calculated attribute ``num_dept``.

.. htsql::
   :hide:
   :cut: 3

   /school.define(num_dept:=count(department))
     {name, num_dept}? num_dept>3

As syntax sugar, you could combine definition and selection.

.. htsql::
   :hide:
   :cut: 3

   /school{name, num_dept:=count(department)}? num_dept>3

All three of these examples return the same result. 


Calculated Links
----------------

In the prior example ``num_dept`` was a scalar value with respect to each
school.  It's possible to define links as well.  Suppose we'd like to
calculate a set of statistics by department on 200 level courses typically
taken by sophomores.

.. htsql::
   :cut: 3

   /department{name, count(course?no>=200&no<300),
                     max((course?no>=200&no<300).credits),
                     min((course?no>=200&no<300).credits),
                     avg((course?no>=200&no<300).credits)}

Here the link expression ``(course?no>=200&no<300)`` is duplicated.  We can
define a ``sophomore`` link to these courses as follows.

.. htsql::
   :cut: 3
   :hide:

   /department.define(sophomore := course?no>=200&no<300)
              {name, count(sophomore),
                     max(sophomore.credits),
                     min(sophomore.credits),
                     avg(sophomore.credits)}

For readability, it is helpful to put definitions at the end of an
expression where it is used.  In the following example the usage of
``sophomore`` precedes its definition.

.. htsql::
   :cut: 3
   :hide:

   /department{name,
                {count(sophomore),
                 max(sophomore.credits),
                 min(sophomore.credits),
                 avg(sophomore.credits)
                } :where(sophomore := course?no>=200&no<300)}

In this example we use infix notation to call the ``where()`` function.
Generally, any function call ``f(x,y)`` could be written ``x :f y``.


Parameterized Calculations
--------------------------

Suppose we want to expand the previous example, by calculating the same set
of statistics over 4 sets of courses: 100's, 200's, 300's and 400's. 

.. htsql::
   :cut: 3

   /department.define(freshman := course?no>=100&no<200,
                      sophomore := course?no>=200&no<300,
                      junior := course?no>=300&no<400,
                      senior := course?no>=400&no<500)
              {name, count(freshman),
                     max(freshman.credits),
                     min(freshman.credits),
                     avg(freshman.credits),
                     count(sophomore), 
                     max(sophomore.credits),
                     min(sophomore.credits),
                     avg(sophomore.credits),
                     count(junior), 
                     max(junior.credits),
                     min(junior.credits), 
                     avg(junior.credits),
                     count(senior), 
                     max(senior.credits),
                     min(senior.credits),
                     avg(senior.credits)}

In the above examples, we repeat the same group of aggregates four times,
but each time with different set of courses.  We could write this more
concisely defining a calculation with a parameter.

.. htsql::
   :hide:
   :cut: 3

   /department.define(freshman := course?no>=100&no<200,
                      sophomore := course?no>=200&no<300,
                      junior := course?no>=300&no<400,
                      senior := course?no>=400&no<500,
                      stats(set) := {count(set),
                                     max(set.credits),
                                     min(set.credits),
                                     avg(set.credits)})
              {name, stats(freshman),
                     stats(sophomore), 
                     stats(junior),
                     stats(senior)}

Here the parameter ``set`` is bound to a subset of courses for each grade
level.  The calculation returns a set of columns that appear in the output.


Argument References
-------------------

Instead of defining four different subsets of courses, we may want to define
a parameterized calculation which takes a the course level and produces
courses of this level.  Naively, we could write:

.. htsql::
   :hide:
   :error:

   /department.define(course(level) := course?no>=level*100
                                             &no<(level+1)*100)
              {name, count(course(1)),
                     count(course(2)),
                     count(course(3)),
                     count(course(4))}

Here we have a problem with the definition of ``course(level)``.  In the
body of the calculation, ``course`` introduces a new naming scope with
attributes from the course table, such as the course ``no``.  Names from the
previous scope, such as ``level``, are not available.  To overcome this
deliberate limitation, we mark ``level`` with a dollar sign to indicate that
it can be referenced from nested scopes.

.. htsql::
   :cut: 3

   /department.define(course($level) := course?no>=$level*100
                                              &no<($level+1)*100)
              {name, count(course(1)),
                     count(course(2)),
                     count(course(3)),
                     count(course(4))}

Using this technique, we could rewrite the last example from the previous
section as:

.. htsql::
   :hide:
   :cut: 3

   /department.define(
                 stats($level) := {count(set),
                                   max(set.credits),
                                   min(set.credits),
                                   avg(set.credits)
                                   } :where set :=
                                    course?no>=$level*100
                                          &no<($level+1)*100)
              {name, stats(1),
                     stats(2),
                     stats(3),
                     stats(4)}


Defined References
------------------

References are not limited to parameters of calculations, they could be
defined separately.  In the following example ``$avg_credits`` defines the
average number of credits per course.  This reference is then used to return
courses with more credits than average.

.. htsql::
   :cut: 3

   /define($avg_credits := avg(course.credits))
   .course?credits>$avg_credits

This same request can be written using ``where``.

.. htsql::
   :hide:
   :cut: 3

   /course?credits>$avg_credits
   :where $avg_credits := avg(course.credits)

Suppose that we'd like to return courses that have more than average
credits for their given department.  We could write this as follows.

.. htsql::
   :cut: 3

   /department.define($avg_credits:=avg(course.credits))
   .course?credits>$avg_credits
   

Projections 
===========

So far we have shown queries that produce either scalar values or rows
that correspond to records from a table.  Occasionally, you may want to
return all unique values of some expression.  For example, to return
distinct values of ``degree`` from the ``program`` table, write:

.. htsql:: /program^degree

In HTSQL, we call this a *projection*.  This construct creates a virtual
table of all unique records from a set of expressions.


Distinct Expressions
--------------------

The following example lists values from the degree column for each
record of the program table.  Observe that you get duplicate rows
corresponding to different records from the program table that share the
same degree:

.. htsql:: /program{degree}
   :cut: 4

To get unique rows from the example above, the ``distinct()`` function
can be used:

.. htsql:: /distinct(program{degree})
   :cut: 3

Equivalently, this could be written using the ``^`` operator:

.. htsql:: /program^degree
   :cut: 3

Note that the projection operator skips rows containing a *NULL*.
Hence, even though there are rows in the program without a degree,
``program^degree`` doesn't contain a *NULL*.

You could use projections anywhere a table expression is permitted. 
For instance, to get the number of distinct degrees offered at the
university, write:

.. htsql:: /count(program^degree)

Or, one could count distinct degrees by school:

.. htsql:: /school{name, count(program^degree)}
   :cut: 3

Projections aren't limited to table attributes.  Let's assume course
level as the first digit of the course number.  Then, hence following
expression returns distinct course levels:

.. htsql:: /course^trunc(no/100)
   :cut: 3

If you wish to project by more than one expression, use a selector
``{}`` to group the expressions.  In this example we return distinct
combinations of course level and credits.

.. htsql:: /course^{trunc(no/100), credits}
   :cut: 4

Just as tables are sorted by default using the table's primary key,
projected expressions are also sorted using the distinct columns.


Working with Projections
------------------------

Each projection is a virtual table with its own attributes and links to
other tables.  For instance, ``program^degree`` has two attributes, a
column ``degree`` and a plural link ``program`` to records of the
program table having that degree.  In the query below, we return
distinct degrees with the number of corresponding programs.

.. htsql:: /program^degree {degree, count(program)}
   :cut: 4

We may want to filter the base table before projecting.  For example,
listing only distinct degrees in the School of Engineering.

.. htsql:: 
   :cut: 5

   /program?school_code='eng' 
           ^degree

Or, we could filter the expression after the projection has happened.
In the next query we return only degrees having more than 5
corresponding programs.

.. htsql::
   :cut: 5

   /program^degree
           ?count(program)>5

Usually HTSQL automatically assigns names to projected columns, however,
in cases where you have an expression, you have to name them.  In the
following example, we return distinct course level and credits
combinations sorted in descending order by level and credits.

.. htsql:: /course^{level:=round(no/100),credits}{level-, credits-}
   :cut: 4

Sometimes HTSQL cannot assign a name linking to the base of the
projection.  In these cases, you may use ``^`` to refer to it.
Additionally ``*`` can be used to return all columns of the projection.
Thus, the first example of this section could be written:

.. htsql:: /program^degree{*, count(^)}
   :cut: 4

.. **


Logical Expressions
===================

A *filter* refines results by including or excluding data by specific
criteria.  This section reviews comparison operators, boolean
expressions, and ``NULL`` handling.

Comparison Operators
--------------------

The equality operator (``=``) is overloaded to support various types.
For character strings, this depends upon the underlying database's
collation rules but typically is case-sensitive.  For example, to return
a ``department`` by ``name``:

.. htsql:: /department?name='Economics'

If you're not sure of the exact department name, use the case-insensitive
*contains* operator (``~``).  The example below returns all ``department``
records that contain the substring ``'engineering'``:

.. htsql:: /department?name~'engineering'
   :cut: 4

Use the *not-contains* operator (``!~``) to exclude all courses with
*science* in the name:

.. htsql:: /department?name!~'science'
   :cut: 4
   :hide:

To exclude a specific department, use the *not-equals* operator:

.. htsql:: /department?name!='Management & Marketing'
   :cut: 4
   :hide:

The *equality* (``=``) and *inequality* (``!=``) operators are
straightforward when used with numbers:

.. htsql:: /department?count(course)!=0
   :cut: 2

The *in* operator (``={}``) can be thought of as equality over a set.
This example, we return departments that don't belong to either the
School of Engineering or the School of Natural Sciences:

.. htsql:: /department?school_code!={'eng','ns'}
   :cut: 4
   :hide:

Use the *greater-than* (``>``) operator to request departments with
more than 20 offered courses:

.. htsql:: /department?count(course)>20
   :cut: 4

Use the *greater-than-or-equal-to* (``>=``) operator to request
departments with 20 courses or more:

.. htsql:: /department?count(course)>=20
   :cut: 4
   :hide:

Using comparison operators with strings tells HTSQL to compare them
alphabetically (once again, dependent upon database's collation).  For
example, the *greater-than* (``>``) operator can be used to request
departments whose ``code`` follows ``'me'`` in the alphabet:

.. htsql:: /department?code>'me'
   :cut: 4


Boolean Expressions
-------------------

HTSQL uses function notation for constants such as ``true()``, ``false()``
and ``null()``.  For the text formatter, a ``NULL`` is shown as a blank,
while the empty string is presented as a double-quoted pair:

.. htsql:: /{true(), false(), null(), ''}

The ``is_null()`` function returns ``true()`` if it's operand is
``null()``.  In our schema, non-academic ``department`` records with
a ``NULL`` ``school_code`` can be listed:

.. htsql:: /department{code, name}?is_null(school_code)

The *negation* operator (``!``) is ``true()`` when it's operand is
``false()``.   To skip non-academic ``department`` records:

.. htsql:: /department{code, name}?!is_null(school_code)
   :cut: 4

The *conjunction* (``&``) operator is ``true()`` only if both of its
operands are ``true()``.   This example asks for courses in the
``'Accounting'`` department having less than 3 credits:

.. htsql:: /course?department_code='acc'&credits<3

The *alternation* (``|``) operator is ``true()`` if either of its
operands is ``true()``.  For example, we could list courses having
anomalous number of credits:

.. htsql:: /course?credits>5|credits<3
   :cut: 4

The precedence rules for boolean operators follow typical programming
convention; negation binds more tightly than conjunction, which binds
more tightly than alternation.  Parenthesis can be used to override this
default grouping rule or to better clarify intent.  The next example
returns courses that are in "Art History" or "Studio Art" departments
that have more than three credits:

.. htsql:: /course?(department_code='arthis'|department_code='stdart')&credits>3
   :cut: 4

.. ** || 

Without the parenthesis, the expression above would show all courses
from ``'arthis'`` regardless of credits:

.. htsql:: /course?department_code='arthis'|department_code='stdart'&credits>3
   :cut: 3

.. ** ||

When a non-boolean is used in a logical expression, it is implicitly
cast as a *boolean*.  As part of this cast, tri-value logic is
flattened, ``null()`` is converted into ``false()``.  For strings, the
empty string (``''``) is also treated as ``false()``.  This conversion
rule shortens URLs and makes them more readable.

For example, this query returns only ``course`` records having a
``description``:

.. htsql:: /course?description
   :cut: 4
   :hide:

The predicate ``?description`` is treated as a short-hand for
``?(!is_null(description)&description!='')``.  The negated variant of
this shortcut is more illustrative:

.. htsql:: /course{department_code,no,description}? !description


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
returns 0 rows:

.. htsql:: /department?school_code=null()

While you wouldn't directly write that query, it could be the final
result after parameter substitution for a templatized query such as
``/department?school=$var``.  For cases like this, use *total equality*
operator (``==``) which treats ``NULL`` values as equivalent:

.. htsql:: /department?school_code==null()

The ``!==`` operator lists distinct values, including records with
a ``NULL`` for the field tested:

.. htsql:: /department?school_code!=='art'
   :cut: 5


