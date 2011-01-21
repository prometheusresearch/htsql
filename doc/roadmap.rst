*****************
  HTSQL Roadmap
*****************


SQL Translator
==============


Rewrite phase
-------------

Add a new translation phase *rewrite*, which optimizes the structure
of the code tree.  The goal is to help the compiler to generate
an optimal term tree.

Optimizations we could apply at this phase:

*Unmasking unit spaces*
    Remove filtering operations from unit spaces when they are already
    applied by the term to which the unit is attached.  Currently, it
    is done by the compiler, but there is no reason why it couldn't be
    done directly in the code tree, thus reducing the complexity of
    the compiler.

*Bundling aggregate and scalar units*
    Aggregate units sharing the same base and plural spaces should
    be evaluated in the same term node.  Similarly, scalar units
    sharing the same base space should be evaluated in the same
    term node.

    Currently, the compiler takes all aggregate units to be compiled,
    group them by their base and plural spaces, and then compile the
    groups.  However, that is suboptimal since the compiler could only
    group units that are scheduled for compilation at the same time,
    so it misses some potential groupings.

    Instead, on the rewrite phase, we will look for aggregates sharing
    the same base/plual space, make a bundle node, and replace the
    original unit nodes with links to the bundle node.

    A more advanced optimization requires rewriting the operand of
    an aggregate expression.  For instance::

        /{sum(course{credits}?department='comp'),
          sum(course{credits}?department='mth')}

    could be rewritten to::

        /{sum(course{if(department='comp',credits)}),
          sum(course{if(department='mth',credits)})}

    The latter is trivially expressed using a single frame.

*Evaluating expressions with literal operands*
    Perform arithmetic and logical operations directly on the code
    tree.  Examples::

        2+2 => 4
        3<4 => true()
        false()&... => false()

    Should be careful since the result of the operation may be
    backend-dependent.

**Milestone:** 2.1, 2.2?

**Dependencies:** none

**Difficulty:** weeks


Projections
-----------

Introduce a *quotient* table: given a table ``T`` and a *kernel* expression
``k``, the quotient table ``T^k`` consists of all distinct values of the
kernel when it is applied to every row of ``T``.  In SQL, a quotient
table is expressed as::

    SELECT k
    FROM T
    GROUP BY 1

There is a natural link from the original table ``T`` to the quotient
table ``T^k``, that is, the value of kernel expression applied to
a row of ``T`` determines the link to ``T^k``.

Implementing projections will affect the early phases of the translator:
*parsing*, *binding*, *encoding*, and *compiling*.

Two new operations are to be added: a projection operator and a projection
decorator.  A projection operator takes a table and a kernel and returns
a quotient space.  Provisional syntax::

    /(T ^ k)
    /(T ^ {k1,k2,...})
    /T1.(T2 ^ k)

A projection decorator provides short-cut syntax::

    /T{k^}

is equivalent to::

    /(T ^ k){kernel()}

Open questions are:

* syntax for making a quotient space (``T ^ k`` or ``quotient(T, k)``?);
* how to refer to the kernel in the quotien context (``kernel()``, but
  what about multi-column kernels?);
* how to follow the link to the original table (``(T ^ k).col``, but then
  how to express ``(T ^ k).avg(sum(col))``, where ``avg()`` is taken against
  the quotient space and ``sum()`` is taken against the complement space?).

On the code tree level, two new axis spaces are to be added: a *quotient*
space and a *complement* space.  A complement space must conform the space
of the original table, though it is not clear how it could be implemented.

The compiler should be updated to handle translation of the quotient
and complement spaces into terms.

**Milestone:** 2.1, 2.2?

**Dependencies:** *rewrite phase*?

**Difficulty:** weeks to months


Multi-segments
--------------

A *segment* operator converts a plural expression into a list.  Example::

    /school{code,/program,/department}

With a scalar base::

    /{/program,/department}

Short-cut syntax::

    /school/department

It is a big change that affects both the translator and the presentation
stacks.

For the translator, we need to

* update the grammar to add the prefix ``/`` operator (note that it clashes
  with the division operator) and a short-cut syntax;
* refactor the query and segment nodes for every phase of the translator to
  support subsegments;
* refactor the query plan object to support multi-segments.

For the presentation, we need to update every formatter to support the
list data type.  Some of the formatters (HTML, TXT) may support several
styles for presenting links.

Also, add ``/*`` operator, where::

    /school/*department

is equivalent to::

    /school?exists(department)/department

**Milestone:** 2.3, 2.4?

**Dependencies:** formatting styles? projections?

**Difficulty:** weeks to months


MySQL, Oracle, Microsoft SQL Server backends
--------------------------------------------

Possibly others.  DB2?

Complications:

First, Oracle, MS SQL do not have Boolean data type; moreover, they
do not permit Boolean expressions in the ``SELECT`` list.  Thus we need
to wrap/unwrap every predicate --- non-trivial since it is hard to
convert a three-state Boolean value into a non-Boolean expression.  That
could be done as a part of the *reduce* phase.

Second, Oracle, MS SQL do not have ``LIMIT`` and ``OFFSET`` clauses
(technically, MS SQL has ``TOP``, which is equivalent to ``LIMIT``).
A term node with ``LIMIT`` must be rewritten using ``ROWNUM`` or
``ROW_NUMBER``.  That requires modifying the compiler.

Plan:

* add backend modules ``htsql_mysql``, ``htsql_oracle``, ``htsql_mssql``;
* port the regression schema to each of the backend;
* add buildbot/virtual machine for each of the backends.

It would be great to have all backends added before other major
changes are introduced to the translator.

**Milestone:** 2.1, 2.2?

**Dependencies:** split distributions

**Difficutly:** weeks


Date/Time data types
--------------------

Currently, HTSQL only supports the ``DATE`` data type.  More should be added:

* ``TIME``;
* ``TIME WITH TIMEZONE``;
* ``TIMESTAMP``;
* ``TIMESTAMP WITH TIMEZONE``;
* ``INTERVAL FROM YEAR TO MONTH``;
* ``INTERVAL FROM DAY TO SECOND``.

Problems:

* not every backend supports every data type;
* the implementations vary in syntax and semantics.

When a backend does not support a particular date/time type, it could be
emulated.  For instance, MS SQL Server does not have a ``DATE`` data type,
but we could represent a date value using the supported ``DATETIME`` type.
Similarly, an ``INTERVAL`` value could be represented as a decimal or
a float number;  ``TIME`` could be represented by a float number in range
``0.0 <= t < 1.0``.

Support for some of the data types differs considerably across the backends;
making them work uniformly in HTSQL may be difficult.

In HTSQL, we add:

* domains ``TimeDomain``, ``DateTimeDomain``, ``TimeDeltaDomain``;
* casts ``time(_)``, ``datetime(_)``, ``timedelta(_)``;
* constructors ``time(_,_,_)``, ``datetime(_,_,_,_,_,_)``,
  ``timedelta(?)``;
* respective arithmetic operations and extractors.

Open questions:

* how to support ``... WITH TIMEZONE``;
* how to support ``INTERVAL FROM ... TO ...``;
* format of time delta literals.

It is very desirable that we add all planned backends before the new data
types.

**Milestone:** 2.2, 2.3?

**Dependencies:** more backends

**Difficulty:** weeks


Stubs
=====


Array/Composite types
---------------------

For PostgreSQL.


Caching to disk
---------------

Also, use ``CURSOR`` for the PostgreSQL backend to avoid fetching
the whole data set.


Connection pooling
------------------


$variables
----------


Ad-hoc linking
--------------

Including ``:by``, ``@table``.


.top(N)
-------

Using ``RANK``, but what about ``MySQL`` and ``SQLite``?


Locators
--------

Including ``id()``, identity type and literals.


Meta backend
------------

A native Python backend which evaluates an HTSQL query directly
against a Python data structure instead of delegating the query
to a SQL server.

Also, includes a meta schema, which gives access to the HTSQL
meta data::

    /table?name~'course'/:meta


Introspecting views
-------------------

Deduce primary and foreign keys from the definition of a SQL view.


Lookup rules
------------

Make sure that every table and every link is addressable, even
when the name is potentially ambiguous.  Improve error messages.


i18n
----

Add translation framework.


Users and roles
---------------

Native support for database users and roles.  Syntax::

    /~role/query

Tons of open issues though.


Hard limit
----------

Add ``LIMIT hard_limit+1`` clause to the generated SQL.  Report overflows
nicely.


Selector
--------

Review the semantics of the ``{}`` operator; also normalize the grammar
of the ``segment`` production.


``INSERT``/``UPDATE``/``DELETE``
--------------------------------


``:save()``
-----------

Is it essentially an ORM for Javascript?


Record and Union data types
---------------------------

These do not have SQL representations; used solely for the meta schema.


Stored procedures
-----------------


Query introspection
-------------------

UI tools need a way to introspect and modify a query, or even an incomplete
query (for instance, to autocomplete).


``CUBE`` and ``ROLLUP``
-----------------------

Perhaps, should be emulated using multi-segments.


Split repositories
------------------

* ``htsql_core``
* ``htsql_sqlite``
* ``htsql_pgsql``
* ``htsql_tests``?


Binary packages
---------------

For Debian, Red Hat, Mac OS X, MS Windows.


Commands/Formatters
-------------------

Implement them as decorator functions?  A command interface is then
an adapter: ``Render``, ``Produce``, etc applied to the binding tree.


HTRAF
-----

Move the HTRAF demo into a separate repository; convert it to
a proper project.

Add support for ``.htsql`` proxy.


RDOMA
-----

For accidental DBAs.


Explicit catalog config
-----------------------

To override the implicit configuration by the introspector.


Calculated fields
-----------------

And custom functions.


Extendable introspection and catalog entities
---------------------------------------------

UI tools may need extra fields for HTSQL entities.


Column headers
--------------

Review and fix.


Styling HTML output
-------------------

Decorators.


Unicode
-------

Use ``unicode`` data type internally.

Also, Python 3 support.


Website and documentation
-------------------------

* convert *Examples* to a slideshow;
* screencast;
* finish tutorial;
* insallation instructions;
* design rationale;
* language reference;
* function reference;
* reference card.


