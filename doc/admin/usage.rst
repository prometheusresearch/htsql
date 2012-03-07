*******************************
  Using and Configuring HTSQL
*******************************

.. contents:: Table of Contents
   :depth: 1
   :local:

.. highlight:: console


Usage
=====

.. index:: htsql-ctl

.. _htsql-ctl:

Invoking ``htsql-ctl``
----------------------

Installing HTSQL creates a command-line application ``htsql-ctl``::

    $ htsql-ctl

The ``htsql-ctl`` script is a collection of subcommands called
*routines*.  The command-line syntax of ``htsql-ctl`` is

::

    $ htsql-ctl <routine> [options] [arguments]

* ``<routine>`` is the routine name;
* ``options`` are any routine options in short (``-X``)
  or long (``--option-name``) form;
* ``arguments`` are routine arguments.

To get a list of routines, run::

    $ htsql-ctl help

To describe a specific routine, run::

    $ htsql-ctl help <routine>

.. index:: connection URI
.. index:: DBURI
.. _dburi:

Database Connection
-------------------

Many routines require a connection URI parameter, which specifies how to
connect to a database.  The connection URI has the form:

.. sourcecode:: text

    <engine>://<user>:<pass>@<host>:<port>/<database>

* ``<engine>`` is the type of the database server; ``sqlite`` for
  SQLite, ``pgsql`` for PostgreSQL, ``mysql`` for MySQL, ``mssql`` for
  MS SQL Server, ``oracle`` for Oracle.
* ``<user>:<pass>`` are authentication parameters;
* ``<host>:<port>`` is the address of the database server;
* ``<database>`` is the name of the database.

For SQLite, ``<user>:<pass>`` and ``<host>:<port>`` are omitted, and
``<database>`` specifies the path to the database file.  Thus, to
connect to SQLite database ``htsql_demo.db`` located in the
current directory, use the URI:

.. sourcecode:: text

    sqlite:htsql_demo.db

For PostgreSQL, if ``user:pass`` is omitted, the credentials of the
current user are used; if ``host:port`` is omitted, the server is
assumed to run on the local machine.  Thus, to connect to a database
``htsql_demo`` running on the same host under credentials of the
current user, use the URI:

.. sourcecode:: text

    pgsql:htsql_demo

Other database servers use similar conventions.

You can use option ``-p`` to prompt for a password if you do not want
to specify the database password in a command line.

.. index:: htsql-ctl shell

Command-line Shell
------------------

To start a command-line HTSQL shell, run::

    $ htsql-ctl shell <DBURI>

That starts an interactive HTSQL shell, where you could type and execute
HTSQL queries against the specified database.

For example, to start the shell on a PostgreSQL database ``htsql_demo``,
run::

    $ htsql-ctl shell pgsql:htsql_demo

    Interactive HTSQL Shell
    Type 'help' for more information, 'exit' to quit the shell.
    htsql_demo$

For more details on the ``shell`` routine, run::

    $ htsql-ctl help shell

.. index:: htsql-ctl serve
.. _htsql-ctl serve:

HTTP Server
-----------

To start a *demonstration* web server running HTSQL, run::

    $ htsql-ctl server <DBURI> [<HOST> [<PORT>]]

That starts an HTTP server on the address ``<HOST>:<PORT>``.
If ``<HOST>`` and ``<PORT>`` are omitted, the server is started on
``*:8080``.

For example, to start the HTSQL web server against PostgreSQL
database ``htsql_demo`` on ``localhost:3128``, run::

    $ htsql-ctl server pgsql:htsql_demo localhost 3128

    Starting an HTSQL server on localhost:3128 over htsql_demo

If database connection :ref:`configuration <configuration>` is provided
by ``-C``, you could use ``-`` as a place holder for the mandatory
database URI parameter so that you could provide a HOST and PORT.  For
example, to run the server on ``localhost:80`` you would write::

    # htsql-ctl serve -C demo-config.yaml - localhost 80

For more details on the ``server`` routine, run::

    $ htsql-ctl help server

.. index:: htsql-ctl extension

Extension Mechanism
===================

HTSQL has an extensive addon system that can be used to override almost
every aspect of server operation or query construction with an adapter.
Extensions can live in third party modules or be included in the HTSQL
distribution as part of our supported "tweaks".  To list supported
extensions, you could type::

    $ htsql-ctl extension

To find out more about an extension, such as :ref:`tweak.autolimit`, write::

    $ htsql-ctl extension tweak.autolimit

Using Extensions
----------------

An extension can be enabled using ``-E`` parameter on the ``htsql-ctl``
command line.  For example, to enable the :ref:`tweak.meta` addon on the
HTSQL demo database, you'd write::

    $ htsql-ctl shell -E tweak.meta pgsql:htsql_demo

Then, you could use the ``/meta()`` command registered by this addon:

.. sourcecode:: text

    Interactive HTSQL Shell
    Type 'help' for more information, 'exit' to quit the shell.
    htsql_demo$ /meta(/table)

Some addons have parameters which can be added to the command line.
For example, the :ref:`tweak.autolimit` extension truncates output at
``limit`` number of rows.  The default is 10k, but this value
can be changed::

    $ htsql-ctl shell -E tweak.autolimit:limit=10 pgsql:htsql_demo

If more than one parameter is possible, use "," to separate them::

    $ htsql-ctl shell -E tweak.hello:repeat=3,address=home pgsql:htsql_demo

HTSQL plugins are found using Python's entry point feature.  When a
Python package is installed, it can register itself as an
``htsql.addon`` extension so that it could be loaded in this manner.

.. _configuration:

Configuration Files
-------------------

Extension configuration can be provided with a YAML_ (or JSON_) file
using ``-C`` on the command line.  The top level of this file is a
dictionary listing the plugins that are enabled.  The second nesting
level are plugin parameters, if any. 

.. sourcecode:: yaml

    # demo-config.yaml
    htsql:
      db:
        engine: pgsql
        database: htsql_demo
        username: htsql_demo
        password: secret
        host: localhost
        port: 5432
    tweak.autolimit:
      limit: 1000
    tweak.shell.default:

In this example, there are three plugins enabled, ``htsql`` (which is a
mandatory plugin), :ref:`tweak.autolimit` and ref:`tweak.shell.default`.
The ``htsql`` plugin has one argument, ``db`` which has sub-structure
providing connection information.  You could then use this
configuration file using ``-C``::
  
    # htsql-ctl shell -C demo-config.yaml

If both ``-E`` and ``-C`` are used, explicit command line options override
values provided in the configuration file.  This permits a configuration
file to be used as a default perhaps using a different database URI.

.. _YAML: http://yaml.org/
.. _JSON: http://json.org/


Extension Reference
===================

The HTSQL distribution ships with several built-in extensions which
we describe here.

``htsql``
---------

The HTSQL core that provides the SQL translator and an HTTP server
is implemented in form of an addon ``htsql``.  This addon is always
included.

Parameters:

`db`
    The connection URI.

The parameter ``db`` specifies how HTSQL connects to the database.  It
could be written in a short or an expanded form.  In the short form,
the parameter is an URI:

.. sourcecode:: yaml

    htsql:
      db: pgsql://htsql_demo:secret@localhost:5432/htsql_demo

In the expanded form, a mapping notation is used:

.. sourcecode:: yaml

    htsql:
      db:
        engine: pgsql
        username: htsql_demo
        password: secret
        host: localhost
        port: 5432
        database: htsql_demo

Every component except ``engine`` and ``database`` is optional.

.. index:: engine.sqlite, engine.pgsql, engine.mysql, engine.oracle,
           engine.mssql

``engine.*``
------------

Extensions in the ``engine`` namespace implement database backends.
An appropriate extension is loaded automatically based on the ``engine``
parameter of the connection URI.

HTSQL supports the following database servers:

* SQLite 3+ (``engine.sqlite``)
* PostgreSQL 8.3+ (``engine.pgsql``)
* MySQL 5.1+ (``engine.mysql``)
* Oracle 10g+ (``engine.oracle``)
* Microsoft SQL Server 2005+ (``engine.mssql``)

.. index:: tweak.autolimit
.. _tweak.autolimit:

``tweak.autolimit``
-------------------

This addon truncates query output to a given number of rows
(10,000 by default).  It could be used to prevent accidental
denial of service caused by queries returning a large number
of rows.

The addon adds ``LIMIT <limit>`` to every generated SQL query.

Parameters:

`limit`
    Truncation threshold (default: 10,000).

.. sourcecode:: yaml

    tweak.autolimit:
      limit: 1000

.. index:: tweak.cors

``tweak.cors``
--------------

This addon adds CORS_ support to permit AJAX requests to the
HTSQL server by web pages hosted on a different domain.

To prevent data leaks, web browsers do not allow AJAX
requests to cross domain boundaries.  The CORS_
(Cross-Origin Resource Sharing) specification defines
a way for a server to provide a list of domains which
are permitted to make AJAX requests.

CORS_ relies on browser support and may not work with older
web browsers.

Parameters:

`origin`
    Domains allowed to access the server (default: ``*``).

The ``origin`` parameter is a list of domains which are
allowed to access the server.  The value must either be
``*`` (means *any*, which is the default) or a space-separated
list of host names::

    http[s]://domain[:port]

.. warning::

    The default settings permit HTSQL queries from any domain.
    Do not use the default settings with non-public data!

For example, to enable AJAX requests to the HTSQL demo server
(located at http://demo.htsql.org/) from domains http://htsql.org/
and http://htsql.com/, we could write:

.. sourcecode:: yaml

    tweak.cors:
      origin: http://htsql.org http://htsql.com

.. _CORS: http://www.w3.org/TR/cors/

.. index:: tweak.django, Django


.. _tweak.django:

``tweak.django``
----------------

This extension provides integration with Django_ web framework.
It replaces built-in database introspection and connection
handling with facilities provided by Django.

When using ``tweak.django`` addon, do not specify the connection
URI as it is determined from the Django project database
settings.

Parameters:

`settings`
    Path to the settings module (default: use
    ``DJANGO_SETTINGS_MODULE``).

.. sourcecode:: yaml

    tweak.django:
      settings: mysite.settings

.. _Django: https://www.djangoproject.com/

.. index:: tweak.meta

.. _tweak.meta:

``tweak.meta``
--------------

This extension provides a *meta* database describing tables,
columns and links of the primary database.

The ``tweak.meta`` addon has no parameters.

.. sourcecode:: yaml

    tweak.meta:

The meta database is composed of the following tables:

`table`
    all available tables
`field`
    columns and links for a given table
`column`
    all columns for a given table
`link`
    all links from one table to another

Use function ``meta()`` to make a query against the meta database.

To get a list of tables:

.. htsql:: /meta(/table)
   :cut: 4

Mapping call notation is also permitted:

.. htsql:: /table/:meta
   :cut: 4

To list all columns of a given table in the output order:

.. htsql:: /meta(/column.sort(field.sort)?table.name='course')
   :cut: 4

To get all links to and from a specific table:

.. htsql:: /meta(/link?table.name='department'|target.name='department')
   :cut: 4

.. ** ||

To describe the meta database itself, apply ``meta()`` twice:

.. htsql:: /meta(/meta(/table))

.. index:: tweak.override

.. _tweak.override:

``tweak.override``
------------------

This extension provides several ways to adjust database metadata.
It allows the user to restrict access to specific tables and columns,
specify additional database constraints, change the generated names
for tables, columns and links, and define calculated attributes.

Parameters:

`included-tables`
    Tables permitted to use.
`excluded-tables`
    Tables forbidden to use.
`included-columns`
    Columns permitted to use.
`excluded-columns`
    Columns forbidden to use.
`not-nulls`
    Additional ``NOT NULL`` constraints.
`unique-keys`
    Additional ``PRIMARY KEY`` and ``UNIQUE`` constraints.
`foreign-keys`
    Additional ``FOREIGN KEY`` constraints.
`class-labels`
    Labels for tables and top-level calculations.
`field-labels`
    Labels for columns, links and calculated fields.
`field-orders`
    Default table fields.
`unlabeled-tables`
    Tables to hide.
`unlabeled-columns`
    Columns to hide.
`globals`
    Global definitions.

To restrict access to a specific set of tables, use parameters
``included-tables`` and ``excluded-tables``.  Parameter
``included-tables`` is a list of tables allowed to be used
by HTSQL.  If this parameter is provided, any table not
in this list is completely hidden from the HTSQL processor.
Parameter ``excluded-tables`` allows you to forbid access
to a set of tables.

To forbid use of table ``confidential``:

.. sourcecode:: yaml

    tweak.override:
      excluded-tables: [confidential]

To allow access only to tables in ``ad`` and ``ed`` schemas:

.. sourcecode:: yaml

    tweak.override:
      included-tables: [ad.*, ed.*]

We could also use *block* form of a sequence:

.. sourcecode:: yaml

    tweak.override:
      included-tables:
        - ad.*
        - ed.*

In general, the table name may have the form ``<table>`` or
``<schema>.<table>`` and could include ``*`` meta-character to
indicate any number of characters.  Table names are
case-insensitive and normalized: any non-alphanumeric
character is replaced with ``_``.

Similarly, to restrict access to a specific set of columns,
use parameters ``included-columns`` and ``excluded-columns``.
Parameter ``exclude-columns`` is a list of column forbidden
for use by the HTSQL processor.

To exclude column ``SSN`` of table ``confidential``, write:

.. sourcecode:: yaml

    tweak.override:
      excluded-columns: [confidential.ssn]

The column name may have the form ``<column>``, ``<table>.<column>``,
or ``<schema>.<table>.<column>`` and could include ``*`` meta-character.

Note that columns listed in ``excluded-columns`` are removed
together with all associated key constraints.  If you want
to hide a column from output, but keep associated primary and
foreign keys, use the ``unlabeled-columns`` parameter.

HTSQL discovers database constraints from the schema definition.
If some constraints are not explicitly defined in the schema,
you may provide them using parameters ``not-nulls``, ``unique-keys``
and ``foreign-keys``.

.. warning::

    When specifying additional constraints, make sure they are respected
    by the data; otherwise, the output produced by HTSQL may be invalid.

Parameter ``not-nulls`` is a list of columns with ``NOT NULL``
constraints.

To indicate that all columns named ``code`` and ``id``, as well as
column ``student.full_name`` do not contain ``NULL`` value, write:

.. sourcecode:: yaml

    tweak.override:
      not-nulls: ["*.code", "*.id", student.full_name]

Note that we need to put the column patterns into quotes
since YAML syntax does not permit ``*`` character at the
beginning of a scalar value.

Parameter ``unique-keys`` is a list of key specifications
of the form ``<table>(<column>,...)[!]``.  The trailing
symbol ``!`` indicates a ``PRIMARY KEY`` constraint.
All columns in a primary key are marked as ``NOT NULL``.

To indicate that ``school.code`` is a primary key and
``school.name`` is unique, write:

.. sourcecode:: yaml

    tweak.override:
      unique-keys:
        - school(code)!
        - school(name)

Parameter ``foreign-keys`` is a list of foreign key
specifications, which have the form
``<origin>(<column>,...) -> <target>(<column>,...)``.
Target columns could be omitted when they coincide with
the target primary key.

To define two foreign keys on table ``program``, write:

.. sourcecode:: yaml

    tweak.override:
      foreign-keys:
        - program(school_code) -> school(code)
        - program(school_code, part_of_code) -> program

In HTSQL, database tables, columns and links have a *label*,
an identifier by which they are referred in HTSQL queries.
Normally, entity labels coincide with their names, but parameters
``class-labels`` and ``field-labels`` allow you to assign
them arbitrary labels.  In addition, these parameters allow
you to assign a label to an arbitrary HTSQL expression.

Use parameter ``class-labels`` to assign custom labels
to tables and top-level HTSQL expressions.  Parameter ``class-labels``
is a mapping; each key is a label, the corresponding value
is either a table name or an HTSQL expression enclosed in
parentheses.

To rename table ``classification`` to ``c14n`` and to assign
a label to expression ``school^campus``, write:

.. sourcecode:: yaml

    tweak.override:
      class-labels:
        c14n: classification
        campus: (school^campus)

Calculated classes defined by ``class-labels`` may accept
parameters.  For instance, to add a class ``students_by_year()``
which takes the year of admission as an argument, write:

.. sourcecode:: yaml

    tweak.override:
      class-labels:
        students_by_year($year): (student?year(start_date)=$year)

Use parameter ``field-labels`` to assign custom labels
to table fields.  This parameter is a mapping; each
key has a form ``<table>.<field>``, where ``<table>``
is the table label, ``<field>`` is the field label
to define.  The corresponding value is one of:

* a column name;
* a link specification;
* an HTSQL expression enclosed in parentheses.

A link specification is a comma-separated list of
patterns ``<origin>(<column>,...) -> <target>(<column>,...)``.
Each pattern must match a foreign key or a reverse foreign
key.  Column lists could be omitted if the foreign key
could be determined uniquely.

To rename a column ``student.name`` to ``full_name``, write:

.. sourcecode:: yaml

    tweak.override:
      field-labels:
        student.full_name: name

To add a many-to-many link between ``student`` and ``class``
via ``enrollment`` table, write:

.. sourcecode:: yaml

    tweak.override:
      field-labels:
        student.class: student -> enrollment, enrollment -> class
        class.student: class -> enrollment, enrollment -> student

Note that link specifier ``student -> enrollment`` uniquely
matches foreign key ``enrollment(student_id) -> student(id)``
while ``enrollment -> class`` matches foreign key
``enrollment(class_seq) -> class(class_seq)`` so we do not
need to provide column lists.

The self-referential link from ``program`` to all included
programs is called, by default, ``program.program_via_part_of``.
To assign a different label to this link, write:

.. sourcecode:: yaml

    tweak.override:
      field-labels:
        program.includes:
          program(school_code, code) -> program(school_code, part_of_code)

To define a calculated field ``student.avg_grade``, write:

.. sourcecode:: yaml

    tweak.override:
      field-labels:
        student.avg_grade: (avg(enrollment.grade))

Calculated fields may accept a parameter.  To define a calculated
field ``department.students_by_year()`` accepting the year of
admission as a parameter, write:

.. sourcecode:: yaml

    tweak.override:
      field-labels:
        department.students_by_year($year): (student?year(start_date)=$year)

By default, when an HTSQL query does not contain a selector
expression, all table columns are displayed.  To set a custom
list of fields for this case, use parameter ``field-orders``.

.. sourcecode:: yaml

    tweak.override:
      field-orders:
        program: [code, title, degree]

Parameter ``unlabeled-tables`` is a list of tables without an
assigned labels, which effectively hides the tables from the users.
The tables could still be used in SQL generated by the HTSQL
translator.

.. sourcecode:: yaml

    tweak.override:
      unlabeled-tables: [enrollment]

Parameter ``unlabeled-columns`` is a list of columns without
an assigned tables.  Unlabeled columns are hidden from the users,
but could be used in SQL generated by the HTSQL translator.

To hide all ``id`` columns, write:

.. sourcecode:: yaml

    tweak.override:
      unlabeled-columns: [id, "*_id"]

.. **

Use parameter ``globals`` define global attributes and functions.
This parameter is a mapping: each key is the attribute name with
an optional list of parameters, the value is an HTSQL expression.

.. sourcecode:: yaml

    tweak.override:
      globals:
        num_school: (count(@school))
        trunc_month($d): (date(year($d), month($d), 1))

.. index:: tweak.resource

``tweak.pool``
--------------

This addons caches open database connections so that the same
connection could be reused to execute more than one query.
Use this addon with backends where opening a database connection
is an expensive operation.

``tweak.resource``
------------------

This extension adds a mechanism for serving static files via HTTP.
This mechanism is used by other extensions to provide access to
static resources such as Javascript and CSS files.

Parameters:

`indicator`
    HTTP root for static files, excluding leading and trailing ``/``
    (default: ``-``)

.. index:: tweak.shell
.. _tweak.shell:

``tweak.shell``
---------------

This extension adds an in-browser HTSQL editor called the HTSQL shell.
The shell provides a visual query editor (based on CodeMirror_) with
support for syntax highlighting and code completion.

The shell is invoked by command ``/shell()``, which takes an optional
query to edit.

Parameters:

`server-root`
    The root URL of the HTSQL server (default: guess)
`limit`
    Truncation threshold for shell output (default: 1000)

.. sourcecode:: yaml

    tweak.shell:
      server-root: http://demo.htsql.org
      limit: 100

Enable addon ``tweak.shell.default`` to make the shell the default
output format.

.. sourcecode:: yaml

    tweak.shell.default:

.. _CodeMirror: http://codemirror.net/

.. index:: tweak.sqlalchemy, SQLAlchemy

.. _tweak.sqlalchemy:

``tweak.sqlalchemy``
--------------------

This extension provides integration with SQLAlchemy_ toolkit.
It replaces built-in HTSQL database introspection and
connection handling with SQLAlchemy facilities.

When using ``tweak.sqlalchemy`` addon, do not specify the
connection URI as it is determined from the SQLAlchemy
engine settings.

Parameters:

`engine`
    The SQLAlchemy engine object.
`metadata`
    The SQLAlchemy metadata object.

The value must have the form ``<module>.<attr>`` or
``<package>.<module>.<attr>``.

.. sourcecode:: yaml

    tweak.sqlalchemy:
      engine: sademo.engine
      metadata: sademo.metadata

.. _SQLAlchemy: http://www.sqlalchemy.org/

.. index:: tweak.timeout
.. _tweak.timeout:

``tweak.timeout``
-----------------

This extension limits query execution to a given amount
of time (1 minute by default).  Use it to ensure
against accidental denial of service caused by complex
queries.

Parameters:

`timeout`
    The timeout value, in seconds (default: 60).

.. sourcecode:: yaml

    tweak.timeout:
      timeout: 300

Currently, this addon is only supported with PostgreSQL.


.. vim: set spell spelllang=en textwidth=72:
