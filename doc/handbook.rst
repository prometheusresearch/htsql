*******************
  HTSQL Handbook
*******************

.. highlight:: console

.. contents:: Table of Contents
   :depth: 1
   :local:

This handbook presumes you have :doc:`installed <admin/install>` HTSQL.
It also assumes that you have a GNU/Linux based system, although other
systems might also work.  If you need help, please see our `HTSQL
Community <http://htsql.org/community/>`_ page for assistance.

Up & Running
=============

To verify your installation, try ``htsql-ctl``'s ``version`` routine::

  $ htsql-ctl version
  ...

If ``htsql-ctl`` isn't found or it doesn't work, you have an
installation issue. 

Getting Familiar
----------------

To get started, you could test queries with our ``htsql_demo`` demo
SQLite database.  To get a copy of this, download it using ``wget`` or
some other tool::

   $ wget -q http://dist.htsql.org/misc/htsql_demo.sqlite

Then use ``htsql-ctl shell`` to walk through our
:doc:`overview <overview>` and/or :doc:`tutorial <tutorial>`::

   $ htsql-ctl shell sqlite:htsql_demo.sqlite
   Type 'help' for more information, 'exit' to quit the shell
   htsql_demo$ /school
       school                                       
       ---------------------------------------------
       code | name                          | campus
       -----+-------------------------------+-------
       art  | School of Art & Design        | old   
       bus  | School of Business            | south 
       edu  | College of Education          | old   
       ...

There is a ``describe`` command within this ``shell`` which lists
tables, or, if you provide a table, its columns and links::

   htsql_demo$ describe school
       Slots for `school` are:
       code       VARCHAR(16)
       name       VARCHAR(64)
       campus     VARCHAR(5)
       department PLURAL(department)
       program    PLURAL(program)

This ``shell`` command has schema-based completion.  For example, if you
type ``/s`` and then press *TAB*, it will list all of of the possibe
completions: ``school``, ``semester``, and ``student``.   For more
information, please see the :ref:`htsql-ctl reference <htsql-ctl>`.

Test Drive
----------

To attach HTSQL to your database, you'll need a :ref:`Database URI
<dburi>` which take the following form::

   <engine>://<user>:<pass>@<host>:<port>/<database>

For this example, we'll use the ``pgsql`` engine on a local demo
database using the ``-p`` option to prompt for a password.  The 
exact connection details will depend upon your local configuration::
   
   $ htsql-ctl shell -p pgsql://demo@localhost:5432/htsql_demo
   Password: ******
   Type 'help' for more information, 'exit' to quit the shell.
   htsql_demo$ describe
       Tables introspected for this database are:
       course
       department
       program
       ...

If it seems links arn't working properly, you could verify links for a
specific table using ``describe``::

   htsql_demo$ describe department
       Slots for `department` are:
       code        VARCHAR(16)
       name        VARCHAR(64)
       school_code VARCHAR(16)
       school      SINGULAR(school)
       appointment PLURAL(appointment)
       course      PLURAL(course)

You should see ``SINGULAR`` links for foreign key references in this
table to other tables and ``PLURAL`` links for foreign keys in other
tables that reference this one.   In this example, we see that
``department`` is singular to ``school`` and plural to ``course``.

If links arn't introspected, you've got a few options.  The best option
is to create them in your database if they don't exist (this isn't an
option for MyISAM).  Otherwise, you have a few configuration options, 
including manually specifying links or bridging relationship detail 
from a SQLAlchemy or Django model.

Web Service
-----------

Besides ``shell``, the ``htsql-ctl`` program provides a built-in
*demonstration* :ref:`webserver <htsql-ctl serve>`.  You could start it
as follows::

   $ htsql-ctl serve sqlite:htsql_demo.sqlite
       Starting an HTSQL server on localhost:8080 over htsql_demo.sqlite

Then, it might be accessed using any user agent, such as ``wget``::

   $ wget -q -O - --header='Accept: text/csv' http://localhost:8080/school
       code,name,campus
       art,School of Art & Design,old
       bus,School of Business,south
       edu,College of Education,old
       ...

On http://demo.htsql.org, we enable a :ref:`tweak.shell` extension::

    $ htsql-ctl serve -E tweak.shell.default sqlite:htsql_demo.sqlite
        Starting an HTSQL server on localhost:8080 over htsql_demo.sqlite
  
You could then navigate to http://localhost:8080 with your web browser
and type in queries there.  This plugin replaces the default HTML
formatter with our visual shell.  If you press ``CTRL+SPACE`` it should
bring up a context sensitive menu item.

HTSQL Extensions
================

Everything is an Extension
--------------------------

For HTSQL, everything (even database adapters) are plugins that are
independently installed, loaded and configured.  Extensions can be
loaded on the command line using ``-E`` or in a configuration file
format.  You could list installed extensions at the command line::

    $ htsql-ctl extension
        Available extensions:
        engine          :  provides implementations of HTSQL for specific servers
        engine.mysql    : implements HTSQL for MySQL
        engine.pgsql    : implements HTSQL for PostgreSQL
        engine.sqlite   : implements HTSQL for SQLite
        htsql           : HTSQL translator and HTTP service
        tweak           : contain various tweaks for HTSQL
        tweak.autolimit : limit number of rows returned by queries
        ...

One handy extension is :ref:`tweak.autolimit` which limits the number of
rows returned by default.  Using this plugin lets you explore tables
with lots of rows without having to constantly add ``.limit(n)`` to each
of your queries.  In this example, we set the ``limit`` to 5 rows::
  
    $ htsql-ctl shell -E tweak.autolimit:limit=5 sqlite:htsql_demo.sqlite
    Type 'help' for more information, 'exit' to quit the shell.
    htsql_demo$ /count(department)
         | count(department) |
        -+-------------------+-
         |                27 |
                   (1 row)
    htsql_demo$ /department
         | department                             |
         +----------------------------------------+
         | code   | name            | school_code |
        -+--------+-----------------+-------------+-
         | acc    | Accounting      | bus         |
         | arthis | Art History     | art         |
         | astro  | Astronomy       | ns          |
         | be     | Bioengineering  | eng         |
         | bursar | Bursar's Office |             |
                                           (5 rows)

One of the more interesting plugins is :ref:`tweak.meta`.  This adds a
in-memory SQLite database with table and link detail based upon the
current configuration, and a function ``meta()`` to let you query it::

    $ htsql-ctl shell -E tweak.meta sqlite:htsql_demo.sqlite
    Type 'help' for more information, 'exit' to quit the shell.
    htsql_demo$  /meta(/link{name, is_singular}?table_name='school')
         | link                     |
         +--------------------------+
         | name       | is_singular |
        -+------------+-------------+-
         | department | false       |
         | program    | false       |
                             (2 rows)

The PostgreSQL specific :ref:`tweak.timeout` plugin provides a way to
automatically kill expensive queries after a specified number of seconds
have elapsed::

    $ htsql-ctl shell -E tweak.timeout:timeout=3 pgsql:htsql_demo
    Type 'help' for more information, 'exit' to quit the shell.
    htsql_demo$  /count(enrollment.fork().fork())
    engine failure: failed to execute database query:
    canceling statement due to statement timeout

The ``enrollment`` table has 15k rows, and ``fork()`` associates each
row with every row of the same table (a CROSS JOIN).  Hence, this query
would count 15K^3 rows.  Having a query like this auto killed after 3s
is a great way to keep everyone happy.

Extension Configuration
-----------------------

Addons and :ref:`configuration <configuration>` parameters can also be
provided by a configuration file in YAML_ (or JSON_) format and then
included using ``-C`` on the command line.  Here is an example
configuration file for a PostgreSQL database with some addons enabled.

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
    tweak.cors:
    tweak.meta:
    tweak.shell:
      server-root: http://demo.htsql.org
    tweak.shell.default:
    tweak.timeout:
      timeout: 600

You can then start the shell using these parameters::

  $ htsql-ctl serve -C demo-config.yaml

If both ``-E`` and ``-C`` are used, explicit command line options override
values provided in the configuration file.  This permits a configuration
file to be used as a default perhaps using a different database URI.

.. _YAML: http://yaml.org/
.. _JSON: http://json.org/


MetaData Configuration
======================

The :ref:`tweak.override` plugin provides comprehensive control over the
HTSQL system catalog.  


.. note:: 
   
    For more information about configuring and using HTSQL, please
    see our :doc:`admin/usage` guide.


