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

   $ wget http://htsql.org/dist/htsql_demo.sqlite

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
   htsql_demo$ exit

There is a ``describe`` command within this ``shell`` which lists
tables, or, if you provide a table, its columns and links::

   $ htsql-ctl shell sqlite:htsql_demo.sqlite
   htsql_demo$ describe school
       Slots for `school` are:
       code       VARCHAR(16)
       name       VARCHAR(64)
       campus     VARCHAR(5)
       department PLURAL(department)
       program    PLURAL(program)
   htsql_demo$ exit

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
database using the ``-p`` option to prompt for a password.  The exact
connection details will depend upon your local configuration::
   
   $ htsql-ctl shell -p pgsql://demo@localhost:5432/htsql_demo
   Password: ******
   Type 'help' for more information, 'exit' to quit the shell.
   htsql_demo$ describe
       Tables introspected for this database are:
       course
       department
       program
       ...

To test a few queries, it is often handy to limit the number of rows
returned by default.  This can be done with the ``tweak.autolimit``
extension.  In this case, we set it to 5 rows::
  
    $ htsql-ctl shell -E tweak.autolimit:limit=5 sqlite:htsql_demo.sqlite
    Type 'help' for more information, 'exit' to quit the shell.
    htsql_demo$ /count(department)
        count(department)
        -----------------
                       27
        (1 row)
    htsql_demo$ /department
        department
        --------------------------------------
        code   | name            | school_code
        -------+-----------------+------------
        acc    | Accounting      | bus        
        arthis | Art History     | art        
        astro  | Astronomy       | ns         
        be     | Bioengineering  | eng        
        bursar | Bursar's Office |            
        (5 rows)

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
is to create them in your database if they don't exist.  Otherwise, you
have a few configuration options, including manually specifying links or
bridging link detail from a SQLAlchemy or Django model.

Basic Configuration
-------------------

Typically, you'll want to put your connection information as well as
other configuration options into a flat file.  For more information,
please see :doc:`admin/usage`.

