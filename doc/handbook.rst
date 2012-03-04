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

To verify your installation, try the ``htsql-ctl`` command line
utility's ``version`` routine::

  $ htsql-ctl version
  ...

If ``htsql-ctl`` isn't found or it doesn't work, you have an
installation issue.

First Steps
-----------

To get started, you could test queries with our ``htsql_demo`` demo
SQLite database.  To get a copy of this, download it using ``wget`` or
some other tool::

   $ wget http://htsql.org/dist/htsql_demo.sqlite

Then use ``htsql-ctl shell`` to browse, trying queries from the
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
completions: ``school``, ``semester``, and ``student``. 



.. note::
   
   This handbook is currently under construction.  You could refer
   to :doc:`admin/usage` for additional material.

