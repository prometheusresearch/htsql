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
utility's ``version`` or ``help`` routine::

  $ htsql-ctl version
  ...

If ``htsql-ctl`` isn't found or it doesn't work, you have an
installation issue.

First Steps
-----------

The HTSQL interpreter requires a database to be useful.  You could test
things out with our ``htsql_demo`` demo SQLite database.  You could
download this database::

   $ wget http://htsql.org/dist/htsql_demo.sqlite

The ``get`` routine of ``htsql-ctl`` could be used to list the data
found in a table, such as ``school``::

   $ htsql-ctl get sqlite:htsql_demo.sqlite /school
   ...

If this returns an error, you could enumerate tables, using ``describe``
routine to see if the introspection was successful::

   $ htsql-ctl describe sqlite:htsql_demo.sqlite

This command ``shell`` has limited schema-based completion.  So, you
could type ``/s`` then the TAB character to list tables that start 
with ``s``.  At this point, you should be able to walk through our
:doc:`overview <overview>` and/or :doc:`tutorial <tutorial>` with your
local installation.

