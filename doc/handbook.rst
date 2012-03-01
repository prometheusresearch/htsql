*******************
  HTSQL Handbook
*******************

.. highlight:: console

.. contents:: Table of Contents
   :depth: 1
   :local:

This handbook presumes you have :doc:`installed <admin/install>` HTSQL
and have had a chance to skim our :doc:`overview <overview>` to get a
taste of HTSQL.  We also assume that you have a GNU/Linux based system,
although Cygwin, OSX or FreeBSD might work with only a few tweaks. 

If you need help, please see our `HTSQL Community
<http://htsql.org/community/>`_ page for assistance.

Up & Running
=============

In this section we work through the basics of ``htsql-ctl``.  For
starters, you should be able to print the ``version`` information 
and then ``help``::

  $ htsql-ctl version
  ...

  $ htsql-ctl help
  ...

If ``htsql-ctl`` isn't found or it doesn't work, then you have an
installation issue and should fix that first.

First Steps
-----------

The HTSQL interpreter requires a database, passed as a database URI on
the command line.  The built-in SQLite database adapter has an empty,
in-memory database, called ``:memory:``.  You could connect to it, and
then type ``/'Hello World'`` to run your first query::

  $ htsql-ctl shell sqlite::memory:
  :memory:$ /'Hello World'
  ...
  :memory:$ quit

The next step might be to download the ``htsql_demo`` database so that
you could walk through :doc:`overview` and :doc:`tutorial <tutorial>`
examples as you might wish::

   $ fetch http://htsql.org/dist/htsql_demo.sqlite
   $ htsql-ctl shell sqlite:htsql_demo.sqlite
   htsql_demo.sqlite$ /school
   ...

The command ``shell`` has limited schema-based completion.  So, you
could type ``/s<TAB>`` to see tables that start with ``'s'`` which
include ``school``, ``semester``, and ``student``.


