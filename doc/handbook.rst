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
things out with our ``htsql_demo`` demo SQLite database.  To get a copy
of this, you could download it using ``wget`` or some other tool::

   $ wget http://htsql.org/dist/htsql_demo.sqlite

For starters, you could use the ``get`` routine of ``htsql-ctl`` to list
the contents of a given table, such as ``school``::

   $ htsql-ctl get sqlite:htsql_demo.sqlite /school
   ...

At this point, you could then use ``htsql-ctl shell`` to try examples in
our :doc:`overview <overview>` and/or :doc:`tutorial <tutorial>`::

   $ htsql-ctl shell sqlite:htsql_demo.sqlite

The ``shell`` command has limited schema-based completion.  For example,
if you type ``/s`` and then press the TAB character, it will list all of
of the possibe completions: ``school``, ``semester``, and ``student``.

