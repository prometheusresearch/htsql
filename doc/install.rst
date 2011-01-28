*****************************************
  Installation and Administration Guide
*****************************************

.. highlight:: console

The following instructions assume a recent Debian_ or `Debian-derived`_
system, but could be easily adapted to other Linux distributions and
package managers.

.. _Debian: http://debian.org/
.. _Debian-derived: http://ubuntu.com/


Quick Start
===========

1. Install Python_ and required Python modules (setuptools_, pyyaml_,
   psycopg2_)::

        # apt-get install python
        # apt-get install python-setuptools python-yaml python-psycopg2

2. Install Mercurial_ and download `HTSQL source code`_::

        # apt-get install mercurial
        $ hg clone http://bitbucket.org/prometheus/htsql

3. Build and install HTSQL::

        $ cd htsql
        $ make build
        # make install

4. The previous step creates an ``htsql-ctl`` executable.  For general
   help and a list of commands, run::

        $ htsql-ctl help

   To start a command-line HTSQL shell, run::

        $ htsql-ctl shell DBURI

   To start an HTTP server running HTSQL on the address ``HOST:PORT``,
   run::

        $ htsql-ctl server DBURI [HOST [PORT]]

   The parameter ``DBURI`` specifies how to connect to a database.  For
   a SQLite database, ``DBURI`` has the form:

   .. sourcecode:: text

        sqlite:/path/to/database

   For a PostgreSQL database, ``DBURI`` has the form:

   .. sourcecode:: text

        pgsql://user:pass@host:port/database

   Both ``user:pass`` and ``host:port`` components are optional.

.. _Python: http://python.org/
.. _setuptools: http://pypi.python.org/pypi/setuptools
.. _pyyaml: http://pypi.python.org/pypi/PyYAML
.. _psycopg2: http://pypi.python.org/pypi/psycopg2
.. _Mercurial: http://mercurial.selenic.com/
.. _HTSQL source code: http://bitbucket.org/prometheus/htsql


Installation
============

Installing Prerequisites
------------------------

HTSQL requires Python 2.5 or newer, but does not yet support Python 3.
Python 2.6 is the recommended version.  In most distributions, Python
is already installed; if not, install it by running::

    # apt-get install python

HTSQL depends on the following Python libraries:

* setuptools_ (``0.6c9`` or newer);
* pyyaml_ (``3.07`` or newer);
* psycopg2_ (``2.0.10`` or newer).

To install the libraries, run::

    # apt-get install python-setuptools python-yaml python-psycopg2

Installing HTSQL
----------------

Since HTSQL is still at an early stage of development, we recommend
installing HTSQL directly from the `HTSQL source repository`_.  You need
a Mercurial client::

    # apt-get install mercurial

To download `HTSQL source code`_::

    $ hg clone http://bitbucket.org/prometheus/htsql

To build and install HTSQL, run::

    $ cd htsql
    $ make build
    # make install

That installs the HTSQL executable ``htsql-ctl`` to ``/usr/local/bin``
and HTSQL library files to ``/usr/local/lib``.

To install HTSQL in a development mode, run::

    # make develop

When HTSQL is installed in the development mode, any changes in the
source files are reflected immediately without need to reinstall.

HTSQL comes with a comprehensive suite of regression tests.  By default,
the suite assumes that a PostgreSQL instance is running on the same machine
and the current user has administrative permissions.  To run the tests::

    $ make test

Connection parameters to the test server could be specified explicitly.  Copy
the file ``Makefile.env.sample`` to ``Makefile.env`` and open the latter.  To
set the credentials of an administrative user, update parameters
``PGSQL_ADMIN_USERNAME`` and ``PGSQL_ADMIN_PASSWORD``.  To set the address of
the database server, update parameters ``PGSQL_HOST`` and ``PGSQL_PORT``.

Running regression tests creates a PostgreSQL database ``htsql_regress`` and a
database user with the same name.  To remove any database users and databases
deployed by the regression tests, run::

    $ make cleanup

To learn other ``make`` targets, run::

    $ make

.. _HTSQL source repository: http://bitbucket.org/prometheus/htsql


Usage
=====

The ``htsql-ctl`` Executable
----------------------------

Installing HTSQL creates an ``htsql-ctl`` command-line application::

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

Database Connection
-------------------

Many routines require a ``DBURI`` parameter, which specifies how to
connect to a database.  ``DBURI`` has the form:

.. sourcecode:: text

    engine://user:pass@host:port/database

* ``engine`` is the type of the database server; ``sqlite`` for SQLite,
  ``pgsql`` for PostgreSQL;
* ``user:pass`` are authentication parameters;
* ``host:port`` is the address of the database server;
* ``database`` is the name of the database.

For SQLite, ``user:pass`` and ``host:port`` are omitted, and ``database``
specifies the path to the database file.  Thus, for SQLite, ``DBURI`` has
the form:

.. sourcecode:: text

    sqlite:/path/to/database

For PostgreSQL, if ``user:pass`` is omitted, the credentials of the
current user are used; if ``host:port`` is omitted, the server is
assumed to run on the local machine.  Thus, to connect to a database
running on the same host under credentials of the current user, use
the form:

.. sourcecode:: text

    pgsql:database

Command-line Shell
------------------

To start a command-line HTSQL shell, run::

    $ htsql-ctl shell DBURI

That starts an interactive HTSQL shell, where you could type and execute
HTSQL queries against the specified database.

For more details on the ``shell`` routine, run::

    $ htsql-ctl help shell

HTTP Server
-----------

To start an HTTP server running HTSQL, run::

    $ htsql-ctl server DBURI [HOST [PORT]]

That starts an HTTP server on the address ``HOST:PORT``.  If ``HOST``
and ``PORT`` are omitted, the server is started on ``*:8080``.

For more details on the ``server`` routine, run::

    $ htsql-ctl help server


