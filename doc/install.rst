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

1. Install Python_, the pip_ package manager and required Python modules
   (setuptools_ and pyyaml_)::

        # apt-get install python python-pip
        # apt-get install python-setuptools python-yaml

2. Install database adapters.

   * SQLite requires no additional drivers.

   * For PostgreSQL, install psycopg2_::

        # apt-get install python-psycopg2

   * For MySQL, install `MySQL-python`_::

        # apt-get install python-mysqldb

   * For MS SQL Server, install `pymssql`_::

        # apt-get install python-pymssql

   * For Oracle, download and install `Oracle Instant Client`_ from
     http://oracle.com/, then download, build and install cx_Oracle_.
     The latter could be done with the pip_ package manager::

        # pip install cx-oracle

3. Download, build and install HTSQL, either from a package or from
   `HTSQL source`_ repository.

   * To install the latest released version of HTSQL, use pip_::

        # pip install HTSQL

   * To use the latest development version of HTSQL, install Mercurial_,
     download `HTSQL source`_, then build and install HTSQL::

        # apt-get install mercurial
        $ hg clone http://bitbucket.org/prometheus/htsql
        $ cd htsql
        $ make build
        # make install

4. The last step creates an executable ``htsql-ctl``.  For general
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

   For other databases, ``DBURI`` has the form:

   .. sourcecode:: text

        engine://user:pass@host:port/database

   Here, ``engine`` specifies the type of the database server, and must be one
   of: ``sqlite``, ``pgsql``, ``mysql``, ``mssql``, ``oracle``.  ``database``
   is the name of the database to connect to.  The components ``host:port``
   indicate the address of the database server, ``user:pass`` are
   authentication parameters.  Both ``user:pass`` and ``host:port`` components
   are optional and could be omitted.

.. _Python: http://python.org/
.. _pip: http://pypi.python.org/pypi/pip
.. _setuptools: http://pypi.python.org/pypi/setuptools
.. _pyyaml: http://pypi.python.org/pypi/PyYAML
.. _sqlite3: http://docs.python.org/library/sqlite3.html
.. _psycopg2: http://pypi.python.org/pypi/psycopg2
.. _MySQL-python: http://pypi.python.org/pypi/MySQL-python
.. _pymssql: http://pypi.python.org/pypi/pymssql
.. _Oracle Instant Client: http://www.oracle.com/technetwork/database/features/instant-client/index.html
.. _cx_Oracle: http://pypi.python.org/pypi/cx_Oracle
.. _Mercurial: http://mercurial.selenic.com/
.. _HTSQL source: http://bitbucket.org/prometheus/htsql


Installation
============

Installing Prerequisites
------------------------

HTSQL requires Python 2.5 or newer, but does not yet support Python 3.
Python 2.7 is the recommended version.  In most distributions, Python
is already installed; if not, install it by running::

    # apt-get install python

Installation of Python modules that have no system packages requires
the pip_ package manager::

    # apt-get install python-pip

HTSQL depends on the following Python libraries:

* setuptools_ (``0.6c9`` or newer);
* pyyaml_ (``3.07`` or newer);

In addition, HTSQL requires database drivers:

* SQLite is supported out of the box;
* psycopg2_ (``2.0.10`` or newer), for PostgreSQL;
* MySQL-python_ (``1.2.2`` or newer), for MySQL;
* pymssql_ (``1.0.2`` or newer), for MS SQL Server;
* cx_Oracle_ (``5.0`` or newer), for Oracle.

To install the dependencies, run::

    # apt-get install python-setuptools python-yaml

You can install database drivers using system packages (when possible)
or from source, using the pip_ package manager.  Installing drivers from
source requires a C compiler and Python header files, which can be
installed with::

    # apt-get install build-essential python-dev

To install the PostgreSQL driver from the system repository, run::

    # apt-get install python-psycopg2

To install the driver from source, first install the PostgreSQL client
library::

    # apt-get install libpq5

Then run::

    # pip install psycopg2

To install the MySQL driver from the system repository, run::

    # apt-get install python-mysqldb

To install the same driver from source, run::

    # apt-get install libmysqlclient16
    # pip install mysql-python

To install the MS SQL Server driver from the system repository, run::

    # apt-get install python-pymssql

To install the same driver from source, run::

    # apt-get install libsybdb5
    # pip install pymssql

Oracle drivers usually not packaged with the system.  To install the
drivers, first download and install `Oracle Instant Client`_.  Then
build and install the Python driver::

    # pip install cx-oracle

Installing HTSQL
----------------

To install the latest released version of HTSQL, run::

    # pip install HTSQL

If you want to closely follow development of HTSQL, we recommend
installing HTSQL directly from the `HTSQL source`_ repository.  You need
a Mercurial client::

    # apt-get install mercurial

To download `HTSQL source`_::

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

HTSQL comes with a comprehensive suite of regression tests.  Running the
tests requires a working database server for each of the supported database
backends.  To specify connection parameters to the test servers, copy
the file ``Makefile.env.sample`` to ``Makefile.env`` and edit the latter.
For example, to to set the credentials of an administrative user for
a PostgreSQL database, edit parameters ``PGSQL_ADMIN_USERNAME`` and
``PGSQL_ADMIN_PASSWORD``; to set the address of the database server,
edit parameters ``PGSQL_HOST`` and ``PGSQL_PORT``.

To run the tests::

    $ make test

To run the tests against a specific database backend (e.g. SQLite), run::

    # make test-sqlite

Running regression tests creates a database ``htsql_regress`` and a
database user with the same name.

To learn other ``make`` targets, run::

    $ make


Usage
=====

The ``htsql-ctl`` Executable
----------------------------

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

Database Connection
-------------------

Many routines require a ``DBURI`` parameter, which specifies how to
connect to a database.  ``DBURI`` has the form:

.. sourcecode:: text

    engine://user:pass@host:port/database

* ``engine`` is the type of the database server; ``sqlite`` for SQLite,
  ``pgsql`` for PostgreSQL, ``mysql`` for MySQL, ``mssql`` for MS SQL Server,
  ``oracle`` for Oracle.
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

Other database servers use similar conventions.

You can use option ``-p`` to prompt for a password if you do not want
to specify the database password in a command line.

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


Deployment
==========

The built-in HTSQL web server was designed for personal and testing use
and may appear inadequate for production deployment.  In particular,
it does not not provide any means for authentication and lacks SSL support.

Integration with Apache
-----------------------

It is possible to integrate HTSQL with `Apache HTTP Server`_ using
mod_wsgi_.  Here we assume that both Apache and mod_wsgi are already
installed.

First, create a WSGI script file:

.. sourcecode:: python

   from htsql import HTSQL

   # The address of the database in the form:
   #   engine://user:pass@host:port/database
   DB = '...'

   application = HTSQL(DB)

Save this file as ``htsql.wsgi`` and place it to a directory
accessible by Apache (but do not put it below the root of the web
site so that it cannot be downloaded).

Next, add the following line to the Apache configuration file:

.. sourcecode:: apache

   WSGIScriptAlias /htsql /path/to/htsql.wsgi

This line should be added to the ``VirtualHost`` section of the respective
web site.  It associates any URL starting with ``/htsql`` with the HTSQL
server.

For more information of installing and configuring Apache and mod_wsgi,
see documentation for the respective projects, in particular,
`Quick Configuration Guide for mod_wsgi`_.

.. _Apache HTTP Server: http://httpd.apache.org/
.. _mod_wsgi: http://code.google.com/p/modwsgi/
.. _Quick Configuration Guide for mod_wsgi:
    http://code.google.com/p/modwsgi/wiki/QuickConfigurationGuide


Security
========

Giving HTSQL access is practically equivalent to giving an access to
a read-only SQL console and should be planned accordingly.

HTSQL, as a gateway between HTTP server and a database server, does
not provide any security mechanisms.  Any protection should be set
up on either the HTTP or the database layers.  On the HTTP layer,
you may put the HTSQL server behind an HTTP server or a proxy
to provide SSL, authentication and caching.  On the database layer,
you may restrict access to selected database entities using roles and
permissions.

With a proper setup, data leaks should be impossible.  Another
potential vector of attack is overloading the database server,
against which we recommend setting up an HTTP caching layer and
restricting resource usage for the HTSQL database user.


