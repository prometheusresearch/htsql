******************************
  HTSQL Installation & Usage
******************************

.. highlight:: console


Binary Packages
===============

We provide binary packages for various Linux platforms.  They are
available at http://htsql.org/download.html.


Installing from Source
======================

The following instructions assume a recent Debian_ or `Debian-derived`_
system, but could be easily adapted to other Linux distributions and
package managers.

.. _Debian: http://debian.org/
.. _Debian-derived: http://ubuntu.com/

Prerequisites
-------------

HTSQL requires Python 2.5 or newer, but does not yet support Python 3.
Python 2.7 is the recommended version.  In most Linux distributions,
Python is already installed; if not, install it by running::

    # apt-get install python

Installation of Python modules that have no system packages requires
the pip_ package manager::

    # apt-get install python-pip

HTSQL needs setuptools_ and pyyaml_ libraries::

    # apt-get install python-setuptools python-yaml

Furthermore, some database backends require an additional database
driver.

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

Installing HTSQL
----------------

Download, build and install HTSQL, either from a source package
or from `HTSQL source`_ repository.

* To install the latest released version of HTSQL, use pip_::

    # pip install HTSQL

* To use the latest development version of HTSQL, install Mercurial_,
  download `HTSQL source`_, then build and install HTSQL::

    # apt-get install mercurial
    $ hg clone http://bitbucket.org/prometheus/htsql
    $ cd htsql
    # make install

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

.. _dburi:

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


.. vim: set spell spelllang=en textwidth=72:
