********************
  Installing HTSQL
********************

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


.. vim: set spell spelllang=en textwidth=72:
