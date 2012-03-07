********************
  Installing HTSQL
********************

.. contents:: Table of Contents
   :depth: 1
   :local:

.. highlight:: console


Binary Packages
===============

The easiest way to install HTSQL is to use a binary package.
We provide binary packages for various Linux platforms, available
at http://htsql.org/download/.


Install from Source
===================

To install HTSQL from source, use the pip_ package manager.  Run::

    # pip install HTSQL

pip_ will download, build and install HTSQL and all its dependencies.

HTSQL works out of the box with SQLite databases.  To run HTSQL
on top of other database servers, you need to install additional
database backends.

To install a *PostgreSQL* backend, run::

    # pip install HTSQL-PGSQL

To install a *MySQL* backend, run::

    # pip install HTSQL-MYSQL

To install an *Oracle* backend, run::

    # pip install HTSQL-ORACLE

To install a backend for *Microsoft SQL Server*, run::

    # pip install HTSQL-MSSQL

.. _pip: http://pypi.python.org/pypi/pip


.. vim: set spell spelllang=en textwidth=72:
