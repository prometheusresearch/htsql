********************************
  HTSQL in Python Applications
********************************

.. contents:: Table of Contents
   :depth: 1
   :local:

HTSQL is a Python library which can be used to make HTSQL requests
directly from Python applications.


Installation
============

You can install HTSQL with `pip`_ or `easy_install`_ package manager.
For example, to install HTSQL using pip, run:

.. sourcecode:: console

   # pip install HTSQL

Alternatively, you can download `HTSQL source package`_ and install
it manually.

You also need to install a database driver specific to the database
server you are using.  HTSQL requires the following driver libraries:

SQLite
    `sqlite3`_ (built-in Python module)
PostgreSQL
    `psycopg2`_
MySQL
    `MySQL-python`_
Oracle
    `cx_Oracle`_
Microsoft SQL Server
    `pymssql`_

.. _pip: http://pypi.python.org/pypi/pip
.. _easy_install: http://peak.telecommunity.com/DevCenter/EasyInstall
.. _HTSQL source package: http://pypi.python.org/pypi/HTSQL
.. _sqlite3: http://docs.python.org/library/sqlite3.html
.. _psycopg2: http://pypi.python.org/pypi/psycopg2
.. _MySQL-python: http://pypi.python.org/pypi/MySQL-python
.. _cx_Oracle: http://pypi.python.org/pypi/cx_Oracle
.. _pymssql: http://pypi.python.org/pypi/pymssql


Quick Start
===========

Start with creating an instance of class :class:`htsql.HTSQL`.  Pass
the address of the database as the argument of the class constructor:

.. sourcecode:: python

    >>> from htsql import HTSQL
    >>> htsql = HTSQL("pgsql:///htsql_demo")

To execute an HTSQL request and get output rows, use method
:meth:`HTSQL.produce()`:

.. sourcecode:: python

    >>> rows = htsql.produce("/school{name, count(department)}")

:meth:`HTSQL.produce()` returns an iterator emitting output rows.
You could access individual fields of an output row either by name
(when it is specified) or by position:

.. sourcecode:: python

    >>> for row in rows:
    ...     print "%s: %s" % (row.name, row[1])
    ...
    School of Art and Design: 2
    School of Business: 3
    College of Education: 2
    School of Engineering: 4
    School of Arts and Humanities: 5
    School of Music & Dance: 4
    School of Natural Sciences: 4
    Public Honorariums: 0
    School of Continuing Studies: 0

It is easy to pass parameters to the query:

.. sourcecode:: python

    >>> for row in htsql.produce("/department{name}?school.code=$school_code",
    ...                          school_code='ns'):
    ...     print row
    ...
    department(name=u'Astronomy')
    department(name=u'Chemistry')
    department(name=u'Mathematics')
    department(name=u'Physics')

In this example, the parameter ``school_code`` is available in the query
as a reference ``$school_code``.


Reference
=========

.. py:class:: htsql.HTSQL(db, *addons)

    Creates an HTSQL instance.

    `db` (a string, a dictionary or ``None``)
        The address of the database.

    `addons` (a dictionary ``{ addon: { parameter: value } }``)
        Plugins and plugin parameters.

    Parameter `db` specifies connection parameters to the database and
    must be either a string or a dictionary.  If `db` is a string, it must
    have the form of connection URI::

        <engine>://<username>:<password>@<host>:<port>/<database>

    ``<engine>``
        the type of the database server, one of ``sqlite``, ``pgsql``,
        ``mysql``, ``oracle``, ``mssql``;
    ``<username>:<password>``
        authentication credentials;
    ``<host>:<port>``
        address of the server;
    ``<database>``
        the name of the database.

    All parameters except ``<engine>`` and ``<database>`` are optional.

    Examples:

    Connect to a local PostgreSQL database ``htsql_demo`` with the
    credentials of the current system user:

    .. sourcecode:: python

        >>> htsql = HTSQL('pgsql:htsql_demo')

    Connect to a MySQL server running on host ``10.0.0.1`` with the
    username ``root`` and password ``admin``:

    .. sourcecode:: python

        >>> htsql = HTSQL('mysql://root:admin@10.0.0.1/htsql_demo')

    Connect to a SQLite database ``build/regress/sqlite/htsql_demo.sqlite``:

    .. sourcecode:: python

        >>> htsql = HTSQL('sqlite:///build/regress/sqlite/htsql_demo.sqlite')

    Alternatively, the database address could be passed as a dictionary with
    keys ``'engine'``, ``'username'``, ``'password'``, ``'host'``, ``'port'``,
    ``'database'``.  For example,

    .. sourcecode:: python

        >>> htsql = HTSQL({'engine': 'pgsql', 'database': 'htsql_demo'})

    Parameter `addons` allows you to extend HTSQL with additional
    functionality provided by plugins.  This parameter is a dictionary;
    the keys are addon names, the value is a dictionary of addon parameters.
    For example, to use addon ``tweak.autolimit`` and set the parameter
    ``limit`` to ``1000``, run:

    .. sourcecode:: python

        >>> htsql = HTSQL('pgsql:htsql_demo',
        ...               {'tweak.autolimit': {'limit': 1000}})

.. py:method:: htsql.HTSQL.__call__(environ, start_response)

    The WSGI entry point.

    An HTSQL instance is a complete WSGI application.  For example,
    to start HTSQL as an HTTP server on ``localhost:8080``, run:

    .. sourcecode:: python

        >>> htsql = HTSQL('pgsql:htsql_demo')
        >>> from wsgiref.simple_server import make_server
        >>> httpd = make_server('localhost', 8080, htsql)
        >>> httpd.serve_forever()

.. py:method:: htsql.HTSQL.produce(query, **parameters)

    Executes an HTSQL query; returns output rows.

    `query` (a string)
        The query to execute.
    `parameters`
        Parameters passed as top-level references.

    Use this method to execute an HTSQL query and to get the results
    back.  The method returns an iterator that generates output rows.

    Example:

    .. sourcecode:: python

        >>> rows = htsql.produce("/program{code,title}?school.code='ns'")
        >>> for row in rows:
        ...     print row
        ...
        program(code=u'gmth', title=u'Masters of Science in Mathematics')
        program(code=u'pmth', title=u'Doctorate of Science in Mathematics')
        program(code=u'uastro', title=u'Bachelor of Science in Astronomy')
        program(code=u'uchem', title=u'Bachelor of Science in Chemistry')
        program(code=u'umth', title=u'Bachelor of Science in Mathematics')
        program(code=u'uphys', title=u'Bachelor of Science in Physics')

    Individual row fields could be accessed either by name or by position:

    .. sourcecode:: python

        >>> [row[0] for row in rows]
        [u'gmth', u'pmth', u'uastro', u'uchem', u'umth', u'uphys']
        >>> [row.code for row in rows]
        [u'gmth', u'pmth', u'uastro', u'uchem', u'umth', u'uphys']

    You can use in-segment assignment to specify the row name when
    it cannot be automatically inferred from the expression.  In this
    example, the output column ``count(student)`` is assigned
    the name ``num_std``:

    .. sourcecode:: python

        >>> rows = htsql.produce("/program.limit(3)"
        ...                      "{code,num_std:=count(student)}")
        >>> for row in rows:
        ...     print row.code, row.num_std
        ...
        gart 16
        uhist 20
        ustudio 26

    You can pass parameters as keyword arguments.  Use reference syntax
    (with ``$`` prefix) to access the parameters in the query:

    .. sourcecode:: python

        >>> rows = htsql.produce("/program?school.code=$school_code",
        ...                      school_code='ns')
        >>> print [row.code for row in rows]
        [u'gmth', u'pmth', u'uastro', u'uchem', u'umth', u'uphys']

    Values passed as parameters are converted to HTSQL literals.  The
    domain of the literal is determined from the type of the parameter:

    +---------------------------+---------------------------+
    | Python Type               | HTSQL Domain              |
    +===========================+===========================+
    | ``None``                  | ``untyped``               |
    +---------------------------+---------------------------+
    | ``string``, ``unicode``   | ``untyped``               |
    +---------------------------+---------------------------+
    | ``bool``                  | ``boolean``               |
    +---------------------------+---------------------------+
    | ``int``, ``long``         | ``integer``               |
    +---------------------------+---------------------------+
    | ``float``                 | ``float``                 |
    +---------------------------+---------------------------+
    | ``decimal.Decimal``       | ``decimal``               |
    +---------------------------+---------------------------+
    | ``datetime.date``         | ``date``                  |
    +---------------------------+---------------------------+
    | ``datetime.time``         | ``time``                  |
    +---------------------------+---------------------------+
    | ``datetime.datetime``     | ``datetime``              |
    +---------------------------+---------------------------+
    | ``list``, ``tuple``       | ``record``                |
    +---------------------------+---------------------------+


