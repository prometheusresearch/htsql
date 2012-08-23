#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql`
============

:copyright: 2006-2012, Prometheus Research, LLC
:authors: Clark C. Evans <cce@clarkevans.com>,
          Kirill Simonov <xi@resolvent.net>;
          see ``AUTHORS`` file in the source distribution
          for the full list of contributors

This package provides HTSQL, a comprehensive navigational query language for
relational databases.

HTSQL is implemented as a WSGI application.  To create an application, run::

    >>> from htsql import HTSQL
    >>> app = HTSQL(db)

where `db` is a connection URI, a string of the form::

    engine://username:password@host:port/database

`engine`
    The type of the database server; ``pgsql`` or ``sqlite``.

`username:password`
    Used for authentication; optional.

`host:port`
    The server address; optional.

`database`
    The name of the database; for SQLite, the path to the database file.

To execute a WSGI request, run::

    >>> app(environ, start_response)

To execute a raw HTSQL request, run::

    >>> rows = app.produce(query, **parameters)
"""


__version__ = '2.3.2'
__copyright__ = """Copyright (c) 2006-2012, Prometheus Research, LLC"""
__license__ = """
This is free software: you are free to change and redistribute it.
There is NO WARRANTY, to the extent permitted by law.  This software
is released under the AGPLv3 as well as a permissive license for use
with open source databases.  See http://htsql.org/license/.

The HTSQL language and implementation were developed by members
of the HTSQL Project (http://htsql.org/).
"""
__legal__ = """\
HTSQL %(__version__)s
%(__copyright__)s
%(__license__)s""" % vars()


from .core.application import Application as HTSQL


