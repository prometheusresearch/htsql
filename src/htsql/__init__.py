#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql`
============

:copyright: 2006-2011, Prometheus Research, LLC
:authors: Clark C. Evans <cce@clarkevans.com>,
          Kirill Simonov <xi@resolvent.net>;
          see ``AUTHORS`` file in the source distribution
          for the full list of contributors
:license: See ``LICENSE`` file in the source distribution

This package provides HTSQL, a query language for the accidental programmer.

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

To execute a WSGI request, run

    >>> app(environ, start_response)
"""


__version__ = '2.2.0c1'


from . import (adapter, addon, application, cache, cmd, connect, context,
               domain, entity, error, introspect, mark, request, split_sql,
               tr, util, validator, wsgi)
from .validator import DBVal
from .addon import Addon, Parameter
from .connect import ConnectionPool, connect, DBError
from .introspect import introspect
from .cache import GeneralCache

from .application import Application as HTSQL


class HTSQLAddon(Addon):
    """
    Declares the `htsql` addon.
    """

    name = 'htsql'
    hint = """HTSQL translator and HTTP service"""
    help = """
    This extension implements the HTSQL translator and HTTP service.
    It is included to every HTSQL application.

    The parameter `DB` specifies parameters of the database connection;
    it must have the form:

        ENGINE://USERNAME:PASSWORD@HOST:PORT/DATABASE

    Here,

      - ENGINE is the type of the database server; possible values
        are `sqlite`, `pgsql`, `mysql`, `oracle` or `mssql`.
      - USERNAME:PASSWORD are used for authentication to the database
        server.
      - HOST:PORT is the address of the database server.
      - DATABASE is the name of the database, or, for file-based
        backends, the path to the file containing the database.
    """

    parameters = [
            Parameter('db', DBVal(),
                      value_name="""engine:database""",
                      hint="""the connection URI"""),
    ]

    packages = ['.', '.cmd', '.fmt', '.tr', '.tr.fn']
    prerequisites = []
    postrequisites = ['engine']

    def __init__(self, app, attributes):
        super(HTSQLAddon, self).__init__(app, attributes)
        self.cache = GeneralCache()
        self.pool = ConnectionPool()

    def validate(self):
        if self.db is None:
            raise ValueError("database address is not specified")
        try:
            connect().release()
        except DBError, exc:
            raise ValueError("failed to establish database connection: %s"
                             % exc)
        try:
            introspect()
        except DBError, exc:
            raise ValueError("failed to introspect the database: %s" % exc)


