#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from . import (adapter, addon, application, cache, cmd, connect, context,
               domain, entity, error, introspect, mark, split_sql,
               tr, util, validator, wsgi)
from .validator import DBVal, StrVal, BoolVal
from .addon import Addon, Parameter, Variable, addon_registry
from .connect import connect, DBError
from .introspect import introspect
from .cache import GeneralCache


class HTSQLAddon(Addon):

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
            Parameter('password', StrVal(),
                      hint="""override the password"""),
            Parameter('debug', BoolVal(), default=False,
                      hint="""dump debug information""")
    ]

    variables = [
            Variable('connection'),
            Variable('can_read', True),
            Variable('can_write', True),
    ]

    packages = ['.', '.cmd', '.fmt', '.tr', '.tr.fn']
    prerequisites = []
    postrequisites = ['engine']

    def __init__(self, app, attributes):
        super(HTSQLAddon, self).__init__(app, attributes)
        self.cache = GeneralCache()

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


class EngineAddon(Addon):

    name = 'engine'
    hint = """provides implementations of HTSQL for specific servers"""
    help = """
    This extension implements HTSQL translator for specific
    database servers.
    """

    @classmethod
    def get_extension(cls, app, attributes):
        if app.htsql.db is not None:
            name = '%s.%s' % (cls.name, app.htsql.db.engine)
            if name not in addon_registry:
                raise ImportError("unknown engine %r" % app.htsql.db.engine)
            return name


