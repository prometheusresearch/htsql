#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from . import (adapter, addon, application, cache, cmd, connect, context,
        domain, entity, error, introspect, split_sql, syn, tr, util, validator,
        wsgi)
from .validator import DBVal, StrVal, BoolVal, UIntVal
from .addon import Addon, Parameter, Variable, addon_registry
from .connect import connect
from .error import Error
from .introspect import introspect
from .cache import GeneralCache


class HTSQLAddon(Addon):

    name = 'htsql'
    hint = """HTSQL translator and HTTP service"""
    help = """
    This extension implements the HTSQL translator and HTTP service.
    It is included to every HTSQL application.

    The parameter `db` specifies parameters of the database connection;
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

    The parameter `password` sets the database password.  It overrides
    the password given as a part of `db` parameter.

    The parameter `query_cache_size` specifies the number of cached
    query plans.  The default value is 1024.

    The parameter `debug`, if set to `True`, enables debug output.
    """

    parameters = [
            Parameter('db', DBVal(),
                      value_name="""engine:database""",
                      hint="""the connection URI"""),
            Parameter('password', StrVal(),
                      hint="""override the password"""),
            Parameter('query_cache_size', UIntVal(), default=1024,
                      value_name="""size""",
                      hint="""max size of the query cache"""),
            Parameter('debug', BoolVal(), default=False,
                      hint="""dump debug information""")
    ]

    variables = [
            Variable('connection'),
            Variable('can_read', True),
            Variable('can_write', True),
    ]

    packages = ['.', '.cmd', '.fmt', '.tr', '.tr.fn', '.syn']
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
        except Error as exc:
            raise ValueError("failed to establish database connection: %s"
                             % exc)
        try:
            introspect()
        except Error as exc:
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


