#
# Copyright (c) 2006-2008, Prometheus Research, LLC
# Authors: Kirill Simonov <xi@gamma.dn.ua>,
#          Clark C. Evans <cce@clarkevans.com>
#


"""
:mod:`htsql.ctl.server`
=======================

This module implements the `server` routine.
"""


from .routine import Argument, Routine
from .option import QuietOption
from ..validator import StrVal, IntVal, DBVal
import socket
import SocketServer
import wsgiref.simple_server
import binascii


class HTSQLServer(SocketServer.ThreadingMixIn,
                  wsgiref.simple_server.WSGIServer, object):
    # We override `WSGIServer` to pass a `ServerRoutine` object to the
    # constructor.  The routine is used to get the server address
    # and to access the standard output stream.
    # 
    # Note: `HTSQLServer` inherits from `ThreadingMixIn` to handle each
    # request in a separate thread; and from `object` to be able to
    # use `super()`.

    def __init__(self, routine):
        super(HTSQLServer, self).__init__((routine.host, routine.port),
                                          HTSQLRequestHandler)
        self.routine = routine


class HTSQLRequestHandler(wsgiref.simple_server.WSGIRequestHandler):
    # We override `WSGIRequestHandler` to customize logging.

    def get_stderr(self):
        # Returns a stream suitable for dumping logs and tracebacks.

        # We use `stdout` here since `stderr` is reserved for fatal errors.
        return self.server.routine.ctl.stdout

    def log_message(self, format, *args):
        # Dumps a log message in the Apache Common Log Format.
        # We override the method since the standard `log_message()` does
        # not extract the remote user field.

        # If the server was started with the `--quiet` option,
        # we do not output log messages.
        if self.server.routine.quiet:
            return

        # Extract the remote user when Basic Auth is used.
        remote_user = '-'
        auth = self.headers.get('Authorization', '').split()
        if len(auth) == 2 and auth[0].lower() == 'basic':
            auth = auth[1]
            try:
                auth = auth.decode('base64')
            except binascii.Error:
                pass
            else:
                if ':' in auth:
                    remote_user = auth.split(':', 1)[0]

        # Dump the message.
        stderr = self.get_stderr()
        stderr.write("%s - %s [%s] %s\n" % (self.address_string(),
                                            remote_user,
                                            self.log_date_time_string(),
                                            format % args))
        stderr.flush()


class ServerRoutine(Routine):
    """
    Implements the `server` routine.

    The routine starts an HTTP server that serves HTSQL requests over
    the specified database.
    """

    name = 'server'
    aliases = ['serve', 's']
    arguments = [
            Argument('db', DBVal(),
                     hint="""the connection URI"""),
            Argument('host', StrVal(), '',
                     hint="""the host address (by default, *)"""),
            Argument('port', IntVal(1, 65535), 8080,
                     hint="""the port number (by default, 8080)"""),
    ]
    options = [
            QuietOption,
    ]
    hint = """start an HTTP server handling HTSQL requests"""
    help = """
    The routine starts an HTTP server that serves HTSQL requests over the
    specified database.

    The DB argument specifies database connection parameters; must have the
    form:
    
        engine://username:password@host:port/database

    Here,
    
      - ENGINE is the type of the database server; supported values are
        `pgsql` and `sqlite`.
      - The parameters USERNAME:PASSWORD are used for authentication.
      - The parameters HOST:PORT indicate the address of the database
        server.
      - DATABASE is the name of the database; for SQLite, the path to the
        database file.

    All parameters except ENGINE and DATABASE are optional.

    By default, the HTTP server listens on the port 8080 on all available
    interfaces.  Specify the optional arguments HOST and PORT to override
    the default values.

    The HTTP logs are dumped to the standard output in the Apache Common Log
    Format.  Use option `--quiet` to suppress the logs.
    """

    def run(self):
        # Create the HTSQL application and the HTTP server.
        from htsql.application import Application
        app = Application(self.db)
        httpd = HTSQLServer(self)
        httpd.set_app(app)

        # Display the server address and the database connection parameters.
        if not self.quiet:
            host = self.host or socket.gethostname()
            self.ctl.out("Starting an HTSQL server on %s:%s over %s"
                         % (host, self.port, self.db))

        # Start the server.
        httpd.serve_forever()


