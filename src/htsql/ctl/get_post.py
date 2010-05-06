#
# Copyright (c) 2006-2008, Prometheus Research, LLC
# Authors: Kirill Simonov <xi@gamma.dn.ua>,
#          Clark C. Evans <cce@clarkevans.com>
#


"""
This module implements the `get` and `post` routines.
"""


from .error import ScriptError
from .routine import Argument, Routine
from .option import (InputOption, OutputOption,
                     RemoteUserOption, WithHeadersOption,
                     ContentTypeOption)
from ..validator import DBVal, StrVal
from ..util import maybe, oneof, listof, tupleof, filelike
import wsgiref.util
import urllib
import traceback
import StringIO
import mimetypes


class GetPostBaseRoutine(Routine):
    """
    Implements the common methods for the `get` and `post` routines.

    Both routines take a connection URI and an HTSQL query as arguments
    and execute an HTTP request.
    """

    # The arguments are the same for both routines.
    arguments = [
            Argument('db', DBVal(),
                     hint="""the connection URI"""),
            Argument('query', StrVal(),
                     hint="""the HTSQL query"""),
    ]
    # These are common options for both routines.  The `post` routine
    # adds some extra options.
    options = [
            RemoteUserOption,
            OutputOption,
            WithHeadersOption,
    ]

    def prepare_request(self, method, query, remote_user=None,
                        content_type=None, content_body=None,
                        extra_headers=None):
        # Takes the parameters of the request; returns a WSGI `environ`
        # dictionary.

        # Sanity check on the arguments
        assert method in ['GET', 'POST']
        assert isinstance(query, str)
        assert isinstance(remote_user, maybe(str))
        assert isinstance(content_type, maybe(str))
        assert isinstance(content_body, maybe(oneof(str, filelike())))
        assert isinstance(extra_headers, maybe(listof(tupleof(str, str))))
        if method == 'GET':
            assert content_type is None
            assert content_body is None
        if method == 'POST':
            assert content_body is not None

        environ = {}

        environ['REQUEST_METHOD'] = method

        # Split `query` into components.
        environ['SCRIPT_NAME'] = ''
        if '?' in query:
            path_info, query_string = query.string('?', 1)
        else:
            path_info = query
            query_string = ''
        path_info = urllib.unquote(path_info)
        environ['PATH_INFO'] = path_info
        environ['QUERY_STRING'] = query_string

        if remote_user is not None:
            environ['REMOTE_USER'] = remote_user

        if method == 'POST':
            # When `content_type` is not explicitly provided,
            # guess it from the file name if possible.
            if content_type is None:
                if hasattr(content_body, 'name'):
                    content_type = mimetypes.guess_type(content_body.name)[0]
            # If we can't guess the content type, use the default value.
            if content_type is None:
                content_type = 'application/octet-stream'
            # If `content_body` is a file-like object, read its content.
            if not isinstance(content_body, str):
                content_body = content_body.read()
            environ['CONTENT_TYPE'] = content_type
            environ['CONTENT_LENGTH'] = str(len(content_body))
            environ['wsgi.input'] = StringIO.StringIO(content_body)

        # Transfer HTTP headers to the WSGI `environ`.
        if extra_headers is not None:
            for key in extra_headers:
                variable = 'HTTP_%s' % key.upper().replace('-', '_')
                environ[variable] = extra_headers[key]

        # Assign reasonable values for the remaining required WSGI parameters
        # of `environ`.
        wsgiref.util.setup_testing_defaults(environ)

        return environ

    def execute_request(self, app, environ):
        # Executes a WSGI request; returns a `response` object.

        # The container for the response data.
        class response:
            status = None
            headers = None
            exc_info = None
            body = None

        # A WSGI `start_response` function; saves the response data.
        def start_response(status, headers, exc_info=None):
            response.status = status
            response.headers = headers
            response.exc_info = exc_info
            # Note that we don't expect the application to use the returned
            # stream object, so we don't keep it.
            return StringIO.StringIO()

        # Execute the WSGI request.
        try:
            iterator = app(environ, start_response)
            try:
                response.body = ''.join(iterator)
            finally:
                if hasattr(iterator, 'close'):
                    iterator.close()
        except Exception, exc:
            traceback.print_exc(file=self.ctl.stderr)
            raise ScriptError("exception while executing an HTSQL request")

        # Sanity check on the response data.
        if not (isinstance(response.status, str) and
                response.status[:3].isdigit() and
                response.status[3:4] == ' ' and
                isinstance(response.headers, listof(tupleof(str, str))) and
                isinstance(response.body, str)):
            raise ScriptError("incomplete response")

        return response

    def display_response(self, response, stream, with_headers=False):
        # Dumps the WGSI response.

        # Write the HTTP status code and headers if asked to.
        if with_headers:
            stream.write("%s\r\n" % response.status)
            for header, value in response.headers:
                stream.write("%s: %s\r\n" % (header, value))
            stream.write("\r\n")

        # Write the HTTP body.
        stream.write(response.body)

        # Write CR to the terminal if the body does not end with a new line.
        if response.body and response.body[-1] not in "\r\n":
            if hasattr(stream, 'isatty') and stream.isatty():
                stream.write("\r\n")


class GetRoutine(GetPostBaseRoutine):
    """
    Implements the `get` routine.

    The routine executes an HTSQL query over the specified database.
    """

    name = 'get'
    hint = """execute and render an HTSQL query"""
    help = """
    The routine executes an HTSQL query and displays the response.

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

    The QUERY argument is the HTSQL query to execute.

    Use option `--remote-user USER` to specify the remote user of the HTTP
    request.  By default, the remote user is not set.

    Use option `--output FILE` to specify the file to write the response.
    If the option is not set, the response is written to the console.

    Use option `--with-headers` to indicate that the response status code
    and headers should be displayed.  By default, only the response body is
    written.
    """

    def run(self):
        # Create the HTSQL application.
        from htsql import Application
        app = Application(self.db)

        # Prepare a WSGI `environ` dictionary.
        request = self.prepare_request('GET', self.query, self.remote_user)

        # Execute the WSGI request.
        response = self.execute_request(app, request)

        # Dump the response.
        if self.output is None:
            output_stream = self.ctl.stdout
        else:
            output_stream = open(self.output, 'wb')
        self.display_response(response, output_stream, self.with_headers)

        # Complain when the response status is not `200 OK`.
        if not response.status.startswith('200'):
            raise ScriptError("unexpected status code: %s" % response.status)


class PostRoutine(GetPostBaseRoutine):
    """
    Implements the `post` routine.

    The routine executes an HTSQL query with POST data over the specified
    database.
    """

    name = 'post'
    options = [
            InputOption,
            ContentTypeOption,
    ] + GetPostBaseRoutine.options
    hint = """execute and render an HTSQL query with POST data"""
    help = """
    The routine executes an HTSQL query with POST data and displays the
    response.

    The DB argument specifies database connection parameters; must have
    the form:
    
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

    The QUERY argument is the HTSQL query to execute.

    Use option `--content-type TYPE` to specify the content type of the POST
    data.  If the option is not provided, the content type is guessed from
    the file name.

    Use option `--input FILE` to specify a file containing the POST data.
    If the option is not set, the routine reads the POST data from the
    console.

    Use option `--remote-user USER` to specify the remote user of the HTTP
    request.  By default, the remote user is not set.

    Use option `--output FILE` to specify the file to write the response.
    If the option is not set, the response is written to the console.

    Use option `--with-headers` to indicate that the response status code
    and headers should be displayed.  By default, only the response body is
    written.
    """

    def run(self):
        # Create the HTSQL application.
        from htsql import Application
        app = Application(self.db)

        # Prepare a WSGI `environ` dictionary.
        if self.input is None:
            input_stream = self.ctl.stdin
        else:
            input_stream = open(self.input, 'rb')
        request = self.prepare_request('POST', self.query, self.remote_user,
                                       self.content_type, input_stream)

        # Execute the WSGI request.
        response = self.execute_request(app, request)

        # Dump the response.
        if self.output is None:
            output_stream = self.ctl.stdout
        else:
            output_stream = open(self.output, 'wb')
        self.display_response(response, output_stream, self.with_headers)

        # Complain when the response status is not `200 OK`.
        if not response.status.startswith('200'):
            raise ScriptError("unexpected status code: %s" % response.status)


