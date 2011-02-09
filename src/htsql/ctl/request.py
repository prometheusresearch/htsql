#
# Copyright (c) 2006-2008, Prometheus Research, LLC
# Authors: Kirill Simonov <xi@gamma.dn.ua>,
#          Clark C. Evans <cce@clarkevans.com>
#


"""
:mod:`htsql.ctl.request`
========================

This module implements the `get` and `post` routines.
"""


from .error import ScriptError
from .routine import Argument, Routine
from .option import (InputOption, OutputOption,
                     RemoteUserOption, WithHeadersOption,
                     ContentTypeOption, ExtensionsOption)
from ..validator import DBVal, StrVal
from ..util import maybe, oneof, listof, tupleof, dictof, filelike
import sys
import wsgiref.util
import urllib
import traceback
import StringIO
import mimetypes


class Request(object):
    """
    Represents a WSGI request.

    `environ`
        A WSGI `environ` dictionary.
    """

    @classmethod
    def prepare(cls, method, query, remote_user=None,
                content_type=None, content_body=None,
                extra_headers=None):
        """
        Produces a :class:`Request` object from the given parameters.

        `method` (``'GET'`` or ``'POST'``)
            The HTTP request method.

        `query` (a string)
            The path and the query parts of the URI.

        `remote_user` (a string or ``None``)
            The name of the authenticated user.

        `content_type` (a string or ``None``)
            The content type of the POST data, used only when `method` is
            ``'POST'``.  If not provided, guessed from the file name of the
            `content_body` stream.  If that fails,
            ``'application/octet-stream'`` is used.

        `content_body` (a string, a file or a file-like object or ``None``)
            The body of the HTTP request, used only when `method` is
            ``'POST'``.

        `extra_headers` (a dictionary or ``None``)
            A dictionary of HTTP headers.
        """

        # Sanity check on the arguments
        assert method in ['GET', 'POST']
        assert isinstance(query, str)
        assert isinstance(remote_user, maybe(str))
        assert isinstance(content_type, maybe(str))
        assert isinstance(content_body, maybe(oneof(str, filelike())))
        assert isinstance(extra_headers, maybe(dictof(str, str)))
        if method == 'GET':
            assert content_type is None
            assert content_body is None
        if method == 'POST':
            assert content_body is not None

        # The WSGI `environ` variable, see PEP 333.
        environ = {}

        environ['REQUEST_METHOD'] = method

        # Split `query` into components.
        environ['SCRIPT_NAME'] = ''
        if '?' in query:
            path_info, query_string = query.split('?', 1)
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

        # Assign reasonable values of the missing WSGI parameters.
        wsgiref.util.setup_testing_defaults(environ)

        return cls(environ)

    def __init__(self, environ):
        assert isinstance(environ, dictof(str, object))
        self.environ = environ

    def execute(self, app):
        """
        Executes the request against the given WSGI application.

        `app`
            A WSGI application.

        Returns a :class:`Response` object.
        """

        # The container for the response data.
        response = Response()

        # A WSGI `start_response` function; saves the response data.
        def start_response(status, headers, exc_info=None):
            response.set(status=status, headers=headers)
            # Note that we don't expect the application to use the returned
            # stream object, so we don't keep it.
            return StringIO.StringIO()

        # Copy the `environ` dictionary in case the application modifies it.
        # TODO: that is not enough to make `execute()` truly re-entrant: for
        # POST requests, we also need to save the `environ['wsgi.input']`
        # stream.  For now, assume that a `Request` object could be executed
        # only once.
        environ = self.environ.copy()

        # Execute the WSGI request.
        try:
            iterator = app(environ, start_response)
            try:
                response.set(body=''.join(iterator))
            finally:
                if hasattr(iterator, 'close'):
                    iterator.close()
        except Exception:
            # Save the exception data.
            response.set(exc_info=sys.exc_info())

        return response


class Response(object):
    """
    Represents a response to a WSGI request.

    `status` (a string)
        The HTTP status line.

    `headers` (a list of pairs)
        The HTTP headers.

    `body` (a string)
        The HTTP body.

    `exc_info` (a tuple ``(type, value, traceback)`` or ``None``)
        Any exception occured when the request was executed.
    """

    def __init__(self):
        self.status = None
        self.headers = None
        self.body = None
        self.exc_info = None

    def set(self, **attributes):
        """
        Updates the response parameters.
        """
        for name in attributes:
            assert hasattr(self, name)
            setattr(self, name, attributes[name])

    def complete(self):
        """
        Returns ``True`` if the response is complete; ``False`` otherwise.

        The response is considered valid if the HTTP status, headers and
        body are set and valid and no exception occured during the execution
        of the request.
        """
        return (isinstance(self.status, str) and
                self.status[:3].isdigit() and
                self.status[3:4] == ' ' and
                isinstance(self.headers, listof(tupleof(str, str))) and
                isinstance(self.body, str) and
                self.exc_info is None)

    def dump(self, stream, with_headers=False):
        """
        Writes the response to the output stream.

        `stream` (a file or a file-like object)
            The stream where to write the response.

        `with_headers`
            Indicates whether the status line and the headers should
            also be written.
        """
        # The response must be complete at this point.
        assert self.complete()

        # Write the HTTP status code and headers if asked to.
        if with_headers:
            stream.write("%s\r\n" % self.status)
            for header, value in self.headers:
                stream.write("%s: %s\r\n" % (header, value))
            stream.write("\r\n")

        # Write the HTTP body.
        stream.write(self.body)

        # Write CR if the body does not end with a new line and the
        # output stream is a console.
        if self.body and self.body[-1] not in "\r\n":
            if hasattr(stream, 'isatty') and stream.isatty():
                stream.write("\r\n")


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
            ExtensionsOption,
            RemoteUserOption,
            OutputOption,
            WithHeadersOption,
    ]
    # The HTTP method implemented by the routine.
    method = None

    def run(self):
        # Create the HTSQL application.
        from htsql.application import Application
        app = Application(self.db, *self.extensions)

        # Prepare a WSGI `environ` variable.
        if self.method == 'GET':
            request = Request.prepare('GET', self.query, self.remote_user)
        elif self.method == 'POST':
            if self.input is None:
                input_stream = self.ctl.stdin
            else:
                input_stream = open(self.input, 'rb')
            request = Request.prepare('POST', self.query, self.remote_user,
                                      self.content_type, input_stream)

        # Execute the WSGI request.
        response = request.execute(app)

        # Check for errors.
        if response.exc_info is not None:
            exc_type, exc_value, exc_traceback = response.exc_info
            traceback.print_exception(exc_type, exc_value, exc_traceback,
                                      file=self.ctl.stderr)
            raise ScriptError("exception while executing an HTSQL request")
        if not response.complete():
            raise ScriptError("incomplete response")

        # Dump the response.
        if self.output is None:
            output_stream = self.ctl.stdout
        else:
            output_stream = open(self.output, 'wb')
        response.dump(output_stream, self.with_headers)

        # Complain when the response status is not `200 OK`.
        if not response.status.startswith('200'):
            raise ScriptError("unexpected status code: %s" % response.status)


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
    method = 'GET'


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
    method = 'POST'


