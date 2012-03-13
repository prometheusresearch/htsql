#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ..core.util import maybe, oneof, listof, tupleof, dictof, filelike
from .error import ScriptError
from .routine import Argument, Routine
from .option import PasswordOption, ExtensionsOption, ConfigOption
from ..core.util import DB
from ..core.validator import DBVal
import sys
import os.path
import wsgiref.util
import urllib
import StringIO
import mimetypes
import re
import getpass
import yaml, yaml.constructor


BaseYAMLLoader = yaml.SafeLoader
if hasattr(yaml, 'CSafeLoader'):
    BaseYAMLLoader = yaml.CSafeLoader


class ConfigYAMLLoader(BaseYAMLLoader):

    name_pattern = ur"""
        ^
        [a-zA-Z_-][0-9a-zA-Z_-]*
        $
    """
    name_regexp = re.compile(name_pattern, re.X)
    dotted_name_pattern = ur"""
        ^
        [a-zA-Z_-][0-9a-zA-Z_-]*
        (?: \. [a-zA-Z_-][0-9a-zA-Z_-]* )*
        $
    """
    dotted_name_regexp = re.compile(dotted_name_pattern, re.X)

    def load(self):
        return self.get_single_data()

    def construct_document(self, node):
        document_node = node
        if (not (isinstance(document_node, yaml.ScalarNode) and
                document_node.tag == u'tag:yaml.org,2002:null') and
            not (isinstance(document_node, yaml.MappingNode) and
                 document_node.tag == u'tag:yaml.org,2002:map')):
            raise yaml.constructor.ConstructorError(None, None,
                    "invalid structure of configuration file",
                    document_node.start_mark)
        if isinstance(document_node, yaml.MappingNode):
            for name_node, addon_node in document_node.value:
                if not (isinstance(name_node, yaml.ScalarNode) and
                        name_node.tag == u'tag:yaml.org,2002:str' and
                        self.dotted_name_regexp.match(name_node.value)):
                    raise yaml.constructor.ConstructorError(None, None,
                            "invalid addon name", name_node.start_mark)
            if (not (isinstance(addon_node, yaml.ScalarNode) and
                    addon_node.tag == u'tag:yaml.org,2002:null') and
                not (isinstance(addon_node, yaml.MappingNode) and
                     addon_node.tag == u'tag:yaml.org,2002:map')):
                raise yaml.constructor.ConstructorError(None, None,
                        "invalid addon configuration", addon_node.start_mark)
                if isinstance(addon_node, yaml.MappingNode):
                    for attribute_node, value_node in addon_node.value:
                        if not (isinstance(attribute_node, yaml.ScalarNode) and
                                attribute_node.tag
                                    == u'tag:yaml.org,2002:str' and
                                self.name_regexp.match(attribute_node.value)):
                            raise yaml.constructor.ConstructorError(None, None,
                                    "invalid parameter name",
                                    attribute_node.start_mark)
        return super(ConfigYAMLLoader, self).construct_document(document_node)


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


class DBRoutine(Routine):
    """
    Implements a template for routines that create an HTSQL instance.
    """
    arguments = [
            Argument('db', DBVal(), default=None,
                     hint="""the connection URI"""),
    ]
    options = [
            PasswordOption,
            ExtensionsOption,
            ConfigOption,
    ]

    # Path to the default configuration file.
    default_path = '~/.htsql/default.yaml'

    def run(self):
        # Determine HTSQL initialization parameters.
        parameters = [self.db]

        # Ask for the database password if necessary.
        if self.password:
            password = getpass.getpass()
            parameters.append({'htsql': {'password': password}})

        # Load addon configuration.
        parameters.extend(self.extensions)
        if self.config is not None:
            stream = open(self.config, 'rb')
            loader = ConfigYAMLLoader(stream)
            try:
                config_extension = loader.load()
            except yaml.YAMLError, exc:
                raise ScriptError("failed to load application configuration:"
                                  " %s" % exc)
            if config_extension is not None:
                parameters.append(config_extension)

        # Load the default configuration from the RC file.
        path = os.path.abspath(os.path.expanduser(self.default_path))
        if os.path.exists(path):
            stream = open(path, 'rb')
            loader = ConfigYAMLLoader(stream)
            try:
                default_extension = loader.load()
            except yaml.YAMLError, exc:
                raise ScriptError("failed to load default configuration: %s"
                                  % exc)
            if default_extension is not None:
                parameters.append(default_extension)

        # Create the HTSQL application.
        from htsql import HTSQL
        try:
            app = HTSQL(*parameters)
        except ImportError, exc:
            raise ScriptError("failed to construct application: %s" % exc)

        # Run the routine-specific code.
        self.start(app)

    def start(self, app):
        # Override in subclasses.
        raise NotImplementedError()


