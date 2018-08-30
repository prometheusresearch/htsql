
import sys
import os.path
import re
import urllib.request, urllib.parse, urllib.error
import mimetypes
import wsgiref.simple_server, wsgiref.util
from htsql import HTSQL
from htsql.request import Request
from htsql.error import HTTPError


class Application(object):

    static = 'static'
    index = 'index.html'
    ext = '.htsql'
    prefix = '/@/'
    variable = re.compile(r'\$\w+')

    def __init__(self, htsql):
        self.htsql = htsql

    def __call__(self, environ, start_response):
        if environ['REQUEST_METHOD'] != 'GET':
            start_response("400 Bad Request",
                           [('Content-Type', 'text/plain')])
            return ["Only GET requests are supported.\n"]
        path = environ['PATH_INFO']
        if path.startswith(self.prefix):
            wsgiref.util.shift_path_info(environ)
            return self.htsql(environ, start_response)
        root = os.path.abspath(self.static)
        path = os.path.abspath(os.path.join(self.static, path[1:]))
        if not (path == root or path.startswith(root+os.path.sep)):
                start_response("404 Not Found",
                               [('Content-Type', 'text/plain')])
                return ["Invalid path.\n"]
        if os.path.isdir(path) and \
                os.path.exists(os.path.join(path, self.index)):
            path = os.path.join(path, self.index)
        if not os.path.isfile(path):
            start_response("404 Not Found",
                           [('Content-Type', 'text/plain')])
            return ["File not found.\n"]
        if path.endswith(self.ext):
            return self.handle_htsql(path, environ, start_response)
        mimetype = mimetypes.guess_type(path)[0]
        if mimetype is None:
            mimetype = 'application/octet-stream'
        stream = open(path, 'rb')
        data = stream.read()
        stream.close()
        start_response('200 OK',
                       [('Content-Type', mimetype),
                        ('Content-Length', str(len(data)))])
        return [data]

    def handle_htsql(self, path, environ, start_response):
        query_string = environ.get('QUERY_STRING', '')
        variables = {}
        for item in query_string.split('&'):
            if '=' in item:
                key, value = item.split('=', 1)
                key = urllib.parse.unquote_plus(key)
                value = urllib.parse.unquote_plus(value)
                value = urllib.parse.quote("'%s'" % value.replace("'", "''"))
                variables[key] = value
        stream = open(path, 'rb')
        uri = stream.read()
        stream.close()
        uri = self.variable.sub(
                (lambda m: variables.get(m.group()[1:], "null()")), uri)
        with self.htsql:
            request = Request(uri)
            try:
                status, headers, body = request.render(environ)
            except HTTPError as exc:
                return exc(environ, start_response)
            start_response(status, headers)
            return body

    @classmethod
    def main(cls):
        if len(sys.argv) != 4:
            return cls.usage()
        db, host, port = sys.argv[1:]
        port = int(port)
        htsql = HTSQL(db)
        app = cls(htsql)
        print("Starting the SSI demo on http://%s:%s/" \
                            % (host, port), file=sys.stderr)
        httpd = wsgiref.simple_server.make_server(host, port, app)
        httpd.serve_forever()

    @classmethod
    def usage(cls):
        return "Usage: %s DB HOST PORT" % sys.argv[0]


if __name__ == '__main__':
    sys.exit(Application.main())


