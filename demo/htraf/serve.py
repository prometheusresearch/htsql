
import sys
import os.path
import mimetypes
import wsgiref.simple_server, wsgiref.util
from htsql import HTSQL


class Application(object):

    static = 'static'
    prefix = '/@/'
    index = 'index.html'

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

    @classmethod
    def main(cls):
        if len(sys.argv) != 4:
            return cls.usage()
        db, host, port = sys.argv[1:]
        port = int(port)
        htsql = HTSQL(db)
        app = cls(htsql)
        print("Starting the HTRAF demo on http://%s:%s/" \
                            % (host, port), file=sys.stderr)
        httpd = wsgiref.simple_server.make_server(host, port, app)
        httpd.serve_forever()

    @classmethod
    def usage(cls):
        return "Usage: %s DB HOST PORT" % sys.argv[0]


if __name__ == '__main__':
    sys.exit(Application.main())


