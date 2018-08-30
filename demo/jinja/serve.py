
import sys
import wsgiref.simple_server
from htsql import HTSQL
from htsql.request import produce
import jinja2


class Application(object):

    template = 'template.html'

    def __init__(self, htsql):
        self.htsql = htsql
        self.jinja = jinja2.Environment(
                loader=jinja2.FileSystemLoader('.'))

    def __call__(self, environ, start_response):
        if environ['REQUEST_METHOD'] != 'GET':
            start_response("400 Bad Request",
                           [('Content-Type', 'text/plain')])
            return ["Only GET requests are supported.\n"]
        path = environ['PATH_INFO']
        if path != '/':
            start_response("404 Not Found",
                           [('Content-Type', 'text/plain')])
            return ["Only / path is handled.\n"]
        output = self.jinja.get_template(self.template).render(query=self.query)
        start_response("200 OK", [('Content-Type', 'text/html')])
        return [output.encode('utf-8')]

    def query(self, uri):
        with self.htsql:
            product = produce(uri)
        return product

    @classmethod
    def main(cls):
        if len(sys.argv) != 4:
            return cls.usage()
        db, host, port = sys.argv[1:]
        port = int(port)
        htsql = HTSQL(db)
        app = cls(htsql)
        print("Starting the Jinja demo on http://%s:%s/" \
                            % (host, port), file=sys.stderr)
        httpd = wsgiref.simple_server.make_server(host, port, app)
        httpd.serve_forever()

    @classmethod
    def usage(cls):
        return "Usage: %s DB HOST PORT" % sys.argv[0]


if __name__ == '__main__':
    sys.exit(Application.main())


