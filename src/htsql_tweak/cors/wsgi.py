

from htsql.adapter import weigh
from htsql.wsgi import WSGI


class CORSWSGI(WSGI):

    weigh(1.0)

    def __call__(self, environ, start_response):
        def cors_start_response(status, headers, exc_info=None):
            headers = headers + [('Access-Control-Allow-Origin', '*')]
            return start_response(status, headers, exc_info)
        return super(CORSWSGI, self).__call__(environ, cors_start_response)


