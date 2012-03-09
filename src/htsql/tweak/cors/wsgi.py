

from ...core.context import context
from ...core.adapter import rank
from ...core.wsgi import WSGI


class CORSWSGI(WSGI):

    rank(1.0)

    def __call__(self):
        origin = context.app.tweak.cors.origin
        if origin is None:
            return super(CORSWSGI, self).__call__()
        # FIXME: have to replace `self.start_response`.
        old_start_response = self.start_response
        def cors_start_response(status, headers, exc_info=None):
            headers = headers + [('Access-Control-Allow-Origin', origin)]
            return old_start_response(status, headers, exc_info)
        self.start_response = cors_start_response
        return super(CORSWSGI, self).__call__()


