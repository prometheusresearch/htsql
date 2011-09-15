#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.context import context
from htsql.adapter import weigh
from htsql.wsgi import WSGI
from .locate import locate


class ResourceWSGI(WSGI):

    weigh(100)

    def __call__(self, environ, start_response):
        path = environ.get('PATH_INFO')
        indicator = context.app.tweak.resource.indicator
        if not path.startswith('/'+indicator+'/'):
            return super(ResourceWSGI, self).__call__(environ, start_response)
        method = environ['REQUEST_METHOD']
        if method not in ['HEAD', 'GET']:
            start_response('400 Bad Request', [('Content-Type', 'text/plain')])
            return ["Expected a %r request, got %r.\n" % method]
        path = path[len(indicator)+1:]
        resource = None
        if not (path.endswith('/') or '/.' in path or '\\' in path):
            resource = locate(path)
        if resource is None:
            start_response("404 Not Found", [('Content-Type', 'text/plain')])
            return ["Resourse does not exist: %r.\n" % path]
        request_etag = environ.get('HTTP_IF_NONE_MATCH')
        if resource.etag == request_etag:
            start_response("304 Not Modified", [('ETag', resource.etag)])
            return []
        status = "200 OK"
        headers = [('Content-Type', resource.mimetype),
                   ('Content-Length', str(len(resource.data))),
                   ('ETag', resource.etag),
                   ('Content-Disposition', '%s; filename="%s"'
                       % (resource.disposition,
                          resource.name.replace('\\', '\\\\')
                                       .replace('"', '\\"')))]
        start_response(status, headers)
        if method == 'HEAD':
            return []
        return [resource.data]


