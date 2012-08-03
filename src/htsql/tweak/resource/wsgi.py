#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ...core.context import context
from ...core.adapter import rank
from ...core.wsgi import WSGI
from .locate import locate


class ResourceWSGI(WSGI):

    rank(100.0)

    def __call__(self):
        path = self.environ.get('PATH_INFO')
        indicator = context.app.tweak.resource.indicator
        if not path.startswith('/'+indicator+'/'):
            return super(ResourceWSGI, self).__call__()
        method = self.environ['REQUEST_METHOD']
        if method not in ['HEAD', 'GET']:
            self.start_response('400 Bad Request',
                                [('Content-Type', 'text/plain')])
            return ["Expected a GET request, got %r.\n" % method]
        path = path[len(indicator)+1:]
        resource = None
        if not (path.endswith('/') or '/.' in path or '\\' in path):
            resource = locate(path)
        if resource is None:
            self.start_response("404 Not Found",
                                [('Content-Type', 'text/plain')])
            return ["Resourse does not exist: %r.\n" % path]
        request_etag = self.environ.get('HTTP_IF_NONE_MATCH')
        if resource.etag == request_etag:
            self.start_response("304 Not Modified",
                                [('ETag', resource.etag)])
            return []
        status = "200 OK"
        headers = [('Content-Type', resource.mimetype),
                   ('Content-Length', str(len(resource.data))),
                   ('ETag', resource.etag),
                   ('Content-Disposition', '%s; filename="%s"'
                       % (resource.disposition,
                          resource.name.replace('\\', '\\\\')
                                       .replace('"', '\\"')))]
        self.start_response(status, headers)
        if method == 'HEAD':
            return []
        return [resource.data]


