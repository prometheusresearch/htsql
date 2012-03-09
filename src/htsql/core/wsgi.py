#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql.core.wsgi`
======================

This module provides a handler for WSGI requests.
"""

from .adapter import Utility
from .error import HTTPError
from .cmd.command import UniversalCmd
from .cmd.act import render
import urllib


class WSGI(Utility):
    """
    Declares the WSGI interface.

    The WSGI interface is a utility to handle WSGI requests.

    Usage::

        body = wsgi(environ, start_response)
    """

    def __init__(self, environ, start_response):
        self.environ = environ
        self.start_response = start_response

    def request(self):
        # Extract an HTSQL request from `environ`.
        path_info = self.environ['PATH_INFO']
        query_string = self.environ.get('QUERY_STRING')
        uri = urllib.quote(path_info)
        if query_string:
            uri += '?'+query_string
        return uri

    def __call__(self):
        # Pass GET requests only.
        method = self.environ['REQUEST_METHOD']
        if method != 'GET':
            self.start_response('400 Bad Request',
                                [('Content-Type', 'text/plain')])
            return ["%s requests are not permitted.\n" % method]
        # Process the query.
        uri = self.request()
        try:
            command = UniversalCmd(uri)
            status, headers, body = render(command, self.environ)
        except HTTPError, exc:
            return exc(self.environ, self.start_response)
        self.start_response(status, headers)
        return body


wsgi = WSGI.__invoke__


