#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql.wsgi`
=================

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

        wsgi = WSGI()
        body = wsgi(environ, start_response)
    """

    def request(self, environ):
        """
        Extracts HTSQL request from `environ`.
        """
        path_info = environ['PATH_INFO']
        query_string = environ.get('QUERY_STRING')
        uri = urllib.quote(path_info)
        if query_string:
            uri += '?'+query_string
        return uri

    def __call__(self, environ, start_response):
        """
        Implements the WSGI entry point.
        """
        # Pass GET requests only.
        method = environ['REQUEST_METHOD']
        if method != 'GET':
            start_response('400 Bad Request', [('Content-Type', 'text/plain')])
            return ["%s requests are not permitted.\n" % method]
        # Process the query.
        uri = self.request(environ)
        try:
            command = UniversalCmd(uri)
            status, headers, body = render(command, environ)
        except HTTPError, exc:
            return exc(environ, start_response)
        start_response(status, headers)
        return body


