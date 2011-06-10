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
from .request import Request
from .error import HTTPError


class WSGI(Utility):
    """
    Declares the WSGI interface.

    The WSGI interface is a utility to handle WSGI requests.

    Usage::

        wsgi = WSGI()
        body = wsgi(environ, start_response)
    """

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
        request = Request.build(environ)
        try:
            status, headers, body = request.render(environ)
        except HTTPError, exc:
            return exc(environ, start_response)
        start_response(status, headers)
        return body


