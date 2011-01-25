#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
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
        # Process the query.
        request = Request.build(environ)
        try:
            status, headers, body = request.render(environ)
        except HTTPError, exc:
            return exc(environ, start_response)
        start_response(status, headers)
        return body


