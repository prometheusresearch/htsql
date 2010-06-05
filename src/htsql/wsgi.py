#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.wsgi`
=================

This module provides a handler for WSGI requests.

This module exports a global variable:

`wsgi_adapters`
    List of adapters declared in the module.
"""

from .adapter import Utility, weights, find_adapters
from .tr.parser import QueryParser
from .error import HTTPError
import urllib


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
        # Parse and echo the query.
        path_info = environ['PATH_INFO']
        query_string = environ.get('QUERY_STRING')
        uri = urllib.quote(path_info)
        if query_string:
            uri += '?'+query_string
        parser = QueryParser(uri)
        try:
            syntax = parser.parse()
        except HTTPError, exc:
            return exc(environ, start_response)
        start_response("200 OK", [('Content-Type', 'text/plain')])
        return [str(syntax), "\n"]


wsgi_adapters = find_adapters()


