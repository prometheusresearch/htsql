#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
This module implements an HTSQL application.
"""


from .error import HTTPError
from .util import DB
from .parser import QueryParser
import urllib


class Application(object):
    """
    Implements an HTSQL application.

    `db`
        The connection URI.
    """

    def __init__(self, db):
        self.db = DB.parse(db)

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


