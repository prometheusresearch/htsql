#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
This module implements an HTSQL application.
"""


from __future__ import with_statement
from .error import HTTPError
from .context import context
from .util import DB
from .tr.parser import QueryParser
import urllib


class Application(object):
    """
    Implements an HTSQL application.

    `db`
        The connection URI.
    """

    def __init__(self, db, *extensions):
        # Parse the connection URI.
        self.db = DB.parse(db)
        # Generate the list of addon names.
        addon_names = []
        addon_names.append('htsql.core')
        addon_names.append('engine.%s' % self.db.engine)
        addon_names.extend(extensions)
        # Import addons from the entry point group `htsql.addons`.
        addons = []
        for name in addon_names:
            entry_points = list(pkg_resources.iter_entry_points('htsql.addons',
                                                                name))
            if len(entry_points) == 0:
                raise ImportError("unknown entry point %r" % name)
            elif len(entry_points) > 1:
                raise ImportError("ambiguous entry point %r" % name)
            entry_point = entry_points[0]
            addon_class = entry_point.load()
            if not (isinstance(addon_class, type) and
                    issubclass(addon_class, Addon)):
                raise ImportError("invalid entry point %r" % name)
            addons.append(addon_class)
        # Get adapters exported by addons.
        adapters = []
        for addon_class in addons:
            adapters.extend(addon_class.adapters)
        # TODO: these should be defined by the `htsql.core` addon.
        # Initialize the adapter registry.
        self.adapter_registry = AdapterRegistry(adapters)
        #self.connection_pool = ConnectionPool()
        #self.introspector = Introspector()

    def __enter__(self):
        """
        Activates the application in the current thread.
        """
        context.switch(None, self)

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """
        Inactivates the application in the current thread.
        """
        context.switch(self, None)

    def __call__(self, environ, start_response):
        """
        Implements the WSGI entry point.
        """
        with self:
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


