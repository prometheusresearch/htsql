#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql.request`
====================

This module implements the request utility.
"""


from .adapter import Utility, Realization
from .cmd.act import produce as produce_cmd, render as render_cmd
from .cmd.command import UniversalCmd
import urllib


class Request(Utility):

    @classmethod
    def build(cls, environ):
        # FIXME: override `classmethod` in `htsql.adapter`?
        if not issubclass(cls, Realization):
            cls = cls.realize(())
            return cls.build(environ)
        path_info = environ['PATH_INFO']
        query_string = environ.get('QUERY_STRING')
        uri = urllib.quote(path_info)
        if query_string:
            uri += '?'+query_string
        return cls(uri)

    def __init__(self, uri):
        self.uri = uri

    def produce(self):
        command = UniversalCmd(self.uri)
        return produce_cmd(command)

    def render(self, environ):
        command = UniversalCmd(self.uri)
        return render_cmd(command, environ)

    def __call__(self, environ):
        return self.render(environ)


def render(environ):
    request = Request.build(environ)
    return request.render(environ)


def produce(uri):
    request = Request(uri)
    return request.produce()


