#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.fmt.format`
=======================

This module implements the format adapter.
"""


from ..util import setof
from ..adapter import Adapter, Utility, adapts
from ..domain import Domain


class Renderer(object):

    name = None
    aliases = []

    @classmethod
    def names(cls):
        if cls.name is not None:
            yield cls.name
        for name in cls.aliases:
            yield name

    def render(self, product):
        raise NotImplementedError()


class Formatter(Adapter):

    adapts(Renderer)

    def __init__(self, renderer):
        self.renderer = renderer

    def format(self, value, domain):
        format = Format(self.renderer, domain, self)
        return format(value)


class Format(Adapter):

    adapts(Renderer, Domain)

    def __init__(self, renderer, domain, tool):
        self.renderer = renderer
        self.domain = domain
        self.tool = tool

    def __call__(self, value):
        raise NotImplementedError()


class FindRenderer(Utility):

    def get_renderers(self):
        return []

    def __call__(self, names):
        assert isinstance(names, setof(str))
        for renderer_class in self.get_renderers():
            for name in renderer_class.names():
                if name in names:
                    return renderer_class
        return None


