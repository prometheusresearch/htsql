#
# Copyright (c) 2006-2012, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql.core.fmt.format`
============================

This module implements the format adapter.
"""


from ..util import setof
from ..adapter import Adapter, Utility, adapts
from ..domain import Domain, ListDomain, RecordDomain, Profile


class Format(object):
    pass


class JSONFormat(Format):
    pass


class ObjFormat(Format):
    pass


class CSVFormat(Format):

    def __init__(self, dialect='excel'):
        assert dialect in ['excel', 'excel-tab']
        self.dialect = dialect


class HTMLFormat(Format):
    pass


class TextFormat(Format):
    pass


class XMLFormat(Format):
    pass


class EmitHeaders(Adapter):

    adapts(Format)

    def __init__(self, format, product):
        self.format = format
        self.product = product
        self.meta = product.meta
        self.data = product.data

    def __call__(self):
        raise NotImplementedError()


class Emit(Adapter):

    adapts(Format)

    def __init__(self, format, product):
        self.format = format
        self.product = product
        self.meta = product.meta
        self.data = product.data

    def __call__(self):
        raise NotImplementedError()


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


