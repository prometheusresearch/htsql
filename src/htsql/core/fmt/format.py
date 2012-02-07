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

    def flatten_product(self, product):
        if not isinstance(product.meta.domain, ListDomain):
            product.meta.title = []
            return
        self.flatten_data(product)
        self.flatten_meta(product)

    def flatten_data(self, product):
        domain = product.meta.domain.item_domain
        rows = []
        for row in product.data:
            if isinstance(domain, RecordDomain):
                row = self.flatten_row(row, domain)
            else:
                row = (row,)
            rows.append(row)
        product.data = rows

    def flatten_row(self, row, domain):
        if row is None:
            row = (None,)*len(domain.fields)
        items = []
        for item, field in zip(row, domain.fields):
            if isinstance(field.domain, RecordDomain):
                item = self.flatten_row(item, field.domain)
                items.extend(item)
            else:
                items.append(item)
        return tuple(items)

    def flatten_meta(self, product):
        header = product.meta.header
        title = []
        if header is not None:
            title.append(header)
        domain = product.meta.domain.item_domain
        if not isinstance(domain, RecordDomain):
            field = Profile(domain=domain, title=title)
            domain = RecordDomain([field])
        else:
            domain = self.flatten_meta_record(domain, title)
        product.meta.title = title
        product.meta.domain.item_domain = domain

    def flatten_meta_record(self, domain, title_prefix):
        fields = []
        for field in domain.fields:
            title = title_prefix
            if field.header is not None:
                title = title+[field.header]
            if isinstance(field.domain, RecordDomain):
                field_domain = self.flatten_meta_record(field.domain, title)
                fields.extend(field_domain.fields)
            else:
                field.title = title
                fields.append(field)
        return RecordDomain(fields)


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


