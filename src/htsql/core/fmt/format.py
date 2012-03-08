#
# Copyright (c) 2006-2012, Prometheus Research, LLC
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


class TSVFormat(CSVFormat):

    def __init__(self, dialect='excel-tab'):
        super(TSVFormat, self).__init__(dialect)


class HTMLFormat(Format):
    pass


class TextFormat(Format):
    pass


class XMLFormat(Format):
    pass


class ProxyFormat(Format):

    def __init__(self, format):
        assert isinstance(format, Format)
        self.format = format


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


def emit_headers(format, product):
    emit_headers = EmitHeaders(format, product)
    return list(emit_headers())


def emit(format, headers):
    emit = Emit(format, headers)
    for line in emit():
        if isinstance(line, unicode):
            line = line.encode('utf-8')
        yield line


