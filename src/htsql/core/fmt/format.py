#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql.core.fmt.format`
============================

This module implements the format adapter.
"""


from ..util import setof
from ..adapter import Adapter, Utility, adapt
from ..domain import Domain, ListDomain, RecordDomain, Profile


class Format(object):
    pass


class RawFormat(Format):
    pass


class JSONFormat(Format):
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

    adapt(Format)

    def __init__(self, format, product):
        self.format = format
        self.product = product
        self.meta = product.meta
        self.data = product.data

    def __call__(self):
        raise NotImplementedError()


class Emit(Adapter):

    adapt(Format)

    def __init__(self, format, product):
        self.format = format
        self.product = product
        self.meta = product.meta
        self.data = product.data

    def __call__(self):
        raise NotImplementedError()


def emit_headers(format, product):
    return list(EmitHeaders.__invoke__(format, product))


def emit(format, headers):
    for line in Emit.__invoke__(format, headers):
        if isinstance(line, unicode):
            line = line.encode('utf-8')
        yield line


