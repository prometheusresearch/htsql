#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ..adapter import Adapter, adapt
from .format import Format, DefaultFormat, TextFormat, ProxyFormat
from .accept import Accept
import itertools


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


class EmitDefaultHeaders(EmitHeaders):

    adapt(DefaultFormat)

    def __call__(self):
        format = TextFormat()
        return emit_headers(format, self.product)


class EmitDefault(Emit):

    adapt(DefaultFormat)

    def __call__(self):
        format = TextFormat()
        return emit(format, self.product)


class EmitProxyHeaders(EmitHeaders):

    adapt(ProxyFormat)

    def __call__(self):
        for header in emit_headers(self.format.format, self.product):
            yield header
        yield ('Vary', 'Accept')


class EmitProxy(Emit):

    adapt(ProxyFormat)

    def __call__(self):
        return emit(self.format.format, self.product)


def emit_headers(format, product):
    if isinstance(format, str):
        format = Accept.__invoke__(format)
        assert not isinstance(format, DefaultFormat), "unknown format"
    return list(EmitHeaders.__invoke__(format, product))


def emit(format, product):
    if isinstance(format, str):
        format = Accept.__invoke__(format)
        assert not isinstance(format, DefaultFormat), "unknown format"
    tail = (line.encode('utf-8') if isinstance(line, str) else line
            for line in Emit.__invoke__(format, product))
    head = []
    for chunk in tail:
        head.append(chunk)
        break
    return itertools.chain(head, tail)


