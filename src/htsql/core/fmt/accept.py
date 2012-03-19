#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ..adapter import Protocol, call, adapt
from .format import (HTMLFormat, RawFormat, JSONFormat, CSVFormat, TSVFormat,
                     XMLFormat, ProxyFormat, TextFormat, Emit, EmitHeaders,
                     emit, emit_headers)


class Accept(Protocol):

    format = TextFormat

    def __init__(self, content_type):
        self.content_type = content_type

    def __call__(self):
        return self.format()


class AcceptAny(Accept):

    call("*/*")
    format = HTMLFormat


class AcceptRaw(Accept):

    call("x-htsql/raw")
    format = RawFormat


class AcceptJSON(Accept):

    call("application/javascript",
         "application/json",
         "x-htsql/json")
    format = JSONFormat


class AcceptCSV(Accept):

    call("text/csv",
         "x-htsql/csv")
    format = CSVFormat


class AcceptTSV(AcceptCSV):

    call("text/tab-separated-values",
         "x-htsql/tsv")
    format = TSVFormat


class AcceptHTML(Accept):

    call("text/html",
         "x-htsql/html")
    format = HTMLFormat


class AcceptXML(Accept):

    call("application/xml",
         "x-htsql/xml")
    format = XMLFormat


class AcceptText(Accept):

    call("text/plain",
         "x-htsql/txt")
    format = TextFormat


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


def accept(environ):
    content_type = ""
    if 'HTTP_ACCEPT' in environ:
        content_types = environ['HTTP_ACCEPT'].split(',')
        if len(content_types) == 1:
            [content_type] = content_types
            if ';' in content_type:
                content_type = content_type.split(';', 1)[0]
                content_type = content_type.strip()
        else:
            content_type = "*/*"
    format = Accept.__invoke__(content_type)
    return ProxyFormat(format)


