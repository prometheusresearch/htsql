#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ..adapter import Protocol, named, adapts
from .format import (HTMLFormat, JSONFormat, ObjFormat, CSVFormat, TSVFormat,
                     XMLFormat, ProxyFormat, TextFormat, Emit, EmitHeaders)


class Accept(Protocol):

    format = TextFormat

    def __init__(self, content_type):
        self.content_type = content_type

    def __call__(self):
        return self.format()


class AcceptAny(Accept):

    named("*/*")
    format = HTMLFormat


class AcceptJSON(Accept):

    named("application/javascript",
          "application/json",
          "x-htsql/x-json")
    format = JSONFormat


class AcceptObj(Accept):

    named("x-htsql/x-obj")
    format = ObjFormat


class AcceptCSV(Accept):

    named("text/csv",
          "x-htsql/x-csv")
    format = CSVFormat


class AcceptTSV(AcceptCSV):

    named("text/tab-separated-values",
          "x-htsql/x-tsv")
    format = TSVFormat


class AcceptHTML(Accept):

    named("text/html",
          "x-htsql/x-html")
    format = HTMLFormat


class AcceptXML(Accept):

    named("application/xml",
          "x-htsql/x-xml")
    format = XMLFormat


class AcceptText(Accept):

    named("text/plain",
          "x-htsql/x-txt")
    format = TextFormat


class EmitProxyHeaders(EmitHeaders):

    adapts(ProxyFormat)

    def __call__(self):
        emit_headers = EmitHeaders(self.format.format, self.product)
        for header in emit_headers():
            yield header
        yield ('Vary', 'Accept')


class EmitProxy(Emit):

    adapts(ProxyFormat)

    def __call__(self):
        emit = Emit(self.format.format, self.product)
        return emit()


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
    accept = Accept(content_type)
    return ProxyFormat(accept())


