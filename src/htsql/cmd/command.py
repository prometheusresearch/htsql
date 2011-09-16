#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from ..util import Printable
from ..fmt import (TextRenderer, HTMLRenderer, JSONRenderer,
                   CSVRenderer, TSVRenderer)


class Command(Printable):
    pass


class UniversalCmd(Command):

    def __init__(self, query):
        assert isinstance(query, str)
        self.query = query

    def __str__(self):
        return repr(self.query)


class DefaultCmd(Command):

    def __init__(self, binding):
        self.binding = binding

    def __str__(self):
        return str(self.binding)


class ProducerCmd(Command):
    pass


class RetrieveCmd(ProducerCmd):

    def __init__(self, binding):
        self.binding = binding

    def __str__(self):
        return str(self.binding)


class RendererCmd(Command):

    format = None

    def __init__(self, producer):
        assert isinstance(producer, Command)
        self.producer = producer


class TextCmd(RendererCmd):

    format = TextRenderer


class HTMLCmd(RendererCmd):

    format = HTMLRenderer


class JSONCmd(RendererCmd):

    format = JSONRenderer


class CSVCmd(RendererCmd):

    format = CSVRenderer


class TSVCmd(RendererCmd):

    format = TSVRenderer


class SQLCmd(Command):

    def __init__(self, producer):
        assert isinstance(producer, Command)
        self.producer = producer


