#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.request`
====================

This module implements the request utility.
"""


from .adapter import Utility, Realization
from .connect import DBError, Connect, Normalize
from .error import EngineError
from .tr.parse import parse
from .tr.binder import Binder
from .tr.encoder import Encoder
from .tr.assembler import Assembler
from .tr.outliner import Outliner
from .tr.compiler import Compiler
from .tr.serializer import Serializer
#from .fmt.text import TextRenderer
#from .fmt.spreadsheet import CSVRenderer
from .fmt.format import FindRenderer
import urllib


class ElementProfile(object):

    def __init__(self, binding):
        self.binding = binding
        self.domain = binding.domain
        self.syntax = binding.syntax
        self.mark = binding.mark


class SegmentProfile(object):

    def __init__(self, binding):
        self.binding = binding
        self.syntax = binding.syntax
        self.mark = binding.mark
        self.elements = [ElementProfile(element)
                         for element in binding.elements]


class RequestProfile(object):

    def __init__(self, plan):
        self.plan = plan
        self.binding = plan.binding
        self.syntax = plan.syntax
        self.mark = plan.mark
        self.segment = None
        if plan.frame.segment is not None:
            self.segment = SegmentProfile(plan.binding.segment)


class Product(Utility):

    def __init__(self, profile, records=None):
        self.profile = profile
        self.records = records

    def __iter__(self):
        if self.records is not None:
            return iter(self.records)
        else:
            return iter([])

    def __nonzero__(self):
        return (self.records is not None)


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

    def translate(self):
        syntax = parse(self.uri)
        binder = Binder()
        binding = binder.bind_one(syntax)
        encoder = Encoder()
        code = encoder.encode(binding)
        assembler = Assembler()
        term = assembler.assemble(code)
        outliner = Outliner()
        sketch = outliner.outline(term)
        compiler = Compiler()
        frame = compiler.compile(sketch)
        serializer = Serializer()
        plan = serializer.serialize(frame)
        return plan

    def produce(self):
        plan = self.translate()
        profile = RequestProfile(plan)
        records = None
        if plan.sql:
            try:
                connect = Connect()
                connection = connect()
                cursor = connection.cursor()
                cursor.execute(plan.sql)
                rows = cursor.fetchall()
                connection.close()
            except DBError, exc:
                raise EngineError("error while executing %r: %s"
                                  % (plan.sql, exc), plan.mark)
            records = []
            select = plan.frame.segment.select
            normalizers = []
            for phrase in plan.frame.segment.select:
                normalize = Normalize(phrase.domain)
                normalizers.append(normalize)
            for row in rows:
                values = []
                for item, normalize in zip(row, normalizers):
                    value = normalize(item)
                    values.append(value)
                records.append((values))
        return Product(profile, records)

    def render(self, environ):
        accept = set([''])
        if 'HTTP_ACCEPT' in environ:
            for name in environ['HTTP_ACCEPT'].split(','):
                if ';' in name:
                    name = name.split(';', 1)[0]
                name = name.strip()
                accept.add(name)
        find_renderer = FindRenderer()
        renderer_class = find_renderer(accept)
        assert renderer_class is not None
        renderer = renderer_class()
        product = self.produce()
        return renderer.render(product)

    def __call__(self, environ):
        return self.render(environ)


