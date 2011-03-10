#
# Copyright (c) 2006-2011, Prometheus Research, LLC
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
from .error import EngineError, InvalidArgumentError
from .tr.parse import parse
from .tr.bind import bind
from .tr.encode import encode
from .tr.rewrite import rewrite
from .tr.compile import compile
from .tr.assemble import assemble
from .tr.reduce import reduce
from .tr.dump import serialize
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
        binding = bind(syntax)
        expression = encode(binding)
        expression = rewrite(expression)
        term = compile(expression)
        frame = assemble(term)
        frame = reduce(frame)
        plan = serialize(frame)
        return plan

    def produce(self):
        plan = self.translate()
        profile = RequestProfile(plan)
        records = None
        if plan.sql:
            select = plan.frame.segment.select
            normalizers = []
            for phrase in select:
                normalize = Normalize(phrase.domain)
                normalizers.append(normalize)
            connection = None
            try:
                connect = Connect()
                connection = connect()
                cursor = connection.cursor()
                cursor.execute(plan.sql)
                records = []
                for row in cursor:
                    values = []
                    for item, normalize in zip(row, normalizers):
                        value = normalize(item)
                        values.append(value)
                    records.append((values))
                connection.release()
            except DBError, exc:
                raise EngineError("error while executing %r: %s"
                                  % (plan.sql, exc), plan.mark)
            except:
                if connection is not None:
                    connection.invalidate()
        return Product(profile, records)

    def render(self, environ):
        product = self.produce()
        find_renderer = FindRenderer()
        format = product.profile.syntax.format
        if format is not None:
            accept = set([format.value])
            renderer_class = find_renderer(accept)
            if renderer_class is None:
                raise InvalidArgumentError("unknown format", format.mark)
        else:
            accept = set([''])
            if 'HTTP_ACCEPT' in environ:
                for name in environ['HTTP_ACCEPT'].split(','):
                    if ';' in name:
                        name = name.split(';', 1)[0]
                    name = name.strip()
                    accept.add(name)
            renderer_class = find_renderer(accept)
            assert renderer_class is not None
        renderer = renderer_class()
        return renderer.render(product)

    def __call__(self, environ):
        return self.render(environ)


def render(environ):
    request = Request.build(environ)
    return request.render(environ)


def produce(uri):
    request = Request(uri)
    return request.produce()


