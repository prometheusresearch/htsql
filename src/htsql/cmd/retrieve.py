#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from ..adapter import adapts, Utility
from .command import RetrieveCmd, SQLCmd
from .act import (act, analyze, Act, ProduceAction, SafeProduceAction,
                  AnalyzeAction, RenderAction)
from ..tr.encode import encode
from ..tr.flow import OrderedFlow
from ..tr.rewrite import rewrite
from ..tr.compile import compile
from ..tr.assemble import assemble
from ..tr.reduce import reduce
from ..tr.dump import serialize
from ..connect import DBError, Connect, Normalize
from ..error import EngineError


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


class ProduceRetrieve(Act):

    adapts(RetrieveCmd, ProduceAction)

    def __call__(self):
        binding = self.command.binding
        expression = encode(binding)
        # FIXME: abstract it out.
        if isinstance(self.action, SafeProduceAction):
            limit = self.action.limit
            expression = self.safe_patch(expression, limit)
        expression = rewrite(expression)
        term = compile(expression)
        frame = assemble(term)
        frame = reduce(frame)
        plan = serialize(frame)
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
                connection.commit()
                connection.release()
            except DBError, exc:
                raise EngineError("failed to execute a database query: %s"
                                  % exc)
            except:
                if connection is not None:
                    connection.invalidate()
                raise
        return Product(profile, records)

    def safe_patch(self, expression, limit):
        segment = expression.segment
        if segment is None:
            return expression
        flow = segment.flow
        while not flow.is_axis:
            if (isinstance(flow, OrderedFlow) and flow.limit is not None
                                              and flow.limit <= limit):
                return expression
            flow = flow.base
        if flow.is_root:
            return expression
        if isinstance(segment.flow, OrderedFlow):
            flow = segment.flow.clone(limit=limit)
        else:
            flow = OrderedFlow(segment.flow, [], limit, None, segment.binding)
        segment = segment.clone(flow=flow)
        expression = expression.clone(segment=segment)
        return expression


class AnalyzeRetrieve(Act):

    adapts(RetrieveCmd, AnalyzeAction)

    def __call__(self):
        binding = self.command.binding
        expression = encode(binding)
        expression = rewrite(expression)
        term = compile(expression)
        frame = assemble(term)
        frame = reduce(frame)
        plan = serialize(frame)
        return plan


class RenderSQL(Act):

    adapts(SQLCmd, RenderAction)
    def __call__(self):
        plan = analyze(self.command.producer)
        status = '200 OK'
        headers = [('Content-Type', 'text/plain; charset=UTF-8')]
        body = []
        if plan.sql:
            body = [plan.sql]
        return (status, headers, body)


