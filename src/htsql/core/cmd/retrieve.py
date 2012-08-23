#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ..adapter import adapt, Utility
from ..util import Record, listof
from ..context import context
from ..domain import ListDomain, RecordDomain, Profile
from .command import RetrieveCmd, SQLCmd
from .act import (analyze, Act, ProduceAction, SafeProduceAction,
                  AnalyzeAction, RenderAction)
from ..tr.encode import encode
from ..tr.flow import OrderedFlow
from ..tr.rewrite import rewrite
from ..tr.compile import compile
from ..tr.assemble import assemble
from ..tr.reduce import reduce
from ..tr.dump import serialize
from ..tr.plan import Statement
from ..connect import transaction, normalize
from ..error import PermissionError


class Product(object):

    def __init__(self, meta, data=None):
        assert isinstance(meta, Profile)
        self.meta = meta
        self.data = data

    def __iter__(self):
        if self.data is None:
            return iter([])
        else:
            return iter(self.data)

    def __nonzero__(self):
        return (self.data is not None)


class RowStream(object):

    @classmethod
    def open(cls, statement, cursor):
        normalizers = [normalize(domain)
                       for domain in statement.domains]
        cursor.execute(statement.sql.encode('utf-8'))
        rows = []
        for row in cursor:
            row = tuple(normalizer(item)
                    for item, normalizer in zip(row, normalizers))
            rows.append(row)
        substreams = [cls.open(substatement, cursor)
                      for substatement in statement.substatements]
        return cls(rows, substreams)

    def __init__(self, rows, substreams):
        assert isinstance(rows, list)
        assert isinstance(substreams, listof(RowStream))
        self.rows = rows
        self.substreams = substreams
        self.top = 0
        self.last_top = None
        self.last_key = None

    def __iter__(self):
        self.top = 0
        for row in self.rows:
            yield row
            self.top += 1

    def get(self, stencil):
        return tuple(self.rows[self.top][index]
                     for index in stencil)

    def slice(self, stencil, key):
        if key != self.last_key:
            self.last_top = self.top
            self.last_key = key
            if key != ():
                while self.top < len(self.rows):
                    row = self.rows[self.top]
                    if key != tuple(row[index] for index in stencil):
                        break
                    yield row
                    self.top += 1
            else:
                assert not stencil
                while self.top < len(self.rows):
                    yield self.rows[self.top]
                    self.top += 1
        else:
            top = self.top
            self.top = self.last_top
            for idx in range(self.last_top, top):
                self.top = idx
                yield self.rows[idx]
            self.top = top

    def close(self):
        assert self.top == len(self.rows)
        for substream in self.substreams:
            substream.close()


class ProduceRetrieve(Act):

    adapt(RetrieveCmd, ProduceAction)

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
        meta = plan.profile.clone(plan=plan)
        data = None
        if plan.statement:
            if not context.env.can_read:
                raise PermissionError("not enough permissions"
                                      " to execute the query")
            stream = None
            with transaction() as connection:
                cursor = connection.cursor()
                stream = RowStream.open(plan.statement, cursor)
            data = plan.compose(None, stream)
            stream.close()
        return Product(meta, data)

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

    adapt(RetrieveCmd, AnalyzeAction)

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

    adapt(SQLCmd, RenderAction)
    def __call__(self):
        plan = analyze(self.command.producer)
        status = '200 OK'
        headers = [('Content-Type', 'text/plain; charset=UTF-8')]
        body = []
        if plan.statement:
            queue = [plan.statement]
            while queue:
                statement = queue.pop(0)
                if body:
                    body.append("\n")
                body.append(statement.sql.encode('utf-8'))
                queue.extend(statement.substatements)
        return (status, headers, body)


