#
# Copyright (c) 2006-2012, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from ..adapter import adapts, Utility
from ..util import Record
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
from ..tr.lookup import guess_name
from ..connect import DBError, Connect, normalize
from ..error import EngineError


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
        profile = plan.profile.clone(plan=plan)
        records = None
        if plan.sql:
            assert isinstance(profile.domain, ListDomain)
            assert isinstance(profile.domain.item_domain, RecordDomain)
            fields = profile.domain.item_domain.fields
            normalizers = []
            for field in fields:
                normalizers.append(normalize(field.domain))
            record_name = profile.name
            field_names = [field.name for field in fields]
            record_class = Record.make(record_name, field_names)
            connection = None
            try:
                connect = Connect()
                connection = connect()
                cursor = connection.cursor()
                cursor.execute(plan.sql.encode('utf-8'))
                records = []
                for row in cursor:
                    values = []
                    for item, normalizer in zip(row, normalizers):
                        value = normalizer(item)
                        values.append(value)
                    records.append(record_class(*values))
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
            body = [plan.sql.encode('utf-8')]
        return (status, headers, body)


