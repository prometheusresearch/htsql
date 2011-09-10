#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from ..adapter import adapts, Utility
from .command import UniversalCmd, DefaultCmd, RetrieveCmd
from .act import act, Act, ProduceAction
from ..tr.lookup import lookup_command
from ..tr.parse import parse
from ..tr.bind import bind
from ..tr.encode import encode
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


class ProduceUniversal(Act):

    adapts(UniversalCmd, ProduceAction)

    def __call__(self):
        syntax = parse(self.command.query)
        binding = bind(syntax)
        command = lookup_command(binding)
        if command is None:
            command = DefaultCmd(binding)
        return act(command, self.action)


class ProduceDefault(Act):

    adapts(DefaultCmd, ProduceAction)

    def __call__(self):
        command = RetrieveCmd(self.command.binding)
        return act(command, self.action)


class ProduceRetrieve(Act):

    adapts(RetrieveCmd, ProduceAction)

    def __call__(self):
        binding = self.command.binding
        expression = encode(binding)
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
                raise EngineError("error while executing %r: %s"
                                  % (plan.sql, exc))
            except:
                if connection is not None:
                    connection.invalidate()
        return Product(profile, records)


