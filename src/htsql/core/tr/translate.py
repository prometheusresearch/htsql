#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ..util import listof
from ..context import context
from ..domain import ListDomain, RecordDomain, Profile, Product
from ..syn.syntax import Syntax
from ..tr.bind import bind
from ..tr.binding import Binding
from ..tr.encode import encode
from ..tr.space import OrderedSpace
from ..tr.rewrite import rewrite
from ..tr.compile import compile
from ..tr.assemble import assemble
from ..tr.reduce import reduce
from ..tr.dump import serialize
from ..tr.plan import Plan, Statement
from ..connect import transaction, scramble, unscramble
from ..error import PermissionError


class RowStream(object):

    @classmethod
    def open(cls, statement, cursor, input=None):
        converts = [unscramble(domain)
                    for domain in statement.domains]
        sql = statement.sql.encode('utf-8')
        parameters = None
        if statement.placeholders:
            assert input is not None
            parameters = {}
            for index in sorted(statement.placeholders):
                domain = statement.placeholders[index]
                convert = scramble(domain)
                value = convert(input[index])
                parameters[str(index+1)] = value
        if parameters is None:
            cursor.execute(sql)
        else:
            cursor.execute(sql, parameters)
        rows = []
        for row in cursor:
            row = tuple(convert(item)
                        for item, convert in zip(row, converts))
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


class FetchPipe(object):

    def __init__(self, plan):
        assert isinstance(plan, Plan)
        self.plan = plan
        self.profile = plan.profile
        self.statement = plan.statement
        self.compose = plan.compose

    def __call__(self, input=None):
        meta = self.profile.clone(plan=self.plan)
        data = None
        if self.statement:
            if not context.env.can_read:
                raise PermissionError("No read permissions")
            stream = None
            with transaction() as connection:
                cursor = connection.cursor()
                stream = RowStream.open(self.statement, cursor, input)
            data = self.compose(None, stream)
            stream.close()
        return Product(meta, data)


def translate(syntax, environment=None, limit=None):
    assert isinstance(syntax, (Syntax, Binding, unicode, str))
    if isinstance(syntax, (str, unicode)):
        syntax = parse(syntax)
    if not isinstance(syntax, Binding):
        binding = bind(syntax, environment=environment)
    else:
        binding = syntax
    expression = encode(binding)
    if limit is not None:
        expression = safe_patch(expression, limit)
    expression = rewrite(expression)
    term = compile(expression)
    frame = assemble(term)
    frame = reduce(frame)
    plan = serialize(frame)
    return FetchPipe(plan)


def safe_patch(expression, limit):
    segment = expression.segment
    if segment is None:
        return expression
    space = segment.space
    while not space.is_axis:
        if (isinstance(space, OrderedSpace) and space.limit is not None
                                          and space.limit <= limit):
            return expression
        space = space.base
    if space.is_root:
        return expression
    if isinstance(segment.space, OrderedSpace):
        space = segment.space.clone(limit=limit)
    else:
        space = OrderedSpace(segment.space, [], limit, None, segment.binding)
    segment = segment.clone(space=space)
    expression = expression.clone(segment=segment)
    return expression


