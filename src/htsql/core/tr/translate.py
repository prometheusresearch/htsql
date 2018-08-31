#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ..context import context
from ..syn.syntax import Syntax
from ..syn.parse import parse
from .bind import bind
from .binding import Binding
from .decorate import decorate
from .route import route
from .encode import encode
from .space import OrderedSpace
from .rewrite import rewrite
from .compile import compile
from .assemble import assemble
from .reduce import reduce
from .dump import serialize
from .pack import pack
from .pipe import SQLPipe, RecordPipe, ComposePipe, ProducePipe


class CacheItem:

    __slots__ = ('prev', 'next', 'key', 'value')

    def __init__(self, prev, next, key, value):
        self.prev = prev
        self.next = next
        self.key = key
        self.value = value


class LRUCache:

    __slots__ = ('head', 'tail', 'items', 'size')

    def __init__(self, size):
        self.head = None
        self.tail = None
        self.items = {}
        self.size = size

    def __getitem__(self, key):
        item = self.items[key]
        if item.prev is not None:
            item.prev.next = item.next
            if item.next is not None:
                item.next.prev = item.prev
            else:
                self.tail = item.prev
            item.prev = None
            item.next = self.head
            self.head.prev = item
            self.head = item
        return item.value

    def __setitem__(self, key, value):
        try:
            self.items[key].value = value
        except KeyError:
            item = CacheItem(None, self.head, key, value)
            if self.head is not None:
                self.head.prev = item
            else:
                self.tail = item
            self.head = item
            self.items[key] = item
            if len(self.items) > self.size:
                del self.items[self.tail.key]
                self.tail.prev.next = None
                self.tail = self.tail.prev

    def __len__(self):
        return len(self.items)


def cache_plan(key, plan):
    cache = context.app.htsql.cache
    with cache.lock(cache_plan):
        try:
            mapping = cache.values[cache_plan]
        except KeyError:
            size = context.app.htsql.query_cache_size
            if not size:
                return
            mapping = cache.values[cache_plan] = LRUCache(size=size)
        mapping[key] = plan


def get_cached_plan(key, cache_plan=cache_plan):
    cache = context.app.htsql.cache
    with cache.lock(cache_plan):
        try:
            return cache.values[cache_plan][key]
        except KeyError:
            return None


def translate(syntax, environment=None, limit=None, offset=None, batch=None):
    assert isinstance(syntax, (Syntax, Binding, str))
    if isinstance(syntax, str):
        syntax = parse(syntax)
    if not isinstance(syntax, Binding):
        binding = bind(syntax, environment=environment)
    else:
        binding = syntax
    profile = decorate(binding)
    flow = route(binding)
    key = (profile.tag, flow, limit, offset, batch)
    pipe_sql = get_cached_plan(key)
    if pipe_sql is not None:
        pipe, sql = pipe_sql
        pipe = ProducePipe(profile, pipe, sql=sql)
        return pipe
    expression = encode(flow)
    if limit is not None or offset is not None:
        expression = safe_patch(expression, limit, offset)
    expression = rewrite(expression)
    term = compile(expression)
    frame = assemble(term)
    frame = reduce(frame)
    raw_pipe = serialize(frame, batch=batch)
    sql = get_sql(raw_pipe)
    value_pipe = pack(flow, frame, profile.tag)
    pipe = ComposePipe(raw_pipe, value_pipe)
    #print pipe
    cache_plan(key, (pipe, sql))
    pipe = ProducePipe(profile, pipe, sql=sql)
    return pipe


def get_sql(pipe):
    if isinstance(pipe, SQLPipe):
        return pipe.sql
    if isinstance(pipe, ComposePipe):
        return get_sql(pipe.left_pipe)
    elif isinstance(pipe, RecordPipe):
        sqls = []
        for field_pipe in pipe.field_pipes:
            sql = get_sql(field_pipe)
            if sql:
                sqls.append(sql)
        if sqls:
            merged_sqls = [sqls[0]]
            for sql in sqls[1:]:
                merged_sqls.append("\n".join("  "+line if line else ""
                                              for line in sql.splitlines()))
            return "\n\n".join(merged_sqls)


def safe_patch(segment, limit, offset):
    space = segment.space
    if limit is not None:
        while not space.is_axis:
            if (isinstance(space, OrderedSpace) and space.limit is not None
                                              and space.limit <= limit
                                              and offset is None):
                return segment
            space = space.base
        if space.is_root:
            return segment
    if isinstance(segment.space, OrderedSpace):
        space = segment.space.clone(limit=limit, offset=offset)
    else:
        space = OrderedSpace(segment.space, [], limit, offset, segment.flow)
    segment = segment.clone(space=space)
    return segment


