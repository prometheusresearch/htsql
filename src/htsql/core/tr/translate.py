#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


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


def translate(syntax, environment=None, limit=None, offset=None, batch=None):
    assert isinstance(syntax, (Syntax, Binding, unicode, str))
    if isinstance(syntax, (str, unicode)):
        syntax = parse(syntax)
    if not isinstance(syntax, Binding):
        binding = bind(syntax, environment=environment)
    else:
        binding = syntax
    profile = decorate(binding)
    flow = route(binding)
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
    return ProducePipe(profile, pipe, sql=sql)


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
                merged_sqls.append(u"\n".join(u"  "+line if line else u""
                                              for line in sql.splitlines()))
            return u"\n\n".join(merged_sqls)


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


