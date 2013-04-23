#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ..syn.syntax import Syntax
from ..tr.bind import bind
from ..tr.binding import Binding
from ..tr.route import route
from ..tr.encode import encode
from ..tr.space import OrderedSpace
from ..tr.rewrite import rewrite
from ..tr.compile import compile
from ..tr.assemble import assemble
from ..tr.reduce import reduce
from ..tr.dump import serialize


def translate(syntax, environment=None, limit=None):
    assert isinstance(syntax, (Syntax, Binding, unicode, str))
    if isinstance(syntax, (str, unicode)):
        syntax = parse(syntax)
    if not isinstance(syntax, Binding):
        binding = bind(syntax, environment=environment)
    else:
        binding = syntax
    flow = route(binding)
    expression = encode(flow)
    if limit is not None:
        expression = safe_patch(expression, limit)
    expression = rewrite(expression)
    term = compile(expression)
    frame = assemble(term)
    frame = reduce(frame)
    pipe = serialize(frame)
    return pipe


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
        space = OrderedSpace(segment.space, [], limit, None, segment.flow)
    segment = segment.clone(space=space)
    expression = expression.clone(segment=segment)
    return expression


