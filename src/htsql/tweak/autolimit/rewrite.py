#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ...core.context import context
from ...core.tr.rewrite import RewriteSegment
from ...core.tr.space import OrderedSpace


class AutolimitRewriteSegment(RewriteSegment):

    def __call__(self):
        segment = super(AutolimitRewriteSegment, self).__call__()
        limit = context.app.tweak.autolimit.limit
        if limit is None or limit <= 0:
            return segment
        space = segment.space
        while space.is_contracting:
            if (isinstance(space, OrderedSpace) and space.limit is not None
                                              and space.limit <= limit):
                return segment
            space = space.base
        if space.is_root:
            return segment
        space = OrderedSpace(segment.space, [], limit, None, segment.space.flow)
        return segment.clone(space=space)


