#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ...core.context import context
from ...core.tr.encode import EncodeSegment
from ...core.tr.space import OrderedSpace


class AutolimitEncodeSegment(EncodeSegment):

    def __call__(self):
        code = super(AutolimitEncodeSegment, self).__call__()
        limit = context.app.tweak.autolimit.limit
        if limit is None or limit <= 0:
            return code
        space = code.space
        while space.is_contracting:
            if (isinstance(space, OrderedSpace) and space.limit is not None
                                              and space.limit <= limit):
                return code
            space = space.base
        if space.is_root:
            return code
        space = OrderedSpace(code.space, [], limit, None, code.binding)
        return code.clone(space=space)


