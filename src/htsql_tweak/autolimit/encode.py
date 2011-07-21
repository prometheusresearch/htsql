#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.context import context
from htsql.tr.encode import EncodeSegment
from htsql.tr.code import OrderedSpace


class AutolimitEncodeSegment(EncodeSegment):

    def __call__(self):
        code = super(AutolimitEncodeSegment, self).__call__()
        limit = context.app.tweak.autolimit.limit
        if limit is None:
            return code
        space = code.space
        while isinstance(space, OrderedSpace):
            if space.limit is not None and space.limit < limit:
                return code
            space = space.base
        space = OrderedSpace(code.space, [], limit, None, code.binding)
        return code.clone(space=space)


