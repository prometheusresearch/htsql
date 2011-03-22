#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


from htsql.tr.encode import EncodeSegment
from htsql.tr.code import OrderedSpace


class AutolimitEncodeSegment(EncodeSegment):

    default_limit = 10000

    def __call__(self):
        code = super(AutolimitEncodeSegment, self).__call__()
        space = code.space
        while isinstance(space, OrderedSpace):
            if space.limit is not None and space.limit < self.default_limit:
                return code
            space = space.base
        space = OrderedSpace(code.space, [], self.default_limit, None,
                             code.binding)
        return code.clone(space=space)


