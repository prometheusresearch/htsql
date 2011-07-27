#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.tr.encode import EncodeSegment
from htsql.tr.flow import OrderedFlow


class AutolimitEncodeSegment(EncodeSegment):

    default_limit = 10000

    def __call__(self):
        code = super(AutolimitEncodeSegment, self).__call__()
        flow = code.flow
        while isinstance(flow, OrderedFlow):
            if flow.limit is not None and flow.limit < self.default_limit:
                return code
            flow = flow.base
        flow = OrderedFlow(code.flow, [], self.default_limit, None,
                             code.binding)
        return code.clone(flow=flow)


