#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ...core.context import context
from ...core.tr.encode import EncodeSegment
from ...core.tr.flow import OrderedFlow


class AutolimitEncodeSegment(EncodeSegment):

    def __call__(self):
        code = super(AutolimitEncodeSegment, self).__call__()
        limit = context.app.tweak.autolimit.limit
        if limit is None or limit <= 0:
            return code
        flow = code.flow
        while flow.is_contracting:
            if (isinstance(flow, OrderedFlow) and flow.limit is not None
                                              and flow.limit <= limit):
                return code
            flow = flow.base
        if flow.is_root:
            return code
        flow = OrderedFlow(code.flow, [], limit, None, code.binding)
        return code.clone(flow=flow)


