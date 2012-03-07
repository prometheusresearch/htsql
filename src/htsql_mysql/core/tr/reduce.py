#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from htsql.core.tr.frame import ScalarFrame, LeadingAnchor
from htsql.core.tr.reduce import ReduceBranch


class MySQLReduceBranch(ReduceBranch):

    def __call__(self):
        frame = super(MySQLReduceBranch, self).__call__()
        if not frame.include and frame.where is not None:
            include = [LeadingAnchor(
                self.state.reduce(ScalarFrame(self.frame.term)))]
            frame = frame.clone(include=include)
        return frame


