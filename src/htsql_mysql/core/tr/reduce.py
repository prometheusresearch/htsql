#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from htsql.core.adapter import adapt
from htsql.core.tr.frame import ScalarFrame, LeadingAnchor, TruePhrase
from htsql.core.tr.reduce import ReduceBranch, ReduceBySignature
from .signature import NoOpConditionSig


class MySQLReduceBranch(ReduceBranch):

    def __call__(self):
        frame = super(MySQLReduceBranch, self).__call__()
        if not frame.include and frame.where is not None:
            include = [LeadingAnchor(
                self.state.reduce(ScalarFrame(self.frame.term)))]
            frame = frame.clone(include=include)
        return frame


class MySQLReduceNoOpCondition(ReduceBySignature):

    adapt(NoOpConditionSig)

    def __call__(self):
        return TruePhrase(self.phrase.expression)


