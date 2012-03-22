#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from htsql.core.tr.signature import isformula, FromPredicateSig, ToPredicateSig
from htsql.core.tr.reduce import (ReduceFromPredicate, ReduceToPredicate,
                                  InterlinkBranch)


class MSSQLInterlinkBranch(InterlinkBranch):

    def interlink_group(self):
        return self.frame.group


class MSSQLReduceFromPredicate(ReduceFromPredicate):

    def __call__(self):
        op = self.state.reduce(self.phrase.op)
        if isformula(op, ToPredicateSig):
            return op.op
        return self.phrase.clone(is_nullable=op.is_nullable, op=op)


class MSSQLReduceToPredicate(ReduceToPredicate):

    def __call__(self):
        op = self.state.reduce(self.phrase.op)
        if isformula(op, FromPredicateSig):
            return op.op
        return self.phrase.clone(is_nullable=op.is_nullable, op=op)


