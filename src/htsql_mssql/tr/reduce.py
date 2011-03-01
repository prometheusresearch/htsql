#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


from htsql.tr.signature import isformula, FromPredicateSig, ToPredicateSig
from htsql.tr.reduce import ReduceFromPredicate, ReduceToPredicate


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


