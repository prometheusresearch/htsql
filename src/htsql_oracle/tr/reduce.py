#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


from htsql.domain import StringDomain
from htsql.tr.signature import isformula, ToPredicateSig, FromPredicateSig
from htsql.tr.frame import ScalarFrame, NullPhrase, LeadingAnchor
from htsql.tr.reduce import (ReduceScalar, ReduceBranch, ReduceLiteral,
                             ReduceFromPredicate, ReduceToPredicate)


class OracleReduceScalar(ReduceScalar):

    def __call__(self):
        return self.frame


class OracleReduceBranch(ReduceBranch):

    def reduce_include(self):
        include = super(OracleReduceBranch, self).reduce_include()
        if not include:
            frame = ScalarFrame(self.frame.term)
            anchor = LeadingAnchor(frame)
            include = [anchor]
        return include


class OracleReduceLiteral(ReduceLiteral):

    def __call__(self):
        if (isinstance(self.phrase.domain, StringDomain) and
            self.phrase.value == ""):
            return NullPhrase(self.phrase.domain, self.phrase.expression)
        return super(OracleReduceLiteral, self).__call__()


class OracleReduceFromPredicate(ReduceFromPredicate):

    def __call__(self):
        op = self.state.reduce(self.phrase.op)
        if isformula(op, ToPredicateSig):
            return op.op
        return self.phrase.clone(is_nullable=op.is_nullable, op=op)


class OracleReduceToPredicate(ReduceToPredicate):

    def __call__(self):
        op = self.state.reduce(self.phrase.op)
        if isformula(op, FromPredicateSig):
            return op.op
        return self.phrase.clone(is_nullable=op.is_nullable, op=op)


