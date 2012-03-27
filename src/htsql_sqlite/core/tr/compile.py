#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from htsql.core.domain import BooleanDomain
from htsql.core.tr.term import (WrapperTerm, FilterTerm, OrderTerm,
                                CorrelationTerm, EmbeddingTerm)
from htsql.core.tr.flow import (LiteralCode, FormulaCode, ScalarUnit,
                                CorrelationCode)
from htsql.core.tr.coerce import coerce
from htsql.core.tr.signature import IsEqualSig, AndSig
from htsql.core.tr.compile import CompileCovering
from htsql.core.tr.stitch import arrange
from .signature import IsAnySig


class SQLiteCompileCovering(CompileCovering):

    def clip(self, term, order, partition):
        baseline = self.flow.ground
        while not baseline.is_inflated:
            baseline = baseline.base
        kid = self.state.compile(self.flow.seed, baseline=baseline)
        codes = [code for code, direction in order]
        codes += self.flow.companions
        kid = self.state.inject(kid, codes)
        kid = WrapperTerm(self.state.tag(), kid,
                          kid.flow, kid.baseline, kid.routes.copy())
        limit = self.flow.limit
        if limit is None:
            limit = 1
        offset = self.flow.offset
        key = []
        for code, direction in arrange(self.flow.seed, with_strong=False):
            if all(self.flow.base.spans(unit.flow)
                   for unit in code.units):
                continue
            key.append(code)
        assert key
        correlations = []
        filters = []
        for code in partition:
            correlations.append(code)
            lop = CorrelationCode(code)
            rop = code
            filter = FormulaCode(IsEqualSig(+1), coerce(BooleanDomain()),
                                 self.flow.binding, lop=lop, rop=rop)
            filters.append(filter)
        if len(filters) == 0:
            filter = None
        elif len(filters) == 1:
            [filter] = filters
        else:
            filter = FormulaCode(AndSig(), coerce(BooleanDomain()),
                                 self.flow.binding, ops=filters)
        if filter is not None:
            kid = FilterTerm(self.state.tag(), kid, filter,
                             kid.flow, kid.baseline, kid.routes.copy())
        kid = OrderTerm(self.state.tag(), kid, order, limit, offset,
                        kid.flow, kid.baseline, kid.routes.copy())
        kid = CorrelationTerm(self.state.tag(), kid,
                              kid.flow, kid.baseline, kid.routes.copy())
        if len(key) == 1:
            lop = rop = key[0]
            rop = ScalarUnit(rop, kid.flow, kid.flow.binding)
        else:
            filters = []
            for code in key:
                correlations.append(code)
                lop = CorrelationCode(code)
                rop = code
                filter = FormulaCode(IsEqualSig(+1), coerce(BooleanDomain()),
                                 self.flow.binding, lop=lop, rop=rop)
                filters.append(filter)
            filter = FormulaCode(AndSig(), coerce(BooleanDomain()),
                                 self.flow.binding, ops=filters)
            lop = LiteralCode(True, coerce(BooleanDomain()),
                              self.flow.binding)
            rop = ScalarUnit(filter, kid.flow, kid.flow.binding)
        routes = term.routes.copy()
        routes[rop] = kid.tag
        kid = EmbeddingTerm(self.state.tag(), term, kid,
                            correlations,
                            term.flow, term.baseline, routes)
        filter = FormulaCode(IsAnySig(+1), coerce(BooleanDomain()),
                             self.flow.binding, lop=lop, rop=rop)
        return FilterTerm(self.state.tag(), kid, filter,
                          kid.flow, kid.baseline, term.routes.copy())


