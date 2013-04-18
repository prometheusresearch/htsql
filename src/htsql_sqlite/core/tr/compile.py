#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from htsql.core.domain import BooleanDomain
from htsql.core.tr.term import (WrapperTerm, FilterTerm, OrderTerm,
        CorrelationTerm, EmbeddingTerm)
from htsql.core.tr.space import (LiteralCode, FormulaCode, ScalarUnit,
        CorrelationCode)
from htsql.core.tr.coerce import coerce
from htsql.core.tr.signature import IsEqualSig, AndSig
from htsql.core.tr.compile import CompileCovering
from htsql.core.tr.stitch import arrange
from .signature import IsAnySig


class SQLiteCompileCovering(CompileCovering):

    def clip(self, term, order, partition):
        baseline = self.space.ground
        while not baseline.is_inflated:
            baseline = baseline.base
        kid = self.state.compile(self.space.seed, baseline=baseline)
        codes = [code for code, direction in order]
        codes += self.space.companions
        kid = self.state.inject(kid, codes)
        kid = WrapperTerm(self.state.tag(), kid,
                          kid.space, kid.baseline, kid.routes.copy())
        limit = self.space.limit
        if limit is None:
            limit = 1
        offset = self.space.offset
        key = []
        for code, direction in arrange(self.space.seed, with_strong=False):
            if all(self.space.base.spans(unit.space)
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
                                 self.space.flow, lop=lop, rop=rop)
            filters.append(filter)
        if len(filters) == 0:
            filter = None
        elif len(filters) == 1:
            [filter] = filters
        else:
            filter = FormulaCode(AndSig(), coerce(BooleanDomain()),
                                 self.space.flow, ops=filters)
        if filter is not None:
            kid = FilterTerm(self.state.tag(), kid, filter,
                             kid.space, kid.baseline, kid.routes.copy())
        kid = OrderTerm(self.state.tag(), kid, order, limit, offset,
                        kid.space, kid.baseline, kid.routes.copy())
        kid = CorrelationTerm(self.state.tag(), kid,
                              kid.space, kid.baseline, kid.routes.copy())
        if len(key) == 1:
            lop = rop = key[0]
            rop = ScalarUnit(rop, kid.space, kid.space.flow)
        else:
            filters = []
            for code in key:
                correlations.append(code)
                lop = CorrelationCode(code)
                rop = code
                filter = FormulaCode(IsEqualSig(+1), coerce(BooleanDomain()),
                                 self.space.flow, lop=lop, rop=rop)
                filters.append(filter)
            filter = FormulaCode(AndSig(), coerce(BooleanDomain()),
                                 self.space.flow, ops=filters)
            lop = LiteralCode(True, coerce(BooleanDomain()),
                              self.space.flow)
            rop = ScalarUnit(filter, kid.space, kid.space.flow)
        routes = term.routes.copy()
        routes[rop] = kid.tag
        kid = EmbeddingTerm(self.state.tag(), term, kid,
                            correlations,
                            term.space, term.baseline, routes)
        filter = FormulaCode(IsAnySig(+1), coerce(BooleanDomain()),
                             self.space.flow, lop=lop, rop=rop)
        return FilterTerm(self.state.tag(), kid, filter,
                          kid.space, kid.baseline, term.routes.copy())


