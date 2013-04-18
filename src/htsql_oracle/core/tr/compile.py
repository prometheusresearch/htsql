#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from htsql.core.domain import BooleanDomain, IntegerDomain
from htsql.core.tr.term import PermanentTerm, FilterTerm, OrderTerm, WrapperTerm
from htsql.core.tr.space import LiteralCode, FormulaCode, ScalarUnit
from htsql.core.tr.coerce import coerce
from htsql.core.tr.signature import CompareSig
from .signature import RowNumSig
from htsql.core.tr.compile import CompileOrdered, CompileCovering
from htsql.core.tr.stitch import arrange, spread


class OracleCompileOrdered(CompileOrdered):

    def __call__(self):
        if self.space.limit is None and self.space.offset is None:
            return super(OracleCompileOrdered, self).__call__()
        left_limit = None
        if self.space.offset is not None:
            left_limit = self.space.offset+1
        right_limit = None
        if self.space.limit is not None:
            if self.space.offset is not None:
                right_limit = self.space.limit+self.space.offset+1
            else:
                right_limit = self.space.limit+1
        kid = self.state.compile(self.space.base,
                                 baseline=self.state.root)
        order = arrange(self.space)
        kid = self.state.inject(kid, [code for code, direction in order])
        routes = kid.routes.copy()
        for unit in spread(self.space):
            routes[unit] = routes[unit.clone(space=self.backbone)]
        kid = OrderTerm(self.state.tag(), kid, order, None, None,
                        self.space, kid.baseline, routes)
        kid = PermanentTerm(self.state.tag(), kid,
                            kid.space, kid.baseline, kid.routes.copy())
        row_num_code = FormulaCode(RowNumSig(), coerce(IntegerDomain()),
                                   self.space.flow)
        if right_limit is not None:
            right_limit_code = LiteralCode(right_limit,
                                           coerce(IntegerDomain()),
                                           self.space.flow)
            right_filter = FormulaCode(CompareSig('<'),
                                       coerce(BooleanDomain()),
                                       self.space.flow,
                                       lop=row_num_code,
                                       rop=right_limit_code)
            kid = FilterTerm(self.state.tag(), kid, right_filter,
                             kid.space, kid.baseline, kid.routes.copy())
        else:
            kid = WrapperTerm(self.state.tag(), kid,
                              kid.space, kid.baseline, kid.routes.copy())
        routes = kid.routes.copy()
        if left_limit is not None:
            row_num_unit = ScalarUnit(row_num_code, self.space.base,
                                      self.space.flow)
            routes[row_num_unit] = kid.tag
        kid = PermanentTerm(self.state.tag(), kid,
                            kid.space, kid.baseline, routes)
        if left_limit is not None:
            left_limit_code = LiteralCode(left_limit,
                                          coerce(IntegerDomain()),
                                          self.space.flow)
            left_filter = FormulaCode(CompareSig('>='),
                                      coerce(BooleanDomain()),
                                      self.space.flow,
                                      lop=row_num_unit,
                                      rop=left_limit_code)
            kid = FilterTerm(self.state.tag(), kid, left_filter,
                             kid.space, kid.baseline, kid.routes.copy())
        return kid


class OracleCompileCovering(CompileCovering):

    def clip_root(self, term, order):
        left_bound = 1
        if self.space.offset is not None:
            left_bound = self.space.offset+1
        right_bound = left_bound+1
        if self.space.limit is not None:
            right_bound = left_bound+self.space.limit
        term = OrderTerm(self.state.tag(), term, order, None, None,
                        term.space, term.baseline, term.routes.copy())
        term = PermanentTerm(self.state.tag(), term,
                             term.space, term.baseline, term.routes.copy())
        row_num_code = FormulaCode(RowNumSig(), coerce(IntegerDomain()),
                                   self.space.flow)
        right_bound_code = LiteralCode(right_bound, coerce(IntegerDomain()),
                                       self.space.flow)
        right_filter = FormulaCode(CompareSig('<'), coerce(BooleanDomain()),
                                   self.space.flow,
                                   lop=row_num_code, rop=right_bound_code)
        term = FilterTerm(self.state.tag(), term, right_filter,
                          term.space, term.baseline, term.routes.copy())
        routes = term.routes.copy()
        row_num_unit = ScalarUnit(row_num_code, self.space, self.space.flow)
        routes[row_num_unit] = term.tag
        term = PermanentTerm(self.state.tag(), term,
                             term.space, term.baseline, routes)
        left_bound_code = LiteralCode(left_bound, coerce(IntegerDomain()),
                                      self.space.flow)
        left_filter = FormulaCode(CompareSig('>='), coerce(BooleanDomain()),
                                  self.space.flow,
                                  lop=row_num_unit, rop=left_bound_code)
        term = FilterTerm(self.state.tag(), term, left_filter,
                          term.space, term.baseline, term.routes.copy())
        return term


