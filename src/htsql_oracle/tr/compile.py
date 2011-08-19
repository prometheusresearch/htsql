#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.domain import BooleanDomain, IntegerDomain
from htsql.tr.term import PermanentTerm, FilterTerm, OrderTerm
from htsql.tr.flow import LiteralCode, FormulaCode, ScalarUnit
from htsql.tr.coerce import coerce
from htsql.tr.signature import CompareSig
from .signature import RowNumSig
from htsql.tr.compile import CompileOrdered
from htsql.tr.stitch import arrange, spread


class OracleCompileOrdered(CompileOrdered):

    def __call__(self):
        if self.flow.limit is None and self.flow.offset is None:
            return super(OracleCompileOrdered, self).__call__()
        left_limit = None
        if self.flow.offset is not None:
            left_limit = self.flow.offset+1
        right_limit = None
        if self.flow.limit is not None:
            if self.flow.offset is not None:
                right_limit = self.flow.limit+self.flow.offset+1
            else:
                right_limit = self.flow.limit+1
        kid = self.state.compile(self.flow.base,
                                 baseline=self.state.root)
        order = arrange(self.flow)
        codes = [code for code, direction in order]
        kid = self.state.inject(kid, [code for code, direction in order])
        routes = kid.routes.copy()
        for unit in spread(self.flow):
            routes[unit] = routes[unit.clone(flow=self.backbone)]
        kid = OrderTerm(self.state.tag(), kid, order, None, None,
                        self.flow, kid.baseline, routes)
        kid = PermanentTerm(self.state.tag(), kid,
                            kid.flow, kid.baseline, kid.routes.copy())
        row_num_code = FormulaCode(RowNumSig(), coerce(IntegerDomain()),
                                   self.flow.binding)
        if right_limit is not None:
            right_limit_code = LiteralCode(right_limit,
                                           coerce(IntegerDomain()),
                                           self.flow.binding)
            right_filter = FormulaCode(CompareSig('<'),
                                       coerce(BooleanDomain()),
                                       self.flow.binding,
                                       lop=row_num_code,
                                       rop=right_limit_code)
            kid = FilterTerm(self.state.tag(), kid, right_filter,
                             kid.flow, kid.baseline, kid.routes.copy())
        else:
            kid = WrapperTerm(self.state.tag(), kid,
                              kid.flow, kid.baseline, kid.routes.copy())
        routes = kid.routes.copy()
        if left_limit is not None:
            row_num_unit = ScalarUnit(row_num_code, self.flow.base,
                                      self.flow.binding)
            routes[row_num_unit] = kid.tag
        kid = PermanentTerm(self.state.tag(), kid,
                            kid.flow, kid.baseline, routes)
        if left_limit is not None:
            left_limit_code = LiteralCode(left_limit,
                                          coerce(IntegerDomain()),
                                          self.flow.binding)
            left_filter = FormulaCode(CompareSig('>='),
                                      coerce(BooleanDomain()),
                                      self.flow.binding,
                                      lop=row_num_unit,
                                      rop=left_limit_code)
            kid = FilterTerm(self.state.tag(), kid, left_filter,
                             kid.flow, kid.baseline, kid.routes.copy())
        return kid


