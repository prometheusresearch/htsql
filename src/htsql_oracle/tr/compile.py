#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


from htsql.domain import BooleanDomain, IntegerDomain
from htsql.tr.term import PermanentTerm, FilterTerm, OrderTerm
from htsql.tr.code import LiteralCode, FormulaCode, ScalarUnit
from htsql.tr.coerce import coerce
from htsql.tr.signature import CompareSig
from .signature import RowNumSig
from htsql.tr.compile import CompileOrdered, ordering, spread


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
                                  baseline=self.state.scalar,
                                  mask=self.state.scalar)
        order = ordering(self.space)
        codes = [code for code, direction in order]
        kid = self.state.inject(kid, [code for code, direction in order])
        routes = kid.routes.copy()
        for unit in spread(self.space):
            routes[unit.clone(space=self.space)] = routes[unit]
        kid = OrderTerm(self.state.tag(), kid, order, None, None,
                        self.space, kid.baseline, routes)
        kid = PermanentTerm(self.state.tag(), kid,
                            kid.space, kid.baseline, kid.routes.copy())
        row_num_code = FormulaCode(RowNumSig(), coerce(IntegerDomain()),
                                   self.space.binding)
        if right_limit is not None:
            right_limit_code = LiteralCode(right_limit,
                                           coerce(IntegerDomain()),
                                           self.space.binding)
            right_filter = FormulaCode(CompareSig('<'),
                                       coerce(BooleanDomain()),
                                       self.space.binding,
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
                                      self.space.binding)
            routes[row_num_unit] = kid.tag
        kid = PermanentTerm(self.state.tag(), kid,
                            kid.space, kid.baseline, routes)
        if left_limit is not None:
            left_limit_code = LiteralCode(left_limit,
                                          coerce(IntegerDomain()),
                                          self.space.binding)
            left_filter = FormulaCode(CompareSig('>='),
                                      coerce(BooleanDomain()),
                                      self.space.binding,
                                      lop=row_num_unit,
                                      rop=left_limit_code)
            kid = FilterTerm(self.state.tag(), kid, left_filter,
                             kid.space, kid.baseline, kid.routes.copy())
        return kid


