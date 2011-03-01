#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


from htsql.domain import BooleanDomain, IntegerDomain
from htsql.tr.term import PermanentTerm, FilterTerm, OrderTerm
from htsql.tr.code import LiteralCode, FormulaCode, ScalarUnit
from htsql.tr.coerce import coerce
from htsql.tr.signature import CompareSig, AndSig
from htsql.tr.fn.signature import SortDirectionSig
from .signature import RowNumberSig
from htsql.tr.compile import CompileOrdered, ordering, spread


class MSSQLCompileOrdered(CompileOrdered):

    def __call__(self):
        if self.space.offset is None:
            return super(MSSQLCompileOrdered, self).__call__()
        kid = self.state.compile(self.space.base,
                                  baseline=self.state.scalar,
                                  mask=self.state.scalar)
        order = ordering(self.space)
        codes = [code for code, direction in order]
        kid = self.state.inject(kid, [code for code, direction in order])
        ops = []
        for code, direction in order:
            op = FormulaCode(SortDirectionSig(direction=direction),
                             code.domain, code.binding, base=code)
            ops.append(op)
        row_number_code = FormulaCode(RowNumberSig(), coerce(IntegerDomain()),
                                      self.space.binding, ops=ops)
        row_number_unit = ScalarUnit(row_number_code, self.space.base,
                                     self.space.binding)
        tag = self.state.tag()
        routes = kid.routes.copy()
        routes[row_number_unit] = tag
        kid = PermanentTerm(tag, kid, kid.space, kid.baseline, routes)
        left_limit = self.space.offset+1
        right_limit = None
        if self.space.limit is not None:
            right_limit = self.space.limit+self.space.offset+1
        left_limit_code = LiteralCode(left_limit, coerce(IntegerDomain()),
                                      self.space.binding)
        right_limit_code = None
        if right_limit is not None:
            right_limit_code = LiteralCode(right_limit, coerce(IntegerDomain()),
                                           self.space.binding)
        left_filter = FormulaCode(CompareSig('>='), coerce(BooleanDomain()),
                                  self.space.binding,
                                  lop=row_number_unit, rop=left_limit_code)
        right_filter = None
        if right_limit_code is not None:
            right_filter = FormulaCode(CompareSig('<'),
                                       coerce(BooleanDomain()),
                                       self.space.binding,
                                       lop=row_number_unit,
                                       rop=right_limit_code)
        filter = left_filter
        if right_filter is not None:
            filter = FormulaCode(AndSig(), coerce(BooleanDomain()),
                                 self.space.binding,
                                 ops=[left_filter, right_filter])
        routes = kid.routes.copy()
        for unit in spread(self.space):
            routes[unit.clone(space=self.space)] = routes[unit]
        return FilterTerm(self.state.tag(), kid, filter,
                          self.space, kid.baseline, routes)



