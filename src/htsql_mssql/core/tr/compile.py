#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from htsql.core.domain import BooleanDomain, IntegerDomain
from htsql.core.tr.term import PermanentTerm, FilterTerm
from htsql.core.tr.space import LiteralCode, FormulaCode, ScalarUnit
from htsql.core.tr.coerce import coerce
from htsql.core.tr.signature import (CompareSig, AndSig, SortDirectionSig,
                                     RowNumberSig)
from htsql.core.tr.compile import CompileOrdered, CompileCovering
from htsql.core.tr.stitch import arrange, spread


class MSSQLCompileOrdered(CompileOrdered):

    def __call__(self):
        if self.space.offset is None:
            return super(MSSQLCompileOrdered, self).__call__()
        kid = self.state.compile(self.space.base,
                                 baseline=self.state.root)
        order = arrange(self.space)
        kid = self.state.inject(kid, [code for code, direction in order])
        ops = []
        for code, direction in order:
            op = FormulaCode(SortDirectionSig(direction=direction),
                             code.domain, code.flow, base=code)
            ops.append(op)
        row_number_code = FormulaCode(RowNumberSig(), coerce(IntegerDomain()),
                                      self.space.flow,
                                      partition=[], order=ops)
        row_number_unit = ScalarUnit(row_number_code, self.space.base,
                                     self.space.flow)
        tag = self.state.tag()
        routes = kid.routes.copy()
        routes[row_number_unit] = tag
        kid = PermanentTerm(tag, kid, kid.space, kid.baseline, routes)
        left_limit = self.space.offset+1
        right_limit = None
        if self.space.limit is not None:
            right_limit = self.space.limit+self.space.offset+1
        left_limit_code = LiteralCode(left_limit, coerce(IntegerDomain()),
                                      self.space.flow)
        right_limit_code = None
        if right_limit is not None:
            right_limit_code = LiteralCode(right_limit, coerce(IntegerDomain()),
                                           self.space.flow)
        left_filter = FormulaCode(CompareSig('>='), coerce(BooleanDomain()),
                                  self.space.flow,
                                  lop=row_number_unit, rop=left_limit_code)
        right_filter = None
        if right_limit_code is not None:
            right_filter = FormulaCode(CompareSig('<'),
                                       coerce(BooleanDomain()),
                                       self.space.flow,
                                       lop=row_number_unit,
                                       rop=right_limit_code)
        filter = left_filter
        if right_filter is not None:
            filter = FormulaCode(AndSig(), coerce(BooleanDomain()),
                                 self.space.flow,
                                 ops=[left_filter, right_filter])
        routes = kid.routes.copy()
        for unit in spread(self.space):
            routes[unit] = routes[unit.clone(space=self.backbone)]
        return FilterTerm(self.state.tag(), kid, filter,
                          self.space, kid.baseline, routes)


class MSSQLCompileCovering(CompileCovering):

    def clip_root(self, term, order):
        if self.space.offset is not None:
            return self.clip(term, order, [])
        return super(MSSQLCompileCovering, self).clip_root(term, order)


