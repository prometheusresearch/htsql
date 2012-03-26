#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from htsql.core.domain import BooleanDomain, IntegerDomain
from htsql.core.tr.term import PermanentTerm, FilterTerm
from htsql.core.tr.flow import LiteralCode, FormulaCode, ScalarUnit
from htsql.core.tr.coerce import coerce
from htsql.core.tr.signature import (CompareSig, AndSig, SortDirectionSig,
                                     RowNumberSig)
from htsql.core.tr.compile import CompileOrdered, CompileCovering
from htsql.core.tr.stitch import arrange, spread


class MSSQLCompileOrdered(CompileOrdered):

    def __call__(self):
        if self.flow.offset is None:
            return super(MSSQLCompileOrdered, self).__call__()
        kid = self.state.compile(self.flow.base,
                                 baseline=self.state.root)
        order = arrange(self.flow)
        kid = self.state.inject(kid, [code for code, direction in order])
        ops = []
        for code, direction in order:
            op = FormulaCode(SortDirectionSig(direction=direction),
                             code.domain, code.binding, base=code)
            ops.append(op)
        row_number_code = FormulaCode(RowNumberSig(), coerce(IntegerDomain()),
                                      self.flow.binding,
                                      partition=[], order=ops)
        row_number_unit = ScalarUnit(row_number_code, self.flow.base,
                                     self.flow.binding)
        tag = self.state.tag()
        routes = kid.routes.copy()
        routes[row_number_unit] = tag
        kid = PermanentTerm(tag, kid, kid.flow, kid.baseline, routes)
        left_limit = self.flow.offset+1
        right_limit = None
        if self.flow.limit is not None:
            right_limit = self.flow.limit+self.flow.offset+1
        left_limit_code = LiteralCode(left_limit, coerce(IntegerDomain()),
                                      self.flow.binding)
        right_limit_code = None
        if right_limit is not None:
            right_limit_code = LiteralCode(right_limit, coerce(IntegerDomain()),
                                           self.flow.binding)
        left_filter = FormulaCode(CompareSig('>='), coerce(BooleanDomain()),
                                  self.flow.binding,
                                  lop=row_number_unit, rop=left_limit_code)
        right_filter = None
        if right_limit_code is not None:
            right_filter = FormulaCode(CompareSig('<'),
                                       coerce(BooleanDomain()),
                                       self.flow.binding,
                                       lop=row_number_unit,
                                       rop=right_limit_code)
        filter = left_filter
        if right_filter is not None:
            filter = FormulaCode(AndSig(), coerce(BooleanDomain()),
                                 self.flow.binding,
                                 ops=[left_filter, right_filter])
        routes = kid.routes.copy()
        for unit in spread(self.flow):
            routes[unit] = routes[unit.clone(flow=self.backbone)]
        return FilterTerm(self.state.tag(), kid, filter,
                          self.flow, kid.baseline, routes)


class MSSQLCompileCovering(CompileCovering):

    def clip_root(self, term, order):
        if self.flow.offset is not None:
            return self.clip(term, order, [])
        return super(MSSQLCompileCovering, self).clip_root(term, order)


