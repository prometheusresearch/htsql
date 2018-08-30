#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from htsql.core.domain import BooleanDomain, IntegerDomain
from htsql.core.tr.term import (WrapperTerm, FilterTerm, OrderTerm,
        PermanentTerm, JoinTerm, ScalarTerm)
from htsql.core.tr.space import (LiteralCode, FormulaCode, ScalarUnit,
        CorrelationCode)
from htsql.core.tr.coerce import coerce
from htsql.core.tr.signature import IsEqualSig, AndSig, CompareSig
from htsql.core.tr.fn.signature import AddSig
from htsql.core.tr.compile import CompileCovering
from .signature import (UserVariableSig, UserVariableAssignmentSig,
        NoOpConditionSig, IfSig)


class MySQLCompileCovering(CompileCovering):

    def clip(self, term, order, partition):
        prefix = "!htsql:%s" % term.tag
        row_number = FormulaCode(UserVariableSig("%s:row_number" % prefix),
                                 coerce(IntegerDomain()), self.space.flow)
        keys = []
        for idx, code in enumerate(partition):
            key = FormulaCode(UserVariableSig("%s:partition:%s"
                                              % (prefix, idx+1)),
                                              code.domain, self.space.flow)
            keys.append(key)
        #term = PermanentTerm(self.state.tag(), term,
        #                     term.space, term.baseline, term.routes.copy())
        zero_term = ScalarTerm(self.state.tag(),
                               self.state.root, self.state.root, {})
        zero_units = []
        code = FormulaCode(UserVariableAssignmentSig(),
                           row_number.domain, self.space.flow,
                           lop=row_number,
                           rop=LiteralCode(None, row_number.domain,
                                           self.space.flow))
        unit = ScalarUnit(code, self.state.root, code.flow)
        zero_units.append(unit)
        for key in keys:
            code = FormulaCode(UserVariableAssignmentSig(),
                               key.domain, self.space.flow,
                               lop=key,
                               rop=LiteralCode(None, key.domain,
                                               self.space.flow))
            unit = ScalarUnit(code, self.state.root, code.flow)
            zero_units.append(unit)
        tag = self.state.tag()
        routes = {}
        for unit in zero_units:
            routes[unit] = tag
        zero_term.routes = routes
        zero_term = PermanentTerm(tag, zero_term,
                                  zero_term.space, zero_term.baseline, routes)
        filters = [FormulaCode(NoOpConditionSig(), coerce(BooleanDomain()),
                   self.space.flow, op=unit) for unit in zero_units]
        filter = FormulaCode(AndSig(), coerce(BooleanDomain()),
                             self.space.flow, ops=filters)
        zero_term = FilterTerm(self.state.tag(), zero_term, filter,
                               zero_term.space, zero_term.baseline, {})
        term = JoinTerm(self.state.tag(), term, zero_term, [],
                        False, False, term.space, term.baseline,
                        term.routes.copy())
        order = [(code, +1) for code in partition]+order
        term = OrderTerm(self.state.tag(), term, order, None, None,
                         term.space, term.baseline, term.routes.copy())
        term = PermanentTerm(self.state.tag(), term,
                             term.space, term.baseline, term.routes)
        next_units = []
        conditions = []
        for lop, rop in zip(keys, partition):
            condition = FormulaCode(IsEqualSig(+1), coerce(BooleanDomain()),
                                    self.space.flow, lop=lop, rop=rop)
            conditions.append(condition)
        if len(conditions) == 1:
            [condition] = conditions
        else:
            condition = FormulaCode(AndSig(), coerce(BooleanDomain()),
                                    self.space.flow, ops=conditions)
        one_literal = LiteralCode(1, coerce(IntegerDomain()),
                                  self.space.flow)
        on_true = FormulaCode(AddSig(), coerce(IntegerDomain()),
                              self.space.flow,
                              lop=row_number, rop=one_literal)
        on_false = one_literal
        value = FormulaCode(IfSig(), row_number.domain, self.space.flow,
                            condition=condition, on_true=on_true,
                            on_false=on_false)
        code = FormulaCode(UserVariableAssignmentSig(),
                           row_number.domain, self.space.flow,
                           lop=row_number, rop=value)
        row_number_unit = ScalarUnit(code, term.space, code.flow)
        next_units.append(row_number_unit)
        for lop, rop in zip(keys, partition):
            code = FormulaCode(UserVariableAssignmentSig(), lop.domain,
                                    self.space.flow, lop=lop, rop=rop)
            unit = ScalarUnit(code, term.space, code.flow)
            next_units.append(unit)
        tag = self.state.tag()
        routes = term.routes.copy()
        for unit in next_units:
            routes[unit] = tag
        term = PermanentTerm(tag, term, term.space, term.baseline, routes)
        left_bound = 1
        if self.space.offset is not None:
            left_bound = self.space.offset+1
        right_bound = left_bound+1
        if self.space.limit is not None:
            right_bound = left_bound+self.space.limit
        left_bound_code = LiteralCode(left_bound, coerce(IntegerDomain()),
                                      term.space.flow)
        right_bound_code = LiteralCode(right_bound, coerce(IntegerDomain()),
                                       term.space.flow)
        left_filter = FormulaCode(CompareSig('>='), coerce(BooleanDomain()),
                                  term.space.flow,
                                  lop=row_number_unit, rop=left_bound_code)
        right_filter = FormulaCode(CompareSig('<'), coerce(BooleanDomain()),
                                   term.space.flow,
                                   lop=row_number_unit, rop=right_bound_code)
        filters = [left_filter, right_filter]
        filters += [FormulaCode(NoOpConditionSig(), coerce(BooleanDomain()),
                    self.space.flow, op=unit) for unit in next_units]
        filter = FormulaCode(AndSig(), coerce(BooleanDomain()),
                             self.space.flow, ops=filters)
        term = FilterTerm(self.state.tag(), term, filter,
                          term.space, term.baseline, term.routes)
        return term


