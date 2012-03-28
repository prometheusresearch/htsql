#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from htsql.core.domain import BooleanDomain, IntegerDomain
from htsql.core.tr.term import (WrapperTerm, FilterTerm, OrderTerm,
                                PermanentTerm, JoinTerm, ScalarTerm)
from htsql.core.tr.flow import (LiteralCode, FormulaCode, ScalarUnit,
                                CorrelationCode)
from htsql.core.tr.coerce import coerce
from htsql.core.tr.signature import IsEqualSig, AndSig, CompareSig
from htsql.core.tr.fn.signature import AddSig
from htsql.core.tr.compile import CompileCovering
from .signature import (UserVariableSig, UserVariableAssignmentSig,
                        NoOpConditionSig, IfSig)


class MySQLCompileCovering(CompileCovering):

    def clip(self, term, order, partition):
        prefix = u"!htsql:%s" % term.tag
        row_number = FormulaCode(UserVariableSig(u"%s:row_number" % prefix),
                                 coerce(IntegerDomain()), self.flow.binding)
        keys = []
        for idx, code in enumerate(partition):
            key = FormulaCode(UserVariableSig(u"%s:partition:%s"
                                              % (prefix, idx+1)),
                                              code.domain, self.flow.binding)
            keys.append(key)
        #term = PermanentTerm(self.state.tag(), term,
        #                     term.flow, term.baseline, term.routes.copy())
        zero_term = ScalarTerm(self.state.tag(),
                               self.state.root, self.state.root, {})
        zero_units = []
        code = FormulaCode(UserVariableAssignmentSig(),
                           row_number.domain, self.flow.binding,
                           lop=row_number,
                           rop=LiteralCode(None, row_number.domain,
                                           self.flow.binding))
        unit = ScalarUnit(code, self.state.root, code.binding)
        zero_units.append(unit)
        for key in keys:
            code = FormulaCode(UserVariableAssignmentSig(),
                               key.domain, self.flow.binding,
                               lop=key,
                               rop=LiteralCode(None, key.domain,
                                               self.flow.binding))
            unit = ScalarUnit(code, self.state.root, code.binding)
            zero_units.append(unit)
        tag = self.state.tag()
        routes = {}
        for unit in zero_units:
            routes[unit] = tag
        zero_term.routes = routes
        zero_term = PermanentTerm(tag, zero_term,
                                  zero_term.flow, zero_term.baseline, routes)
        filters = [FormulaCode(NoOpConditionSig(), coerce(BooleanDomain()),
                   self.flow.binding, op=unit) for unit in zero_units]
        filter = FormulaCode(AndSig(), coerce(BooleanDomain()),
                             self.flow.binding, ops=filters)
        zero_term = FilterTerm(self.state.tag(), zero_term, filter,
                               zero_term.flow, zero_term.baseline, {})
        term = JoinTerm(self.state.tag(), term, zero_term, [],
                        False, False, term.flow, term.baseline,
                        term.routes.copy())
        order = [(code, +1) for code in partition]+order
        term = OrderTerm(self.state.tag(), term, order, None, None,
                         term.flow, term.baseline, term.routes.copy())
        term = PermanentTerm(self.state.tag(), term,
                             term.flow, term.baseline, term.routes)
        next_units = []
        conditions = []
        for lop, rop in zip(keys, partition):
            condition = FormulaCode(IsEqualSig(+1), coerce(BooleanDomain()),
                                    self.flow.binding, lop=lop, rop=rop)
            conditions.append(condition)
        if len(conditions) == 1:
            [condition] = conditions
        else:
            condition = FormulaCode(AndSig(), coerce(BooleanDomain()),
                                    self.flow.binding, ops=conditions)
        one_literal = LiteralCode(1, coerce(IntegerDomain()),
                                  self.flow.binding)
        on_true = FormulaCode(AddSig(), coerce(IntegerDomain()),
                              self.flow.binding,
                              lop=row_number, rop=one_literal)
        on_false = one_literal
        value = FormulaCode(IfSig(), row_number.domain, self.flow.binding,
                            condition=condition, on_true=on_true,
                            on_false=on_false)
        code = FormulaCode(UserVariableAssignmentSig(),
                           row_number.domain, self.flow.binding,
                           lop=row_number, rop=value)
        row_number_unit = ScalarUnit(code, term.flow, code.binding)
        next_units.append(row_number_unit)
        for lop, rop in zip(keys, partition):
            code = FormulaCode(UserVariableAssignmentSig(), lop.domain,
                                    self.flow.binding, lop=lop, rop=rop)
            unit = ScalarUnit(code, term.flow, code.binding)
            next_units.append(unit)
        tag = self.state.tag()
        routes = term.routes.copy()
        for unit in next_units:
            routes[unit] = tag
        term = PermanentTerm(tag, term, term.flow, term.baseline, routes)
        left_bound = 1
        if self.flow.offset is not None:
            left_bound = self.flow.offset+1
        right_bound = left_bound+1
        if self.flow.limit is not None:
            right_bound = left_bound+self.flow.limit
        left_bound_code = LiteralCode(left_bound, coerce(IntegerDomain()),
                                      term.flow.binding)
        right_bound_code = LiteralCode(right_bound, coerce(IntegerDomain()),
                                       term.flow.binding)
        left_filter = FormulaCode(CompareSig('>='), coerce(BooleanDomain()),
                                  term.flow.binding,
                                  lop=row_number_unit, rop=left_bound_code)
        right_filter = FormulaCode(CompareSig('<'), coerce(BooleanDomain()),
                                   term.flow.binding,
                                   lop=row_number_unit, rop=right_bound_code)
        filters = [left_filter, right_filter]
        filters += [FormulaCode(NoOpConditionSig(), coerce(BooleanDomain()),
                    self.flow.binding, op=unit) for unit in next_units]
        filter = FormulaCode(AndSig(), coerce(BooleanDomain()),
                             self.flow.binding, ops=filters)
        term = FilterTerm(self.state.tag(), term, filter,
                          term.flow, term.baseline, term.routes)
        return term


