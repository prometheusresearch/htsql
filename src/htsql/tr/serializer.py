#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.serializer`
==========================

This module implements the SQL serializer.
"""


from ..adapter import Adapter, Utility, adapts, find_adapters
from ..error import InvalidArgumentError
from ..domain import (Domain, BooleanDomain, NumberDomain, IntegerDomain,
                      DecimalDomain, FloatDomain, StringDomain, DateDomain)
from .frame import (Clause, Frame, LeafFrame, ScalarFrame,
                    BranchFrame, CorrelatedFrame, SegmentFrame,
                    QueryFrame, Phrase, EqualityPhrase, InequalityPhrase,
                    TotalEqualityPhrase, TotalInequalityPhrase,
                    ConjunctionPhrase, DisjunctionPhrase, NegationPhrase,
                    CastPhrase, LiteralPhrase, LeafReferencePhrase,
                    BranchReferencePhrase, CorrelatedFramePhrase, TuplePhrase)
from .plan import Plan
import decimal


class Serializer(object):

    def __init__(self):
        self.format = Format()
        self.alias_by_clause = {}

    def add_alias(self, clause, alias):
        self.alias_by_clause[clause] = alias

    def get_alias(self, clause):
        return self.alias_by_clause[clause]

    def clear_aliases(self):
        self.alias_by_clause = {}

    def serialize(self, clause):
        serialize = Serialize(clause, self)
        return serialize.serialize()

    def serialize_aliases(self, frame, parents=[]):
        serialize = Serialize(frame, self)
        return serialize.serialize_aliases(parents)

    def call(self, clause):
        serialize = Serialize(clause, self)
        return serialize.call()


class Format(Utility):

    def name(self, name):
        return "\"%s\"" % name.replace("\"", "\"\"")

    def alias(self, name, index=None):
        if index is None:
            return name
        return "%s_%s" % (name, index)

    def attr(self, parent, child):
        return "%s.%s" % (parent, child)

    def as_op(self, value, alias):
        return "%s AS %s" % (value, alias)

    def parens(self, value):
        return "(%s)" % value

    def list(self, values):
        return ", ".join(values)

    def null(self):
        return "NULL"

    def true(self):
        return "TRUE"

    def false(self):
        return "FALSE"

    def number(self, value):
        if isinstance(value, (int, float)):
            return repr(value)
        if isinstance(value, (long, decimal.Decimal)):
            return str(value)

    def integer(self, value):
        return str(value)

    def decimal(self, value):
        return str(value)

    def float(self, value):
        return repr(value)

    def string(self, value):
        return "'%s'" % value.replace("'", "''")

    def date(self, value):
        return "CAST('%s' AS DATE)" % value

    def or_op(self, values):
        if not values:
            return self.false()
        return "(%s)" % " OR ".join(values)

    def and_op(self, values):
        if not values:
            return self.true()
        return "(%s)" % " AND ".join(values)

    def not_op(self, value):
        return "(NOT %s)" % value

    def unary_op(self, op, right):
        return "(%s %s)" % (op, right)

    def binary_op(self, left, op, right):
        return "(%s %s %s)" % (left, op, right)

    def equal_op(self, left, right, is_negative=False):
        op = "="
        if is_negative:
            op = "!="
        return self.binary_op(left, op, right)

    def total_equal_op(self, left, right, is_negative=False):
        op = "IS NOT DISTINCT FROM"
        if is_negative:
            op = "IS DISTINCT FROM"
        return self.binary_op(left, op, right)

    def to_boolean(self, value):
        return "(%s IS NOT NULL)" % value

    def to_boolean_from_string(self, value):
        return "(NULLIF(%s, '') IS NOT NULL)" % value

    def to_integer(self, value):
        return "CAST(%s AS INTEGER)" % value

    def to_decimal(self, value):
        return "CAST(%s AS NUMERIC)" % value

    def to_float(self, value):
        return "CAST(%s AS FLOAT)" % value

    def to_string(self, value):
        return "CAST(%s AS TEXT)" % value

    def to_date(self, value):
        return "CAST(%s AS DATE)" % value

    def is_null(self, arg):
        return "(%s IS NULL)" % arg

    def is_not_null(self, arg):
        return "(%s IS NOT NULL)" % arg

    def order(self, value, dir):
        if dir == +1:
            op = "ASC"
        elif dir == -1:
            op = "DESC"
        return "%s %s" % (value, op)

    def join(self, base=None, target=None,
             condition=None, is_inner=True):
        assert target is not None
        if base is None:
            assert condition is None and is_inner
            return target
        if condition is None:
            if is_inner:
                return "%s CROSS JOIN %s" % (base, target)
            else:
                condition = self.true()
        if is_inner:
            op = "INNER JOIN"
        else:
            op = "LEFT OUTER JOIN"
        return "%s %s %s ON (%s)" % (base, op, target, condition)

    def select(self, select_clause, from_clause=None,
               where_clause=None, group_clause=None,
               having_clause=None, order_clause=None,
               limit=None, offset=None):
        clauses = []
        clauses.append("SELECT %s" % select_clause)
        if from_clause is not None:
            clauses.append(" FROM %s" % from_clause)
        if where_clause is not None:
            clauses.append(" WHERE %s" % where_clause)
        if group_clause is not None:
            clauses.append(" GROUP BY %s" % group_clause)
        if having_clause is not None:
            clauses.append(" HAVING %s" % having_clause)
        if order_clause is not None:
            clauses.append(" ORDER BY %s" % order_clause)
        if limit is not None:
            clauses.append(" LIMIT %s" % limit)
        if offset is not None:
            clauses.append(" OFFSET %s" % offset)
        return "".join(clauses)

    def scalar_select(self):
        return "(SELECT 1)"


class Serialize(Adapter):

    adapts(Clause, Serializer)

    def serialize(self):
        raise NotImplementedError()

    def call(self):
        return "!"


class SerializeFrame(Serialize):

    adapts(Frame, Serializer)

    def __init__(self, frame, serializer):
        self.frame = frame
        self.serializer = serializer
        self.format = serializer.format

    def serialize_aliases(self, parents):
        pass


class SerializeLeaf(SerializeFrame):

    adapts(LeafFrame, Serializer)

    def serialize(self):
        parent = self.format.name(self.frame.table.schema_name)
        child = self.format.name(self.frame.table.name)
        return self.format.attr(parent, child)

    def call(self):
        return self.frame.table.name


class SerializeScalar(SerializeFrame):

    adapts(ScalarFrame, Serializer)

    def serialize(self):
        return self.format.scalar_select()


class SerializeBranch(SerializeFrame):

    adapts(BranchFrame, Serializer)

    with_aliases = True

    def serialize(self):
        select = self.serialize_select()
        return self.format.parens(select)

    def serialize_select(self):
        select_clause = self.serialize_select_clause()
        from_clause = self.serialize_from_clause()
        where_clause = self.serialize_where_clause()
        group_clause = self.serialize_group_clause()
        having_clause = self.serialize_having_clause()
        order_clause = self.serialize_order_clause()
        limit = self.frame.limit
        offset = self.frame.offset
        select = self.format.select(select_clause=select_clause,
                                    from_clause=from_clause,
                                    where_clause=where_clause,
                                    group_clause=group_clause,
                                    having_clause=having_clause,
                                    order_clause=order_clause,
                                    limit=limit, offset=offset)
        return select

    def serialize_select_clause(self):
        select_clause = []
        for phrase in self.frame.select:
            value = self.serializer.serialize(phrase)
            if self.with_aliases:
                alias = self.serializer.get_alias(phrase)
                inherited_alias = None
                if isinstance(phrase, LeafReferencePhrase):
                    inherited_alias = phrase.column.name
                elif isinstance(phrase, BranchReferencePhrase):
                    inherited_alias = self.serializer.get_alias(phrase.phrase)
                if alias != inherited_alias:
                    alias = self.format.name(alias)
                    value = self.format.as_op(value, alias)
            select_clause.append(value)
        return self.format.list(select_clause)

    def serialize_from_clause(self):
        from_clause = None
        for link in self.frame.linkage:
            target = self.serializer.serialize(link.frame)
            alias = self.serializer.get_alias(link.frame)
            alias = self.format.name(alias)
            target = self.format.as_op(target, alias)
            condition = None
            if link.condition is not None:
                condition = self.serializer.serialize(link.condition)
            from_clause = self.format.join(from_clause, target,
                                           condition, link.is_inner)
        return from_clause

    def serialize_where_clause(self):
        where_clause = None
        if self.frame.filter is not None:
            where_clause = self.serializer.serialize(self.frame.filter)
        return where_clause

    def serialize_group_clause(self):
        if not self.frame.group:
            return None
        group_clause = []
        position_by_phrase = {}
        for idx, phrase in enumerate(self.frame.select):
            position_by_phrase[phrase] = idx+1
        for phrase in self.frame.group:
            if phrase in position_by_phrase:
                position = position_by_phrase[phrase]
                value = self.format.number(position)
            else:
                value = self.serializer.serialize(phrase)
            group_clause.append(value)
        return self.format.list(group_clause)

    def serialize_having_clause(self):
        having_clause = None
        if self.frame.group_filter is not None:
            having_clause = self.serializer.serialize(self.frame.group_filter)
        return having_clause

    def serialize_order_clause(self):
        if not self.frame.order:
            return None
        order_clause = []
        position_by_phrase = {}
        for idx, phrase in enumerate(self.frame.select):
            position_by_phrase[phrase] = idx+1
        for phrase, dir in self.frame.order:
            if phrase in position_by_phrase:
                position = position_by_phrase[phrase]
                value = self.format.number(position)
            else:
                value = self.serializer.serialize(phrase)
            value = self.format.order(value, dir)
            order_clause.append(value)
        return self.format.list(order_clause)

    def serialize_aliases(self, parents):
        taken_aliases = set()
        for parent in parents:
            for link in parent.linkage:
                alias = self.serializer.get_alias(link.frame)
                taken_aliases.add(alias)
        self.serialize_alias_collection(self.frame.select, taken_aliases)
        collection = []
        for link in self.frame.linkage:
            self.serializer.serialize_aliases(link.frame)
            collection.append(link.frame)
        self.serialize_alias_collection(collection, taken_aliases)

    def serialize_alias_collection(self, collection, taken_aliases):
        clauses_by_name = {}
        for clause in collection:
            if clause in self.serializer.alias_by_clause:
                continue
            name = self.serializer.call(clause)
            clauses_by_name.setdefault(name, []).append(clause)
        for name in sorted(clauses_by_name):
            clauses = clauses_by_name[name]
            for clause in clauses:
                index = None
                if len(clauses) > 1:
                    index = 1
                alias = self.format.alias(name, index)
                while alias in taken_aliases:
                    if index is None:
                        index = 1
                    index += 1
                    alias = self.format.alias(name, index)
                self.serializer.add_alias(clause, alias)
                taken_aliases.add(alias)

    def call(self):
        if self.frame.linkage:
            child = None
            for link in self.frame.linkage:
                if link.is_inner:
                    child = link.frame
            return self.serializer.call(child)
        return super(SerializeBranch, self).call()


class SerializeCorrelated(SerializeFrame):

    adapts(CorrelatedFrame, Serializer)

    with_aliases = False


class SerializeSegment(SerializeFrame):

    adapts(SegmentFrame, Serializer)

    with_aliases = False

    def serialize(self):
        self.serializer.serialize_aliases(self.frame)
        select = self.serialize_select()
        self.serializer.clear_aliases()
        return select


class SerializeQuery(SerializeFrame):

    adapts(QueryFrame, Serializer)

    def serialize(self):
        sql = None
        if self.frame.segment is not None:
            sql = self.serializer.serialize(self.frame.segment)
        return Plan(self.frame, sql, self.frame.mark)


class SerializePhrase(Serialize):

    adapts(Phrase, Serializer)

    def __init__(self, phrase, serializer):
        self.phrase = phrase
        self.serializer = serializer
        self.format = serializer.format


class SerializeEquality(SerializePhrase):

    adapts(EqualityPhrase, Serializer)

    def serialize(self):
        left = self.serializer.serialize(self.phrase.left)
        right = self.serializer.serialize(self.phrase.right)
        return self.format.equal_op(left, right)


class SerializeInequality(SerializePhrase):

    adapts(InequalityPhrase, Serializer)

    def serialize(self):
        left = self.serializer.serialize(self.phrase.left)
        right = self.serializer.serialize(self.phrase.right)
        return self.format.equal_op(left, right, is_negative=True)


class SerializeTotalEquality(SerializePhrase):

    adapts(TotalEqualityPhrase, Serializer)

    def serialize(self):
        left = self.serializer.serialize(self.phrase.left)
        right = self.serializer.serialize(self.phrase.right)
        return self.format.total_equal_op(left, right)


class SerializeTotalInequality(SerializePhrase):

    adapts(TotalInequalityPhrase, Serializer)

    def serialize(self):
        left = self.serializer.serialize(self.phrase.left)
        right = self.serializer.serialize(self.phrase.right)
        return self.format.total_equal_op(left, right, is_negative=True)


class SerializeConjunction(SerializePhrase):

    adapts(ConjunctionPhrase, Serializer)

    def serialize(self):
        values = [self.serializer.serialize(term)
                  for term in self.phrase.terms]
        return self.format.and_op(values)


class SerializeDisjunction(SerializePhrase):

    adapts(DisjunctionPhrase, Serializer)

    def serialize(self):
        values = [self.serializer.serialize(term)
                  for term in self.phrase.terms]
        return self.format.or_op(values)


class SerializeNegation(SerializePhrase):

    adapts(NegationPhrase, Serializer)

    def serialize(self):
        value = self.serializer.serialize(self.phrase.term)
        return self.format.not_op(value)


class SerializeTuple(SerializePhrase):

    adapts(TuplePhrase, Serializer)

    def serialize(self):
        units = [self.serializer.serialize(unit)
                 for unit in self.phrase.units]
        conditions = [self.format.is_not_null(unit) for unit in units]
        return self.format.and_op(conditions)


class SerializeCast(SerializePhrase):

    adapts(CastPhrase, Serializer)

    def serialize(self):
        serialize_to = SerializeTo(self.phrase.domain,
                                   self.phrase.phrase.domain,
                                   self.serializer)
        return serialize_to.serialize(self.phrase.phrase)


class SerializeTo(Adapter):

    adapts(Domain, Domain, Serializer)

    def __init__(self, to_domain, from_domain, serializer):
        self.to_domain = to_domain
        self.from_domain = from_domain
        self.serializer = serializer
        self.format = serializer.format

    def serialize(self, phrase):
        raise InvalidArgumentError("unable to cast", phrase.mark)


class SerializeToBooleanFromString(SerializeTo):

    adapts(BooleanDomain, StringDomain, Serializer)

    def serialize(self, phrase):
        value = self.serializer.serialize(phrase)
        return self.format.to_boolean_from_string(value)


class SerializeToBooleanFromNumber(SerializeTo):

    adapts(BooleanDomain, NumberDomain, Serializer)

    def serialize(self, phrase):
        value = self.serializer.serialize(phrase)
        return self.format.to_boolean(value)


class SerializeToInteger(SerializeTo):

    adapts(IntegerDomain, Domain, Serializer)

    def serialize(self, phrase):
        value = self.serializer.serialize(phrase)
        return self.format.to_integer(value)


class SerializeToDecimal(SerializeTo):

    adapts(DecimalDomain, Domain, Serializer)

    def serialize(self, phrase):
        value = self.serializer.serialize(phrase)
        return self.format.to_decimal(value)


class SerializeToFloat(SerializeTo):

    adapts(FloatDomain, Domain, Serializer)

    def serialize(self, phrase):
        value = self.serializer.serialize(phrase)
        return self.format.to_float(value)


class SerializeToString(SerializeTo):

    adapts(StringDomain, Domain, Serializer)

    def serialize(self, phrase):
        value = self.serializer.serialize(phrase)
        return self.format.to_string(value)


class SerializeToDateFromString(SerializeTo):

    adapts(DateDomain, StringDomain, Serializer)

    def serialize(self, phrase):
        value = self.serializer.serialize(phrase)
        return self.format.to_date(value)


class SerializeLiteral(SerializePhrase):

    adapts(LiteralPhrase, Serializer)

    def serialize(self):
        serialize_constant = SerializeConstant(self.phrase.domain,
                                               self.serializer)
        return serialize_constant.serialize(self.phrase.value)


class SerializeConstant(Adapter):

    adapts(Domain, Serializer)

    def __init__(self, domain, serializer):
        self.domain = domain
        self.serializer = serializer
        self.format = serializer.format

    def serialize(self, value):
        raise NotImplementedError()


class SerializeBooleanConstant(SerializeConstant):

    adapts(BooleanDomain, Serializer)

    def serialize(self, value):
        if value is None:
            return self.format.null()
        if value is True:
            return self.format.true()
        if value is False:
            return self.format.false()


class SerializeNumberConstant(SerializeConstant):

    adapts(NumberDomain, Serializer)

    def serialize(self, value):
        if value is None:
            return self.format.null()
        return self.format.number(value)


class SerializeIntegerConstant(SerializeConstant):

    adapts(IntegerDomain, Serializer)

    def serialize(self, value):
        if value is None:
            return self.format.null()
        return self.format.integer(value)


class SerializeDecimalConstant(SerializeConstant):

    adapts(DecimalDomain, Serializer)

    def serialize(self, value):
        if value is None:
            return self.format.null()
        return self.format.decimal(value)


class SerializeFloatConstant(SerializeConstant):

    adapts(FloatDomain, Serializer)

    def serialize(self, value):
        if value is None:
            return self.format.null()
        return self.format.float(value)


class SerializeStringConstant(SerializeConstant):

    adapts(StringDomain, Serializer)

    def serialize(self, value):
        if value is None:
            return self.format.null()
        return self.format.string(value)


class SerializeDateConstant(SerializeConstant):

    adapts(DateDomain, Serializer)

    def serialize(self, value):
        if value is None:
            return self.format.null()
        return self.format.date(value)


class SerializeLeafReference(SerializePhrase):

    adapts(LeafReferencePhrase, Serializer)

    def serialize(self):
        parent = self.serializer.get_alias(self.phrase.frame)
        parent = self.format.name(parent)
        child = self.format.name(self.phrase.column.name)
        return self.format.attr(parent, child)

    def call(self):
        return self.phrase.column.name


class SerializeBranchReference(SerializePhrase):

    adapts(BranchReferencePhrase, Serializer)

    def serialize(self):
        parent = self.serializer.get_alias(self.phrase.frame)
        parent = self.format.name(parent)
        child = self.serializer.get_alias(self.phrase.phrase)
        child = self.format.name(child)
        return self.format.attr(parent, child)

    def call(self):
        return self.serializer.call(self.phrase.phrase)


class SerializeCorrelatedFrame(SerializePhrase):

    adapts(CorrelatedFramePhrase, Serializer)


serialize_adapters = find_adapters()


