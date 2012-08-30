#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ....core.adapter import Adapter, adapt
from ....core.error import BadRequestError
from ....core.connect import transaction, scramble, unscramble
from ....core.mark import EmptyMark
from ....core.domain import Domain, ListDomain, RecordDomain, BooleanDomain
from ....core.classify import normalize, classify, relabel
from ....core.model import HomeNode, TableNode, TableArc, ColumnArc
from ....core.cmd.act import Act, ProduceAction, produce
from ....core.cmd.retrieve import Product, RowStream
from ....core.tr.bind import BindingState, Select
from ....core.tr.syntax import VoidSyntax, IdentifierSyntax
from ....core.tr.binding import (VoidBinding, RootBinding, FormulaBinding,
        SieveBinding, AliasBinding, SegmentBinding, QueryBinding,
        FreeTableRecipe, ColumnRecipe)
from ....core.tr.signature import IsEqualSig, AndSig, PlaceholderSig
from ....core.tr.decorate import decorate
from ....core.tr.coerce import coerce
from ....core.tr.encode import encode
from ....core.tr.flow import OrderedFlow
from ....core.tr.rewrite import rewrite
from ....core.tr.compile import compile
from ....core.tr.assemble import assemble
from ....core.tr.reduce import reduce
from ....core.tr.dump import serialize
from ....core.tr.lookup import identify
from .command import InsertCmd
from ..tr.dump import serialize_insert


class Clarify(Adapter):

    adapt(Domain, Domain)

    @staticmethod
    def convert(value):
        return value

    def __init__(self, origin_domain, domain):
        self.origin_domain = origin_domain
        self.domain = domain

    def __call__(self):
        if self.origin_domain == self.domain:
            return self.convert
        return None


class ProduceInsert(Act):

    adapt(InsertCmd, ProduceAction)

    def __call__(self):
        with transaction() as connection:
            product = produce(self.command.feed)
            table, columns, slice = self.introspect_feed(product.meta)
            converts = [unscramble(column.domain) for column in columns]
            reconverts = [scramble(column.domain) for column in columns]
            returning_columns = self.find_unique_key(table)
            sql = serialize_insert(table, columns, returning_columns)
            sql = sql.encode('utf-8')
            identity_plan = self.make_identity_statement(table,
                                                    returning_columns)
            identity_sql = identity_plan.statement.sql.encode('utf-8')
            identity_converts = [unscramble(domain)
                        for domain in identity_plan.statement.domains]
            meta = identity_plan.profile
            data = []
            if product.data is not None:
                cursor = connection.cursor()
                for record in product.data:
                    if record is None:
                        continue
                    values = tuple(record[idx] for idx in slice)
                    values = [convert(value)
                              for value, convert in zip(values, converts)]
                    values = [reconvert(value)
                              for value, reconvert in zip(values, reconverts)]
                    cursor.execute(sql, values)
                    returning_values = cursor.fetchall()
                    if len(returning_values) != 1:
                        raise BadRequestError("unable to locate inserted row")
                    [returning_values] = returning_values
                    cursor.execute(identity_sql, returning_values)
                    rows = []
                    for row in cursor:
                        row = tuple(convert(item)
                                    for item, convert in zip(row,
                                                    identity_converts))
                        rows.append(row)
                    stream = RowStream(rows, [])
                    ids = identity_plan.compose(None, stream)
                    stream.close()
                    if len(ids) != 1:
                        raise BadRequestError("unable to locate inserted row")
                    data.extend(ids)
        return Product(meta, data)

    def introspect_feed(self, profile):
        domain = profile.domain
        if not (isinstance(domain, ListDomain) and
                isinstance(domain.item_domain, RecordDomain)):
            feed_type = domain.family
            if isinstance(domain, ListDomain):
                feed_type += " of " + domain.item_domain.family
            raise BadRequestError("unexpected feed type: expected"
                                  " a list of records; got %s" % feed_type)
        fields = domain.item_domain.fields
        if profile.tag is None:
            raise BadRequestError("missing table name")
        signature = (normalize(profile.tag), None)
        arc_by_signature = dict(((label.name, label.arity), label.arc)
                                for label in classify(HomeNode()))
        if signature not in arc_by_signature:
            raise BadRequestError("unknown table name %s"
                                  % profile.tag.encode('utf-8'))
        arc = arc_by_signature[signature]
        if not isinstance(arc, TableArc):
            raise BadRequestError("expected a table name; got %s"
                                  % profile.tag.encode('utf-8'))
        table = arc.table
        arc_by_signature = dict(((label.name, label.arity), label.arc)
                                for label in classify(TableNode(table)))
        slots = []
        index_by_column = {}
        for idx, field in enumerate(fields):
            if field.tag is None:
                continue
            signature = (normalize(field.tag), None)
            if signature not in arc_by_signature:
                raise BadRequestError("unknown column name %s"
                                      % field.tag.encode('utf-8'))
            arc = arc_by_signature[signature]
            if not isinstance(arc, ColumnArc):
                raise BadRequestError("expected a column name; got %s"
                                      % field.tag.encode('utf-8'))
            column = arc.column
            index_by_column[column] = idx
        slice = []
        columns = []
        for column in table.columns:
            if column not in index_by_column:
                continue
            idx = index_by_column[column]
            field = fields[idx]
            if coerce(field.domain, column.domain) is None:
                raise BadRequestError("invalid type for column %s:"
                                      " expected %s; got %s"
                                      % (field.tag.encode('utf-8'),
                                         column.domain.family,
                                         field.domain.family))
            slice.append(idx)
            columns.append(column)
        return table, columns, slice

    def find_unique_key(self, table):
        returning_columns = []
        if table.primary_key is not None:
            returning_columns = table.primary_key.origin_columns
        else:
            for key in table.unique_keys:
                if key.is_partial:
                    continue
                if all(not column.is_nullable
                       for column in key.origin_columns):
                    returning_columns = key.origin_columns
                    break
        if not returning_columns:
            raise BadRequestError("table does not have a primary key")
        return returning_columns

    def make_identity_statement(self, table, returning_columns):
        state = BindingState()
        syntax = VoidSyntax()
        scope = RootBinding(syntax)
        state.set_root(scope)
        scope = state.use(FreeTableRecipe(table), syntax)
        state.push_scope(scope)
        conditions = []
        for idx, column in enumerate(returning_columns):
            column_binding = state.use(ColumnRecipe(column), syntax)
            placeholder_binding = FormulaBinding(scope,
                                                 PlaceholderSig(idx+1),
                                                 column_binding.domain,
                                                 syntax)
            condition = FormulaBinding(scope,
                                       IsEqualSig(+1),
                                       coerce(BooleanDomain()),
                                       syntax,
                                       lop=column_binding,
                                       rop=placeholder_binding)
            conditions.append(condition)
        if len(conditions) == 1:
            [condition] = conditions
        else:
            condition = FormulaBinding(scope,
                                       AndSig(),
                                       coerce(BooleanDomain()),
                                       syntax,
                                       ops=conditions)
        scope = SieveBinding(scope, condition, syntax)
        state.push_scope(scope)
        recipe = identify(scope)
        if recipe is None:
            raise BindRequestError("cannot determine table identity")
        binding = state.use(recipe, syntax)
        labels = relabel(TableArc(table))
        if labels:
            label = labels[0]
            identifier = IdentifierSyntax(label.name, EmptyMark())
            binding = AliasBinding(binding, identifier)
        state.pop_scope()
        state.pop_scope()
        binding = Select.__invoke__(binding, state)
        domain = ListDomain(binding.domain)
        binding = SegmentBinding(state.scope, binding, domain, syntax)
        profile = decorate(binding)
        binding = QueryBinding(state.scope, binding, profile, syntax)
        expression = encode(binding)
        expression = rewrite(expression)
        term = compile(expression)
        frame = assemble(term)
        frame = reduce(frame)
        plan = serialize(frame)
        assert plan.statement and not plan.statement.substatements
        return plan



