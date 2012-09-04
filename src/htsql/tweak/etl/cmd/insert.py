#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ....core.adapter import Adapter, adapt, adapt_many
from ....core.error import BadRequestError
from ....core.connect import transaction, scramble, unscramble
from ....core.mark import EmptyMark
from ....core.domain import (Domain, ListDomain, RecordDomain, BooleanDomain,
        IntegerDomain, FloatDomain, DecimalDomain, StringDomain, DateDomain,
        TimeDomain, DateTimeDomain, IdentityDomain, UntypedDomain)
from ....core.classify import normalize, classify, relabel
from ....core.model import HomeNode, TableNode, TableArc, ColumnArc, ChainArc
from ....core.cmd.act import Act, ProduceAction, produce
from ....core.cmd.retrieve import Product, RowStream
from ....core.tr.bind import BindingState, Select
from ....core.tr.syntax import VoidSyntax, IdentifierSyntax
from ....core.tr.binding import (VoidBinding, RootBinding, FormulaBinding,
        LocatorBinding, SelectionBinding, SieveBinding, AliasBinding,
        SegmentBinding, QueryBinding, FreeTableRecipe, ColumnRecipe)
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
import itertools
import datetime
import decimal


class Clarify(Adapter):

    adapt(Domain, Domain)

    def __init__(self, origin_domain, domain):
        self.origin_domain = origin_domain
        self.domain = domain

    def __call__(self):
        if self.origin_domain == self.domain:
            return (lambda v: v)
        return None


class ClarifyFromUntyped(Clarify):

    adapt(UntypedDomain, Domain)

    def __call__(self):
        return (lambda v, p=self.domain.parse: p(v))


class ClarifyFromSelf(Clarify):

    adapt_many((BooleanDomain, BooleanDomain),
               (IntegerDomain, IntegerDomain),
               (FloatDomain, FloatDomain),
               (DecimalDomain, DecimalDomain),
               (StringDomain, StringDomain),
               (DateDomain, DateDomain),
               (TimeDomain, TimeDomain),
               (DateTimeDomain, DateTimeDomain))

    def __call__(self):
        return (lambda v: v)


class ClarifyDecimal(Clarify):

    adapt_many((IntegerDomain, DecimalDomain),
               (FloatDomain, DecimalDomain))

    def __call__(self):
        return (lambda v: decimal.Decimal(v) if v is not None else None)


class ClarifyFloat(Clarify):

    adapt_many((IntegerDomain, FloatDomain),
               (DecimalDomain, FloatDomain))

    def __call__(self):
        return (lambda v: float(v) if v is not None else None)


class ClarifyDateTimeFromDate(Clarify):

    adapt(DateDomain, DateTimeDomain)

    def __call__(self):
        return (lambda v: datetime.datetime.combine(v, datetime.time())
                            if v is not None else None)


class ProduceInsert(Act):

    adapt(InsertCmd, ProduceAction)

    def __call__(self):
        with transaction() as connection:
            product = produce(self.command.feed)
            table, columns, extract = self.introspect_feed(product.meta)
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
                    values = extract(record, cursor)
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
        index_by_column = {}
        index_by_chain = {}
        plan_by_chain = {}
        for idx, field in enumerate(fields):
            if field.tag is None:
                continue
            signature = (normalize(field.tag), None)
            if signature not in arc_by_signature:
                raise BadRequestError("unknown column name %s"
                                      % field.tag.encode('utf-8'))
            arc = arc_by_signature[signature]
            if isinstance(arc, ColumnArc):
                column = arc.column
                index_by_column[column] = idx
            elif isinstance(arc, ChainArc):
                joins = tuple(arc.joins)
                if not joins[0].is_direct and all(join.reverse().is_contracting
                                                  for join in joins[1:]):
                    raise BadRequestError("cannot assign to a link %s"
                                          % field.tag.encode('utf-8'))
                index_by_chain[joins] = idx
                plan = self.make_link_statement(joins)
                plan_by_chain[joins] = plan
            else:
                raise BadRequestError("expected a column or a link name; got %s"
                                      % field.tag.encode('utf-8'))
        slice = []
        columns = []
        clarifies = []
        reconverts = []
        resolves = []
        indices = []
        output_index_by_column = {}
        for column in table.columns:
            chains = [joins for joins in index_by_chain
                      if column in joins[0].origin_columns]
            if column not in index_by_column and not chains:
                continue
            if (column in index_by_column and chains) or len(chains) > 1:
                raise BadRequestError("duplicate column assignment for %s"
                                      % column.name.encode('utf-8'))
            if column in index_by_column:
                idx = index_by_column[column]
                field = fields[idx]
                clarify = Clarify.__invoke__(field.domain, column.domain)
                if clarify is None:
                    raise BadRequestError("invalid type for column %s:"
                                          " expected %s; got %s"
                                          % (field.tag.encode('utf-8'),
                                             column.domain.family,
                                             field.domain.family))
                reconvert = scramble(column.domain)
                slice.append(idx)
                columns.append(column)
                clarifies.append(clarify)
                reconverts.append(reconvert)
                indices.append(len(slice)-1)
            else:
                [chain] = chains
                idx = index_by_chain[chain]
                plan, identity_domain, resolve = plan_by_chain[chain]
                field = fields[idx]
                if column == chain[0].origin_columns[0]:
                    clarify = Clarify.__invoke__(field.domain, identity_domain)
                    if clarify is None:
                        raise BadRequestError("invalid type for column %s:"
                                              " expected identity; got %s"
                                              % (field.tag.encode('utf-8'),
                                                 field.domain.family))
                    start = len(index_by_column)+len(index_by_chain)+ \
                            len(output_index_by_column)
                    for origin_column in chain[0].origin_columns:
                        output_index_by_column[origin_column] = start
                        start += 1
                    reconvert = scramble(column.domain)
                    index = output_index_by_column[column]
                    slice.append(idx)
                    columns.append(column)
                    clarifies.append(clarify)
                    reconverts.append(reconvert)
                    indices.append(index)
                    resolves.append((resolve, len(slice)-1))
                else:
                    reconvert = scramble(column.domain)
                    index = output_index_by_column[column]
                    columns.append(column)
                    reconverts.append(reconvert)
                    indices.append(index)
        def extract(record, cursor):
            input_values = tuple(record[idx] for idx in slice)
            try:
                input_values = [clarify(value)
                                for value, clarify
                                    in zip(input_values, clarifies)]
            except ValueError, exc:
                raise BadRequestError(str(exc))
            for resolve, resolve_idx in resolves:
                input_value = input_values[resolve_idx]
                resolved_values = resolve(input_value, cursor)
                input_values.extend(resolved_values)
            values = [input_values[idx] for idx in indices]
            values = [reconvert(value)
                      for value, reconvert in zip(values, reconverts)]
            return values
        return table, columns, extract

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

    def make_link_statement(self, joins):
        state = BindingState()
        syntax = VoidSyntax()
        scope = RootBinding(syntax)
        state.set_root(scope)
        table = joins[-1].target
        seed = state.use(FreeTableRecipe(table), syntax)
        recipe = identify(seed)
        if recipe is None:
            raise BadRequestError("cannot determine identity of a link")
        identity = state.use(recipe, syntax, scope=seed)
        idx = itertools.count()
        def make_value(domain):
            value = []
            for field in domain.fields:
                if isinstance(field, IdentityDomain):
                    item = make_value(field)
                else:
                    item = FormulaBinding(scope,
                                          PlaceholderSig(next(idx)+1),
                                          field,
                                          syntax)
                value.append(item)
            return tuple(value)
        value = make_value(identity.domain)
        scope = LocatorBinding(scope, seed, identity, value, syntax)
        state.push_scope(scope)
        if len(joins) > 1:
            recipe = AttachedTableRecipe([join.reverse()
                                          for join in reversed(joins[1:])])
            scope = state.use(recipe, syntax)
            state.push_scope(scope)
        elements = []
        for column in joins[0].target_columns:
            binding = state.use(ColumnRecipe(column), syntax)
            elements.append(binding)
        fields = [decorate(element) for element in elements]
        domain = RecordDomain(fields)
        scope = SelectionBinding(scope, elements, domain, syntax)
        binding = Select.__invoke__(scope, state)
        domain = ListDomain(binding.domain)
        binding = SegmentBinding(state.root, binding, domain, syntax)
        profile = decorate(binding)
        binding = QueryBinding(state.root, binding, profile, syntax)
        expression = encode(binding)
        expression = rewrite(expression)
        term = compile(expression)
        frame = assemble(term)
        frame = reduce(frame)
        plan = serialize(frame)
        assert plan.statement and not plan.statement.substatements
        raw_domains = []
        for leaf in identity.domain.leaves:
            domain = identity.domain
            for idx in leaf:
                domain = domain.fields[idx]
            raw_domains.append(domain)
        raw_reconverts = []
        for raw_domain in raw_domains:
            raw_reconvert = scramble(raw_domain)
            raw_reconverts.append(raw_reconvert)
        resolve_sql = plan.statement.sql.encode('utf-8')
        resolve_converts = [unscramble(domain)
                            for domain in plan.statement.domains]
        def resolve(value, cursor,
                    resolve_sql=resolve_sql,
                    resolve_converts=resolve_converts,
                    reconverts=raw_reconverts,
                    leaves=identity.domain.leaves):
            raw_values = []
            for leaf in leaves:
                raw_value = value
                for idx in leaf:
                    raw_value = raw_value[idx]
                raw_values.append(raw_value)
            raw_values = [reconvert(raw_value)
                          for raw_value, reconvert
                            in zip(raw_values, reconverts)]
            cursor.execute(resolve_sql, raw_values)
            rows = []
            for row in cursor:
                row = tuple(convert(item)
                            for item, convert in zip(row, resolve_converts))
                rows.append(row)
            stream = RowStream(rows, [])
            data = plan.compose(None, stream)
            stream.close()
            if len(data) != 1:
                raise BadRequestError("unable to resolve a link")
            return data[0]
        return plan, identity.domain, resolve


