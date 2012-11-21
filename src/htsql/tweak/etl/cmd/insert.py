#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ....core.util import listof
from ....core.adapter import Utility, Adapter, adapt, adapt_many
from ....core.error import BadRequestError, EmptyMark
from ....core.connect import transaction, scramble, unscramble
from ....core.domain import (Domain, ListDomain, RecordDomain, BooleanDomain,
        IntegerDomain, FloatDomain, DecimalDomain, TextDomain, DateDomain,
        TimeDomain, DateTimeDomain, IdentityDomain, UntypedDomain, Product)
from ....core.classify import normalize, classify, relabel
from ....core.model import (HomeNode, TableNode, Arc, TableArc, ColumnArc,
        ChainArc)
from ....core.entity import TableEntity, ColumnEntity
from ....core.cmd.act import Act, ProduceAction, act
from ....core.cmd.fetch import RowStream, build_fetch
from ....core.tr.bind import BindingState, Select
from ....core.syn.syntax import VoidSyntax, IdentifierSyntax
from ....core.tr.binding import (VoidBinding, RootBinding, FormulaBinding,
        LocatorBinding, SelectionBinding, SieveBinding, AliasBinding,
        SegmentBinding, QueryBinding, FreeTableRecipe, ColumnRecipe)
from ....core.tr.signature import IsEqualSig, AndSig, PlaceholderSig
from ....core.tr.decorate import decorate
from ....core.tr.coerce import coerce
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
               (TextDomain, TextDomain),
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


class ClarifyIdentity(Clarify):

    adapt(IdentityDomain, IdentityDomain)

    def __call__(self):
        if self.origin_domain == self.domain:
            return (lambda v: v)
        if self.origin_domain.width != self.domain.width:
            return None
        converts = []
        for origin_field, field in zip(self.origin_domain.labels,
                                       self.domain.labels):
            convert = Clarify.__invoke__(origin_field, field)
            if convert is None:
                return None
            converts.append(convert)
        return (lambda v, cs=converts: tuple(c(i) for i, c in zip(v, cs))
                                       if v is not None else None)


class ExtractNodePipe(object):

    def __init__(self, node, arcs, id_convert, converts, is_list):
        assert isinstance(node, TableNode)
        assert isinstance(arcs, listof(Arc))
        assert isinstance(converts, list)
        self.node = node
        self.arcs = arcs
        self.id_convert = id_convert
        self.converts = converts
        self.is_list = is_list

    def __call__(self, row):
        if self.id_convert is not None:
            return (self.id_convert(row),
                    tuple(convert(row) for convert in self.converts))
        else:
            return tuple(convert(row) for convert in self.converts)


class BuildExtractNode(Utility):

    def __init__(self, profile, with_id=False, with_fields=True):
        self.profile = profile
        self.with_id = with_id
        self.with_fields = with_fields

    def __call__(self):
        domain = self.profile.domain
        is_list = (isinstance(domain, ListDomain))
        if not ((isinstance(domain, ListDomain) and
                 isinstance(domain.item_domain, RecordDomain)) or
                isinstance(domain, RecordDomain)):
            raise BadRequestError("unexpected feed type: expected"
                                  " a list of records; got %s" % domain)
        if is_list:
            fields = domain.item_domain.fields
        else:
            fields = domain.fields
        if self.profile.tag is None:
            raise BadRequestError("missing table name")
        signature = (normalize(self.profile.tag), None)
        arc_by_signature = dict(((label.name, label.arity), label.arc)
                                for label in classify(HomeNode()))
        if signature not in arc_by_signature:
            raise BadRequestError("unknown table name %s"
                                  % self.profile.tag.encode('utf-8'))
        arc = arc_by_signature[signature]
        if not isinstance(arc, TableArc):
            raise BadRequestError("expected a table name; got %s"
                                  % self.profile.tag.encode('utf-8'))
        node = TableNode(arc.table)
        id_convert = None
        if self.with_id:
            if not fields:
                raise BadRequestError("the first field is expected to be"
                                      " a table identity")
            id_field = fields[0]
            state = BindingState()
            syntax = VoidSyntax()
            scope = RootBinding(syntax)
            state.set_root(scope)
            seed = state.use(FreeTableRecipe(node.table), syntax)
            recipe = identify(seed)
            if recipe is None:
                raise BadRequestError("cannot determine identity of the table")
            identity = state.use(recipe, syntax, scope=seed)
            id_domain = identity.domain
            clarify = Clarify.__invoke__(id_field.domain, id_domain)
            if clarify is None:
                raise BadRequestError("the first field is expected to be"
                                      " a table identity")
            id_convert = (lambda r, c=clarify: c(r[0]))
        if self.with_fields:
            labels = classify(node)
            arc_by_signature = dict(((label.name, label.arity), label.arc)
                                    for label in labels)
            index_by_arc = {}
            for idx, field in enumerate(fields):
                if self.with_id and idx == 0:
                    continue
                if field.tag is None:
                    continue
                signature = (normalize(field.tag), None)
                if signature not in arc_by_signature:
                    raise BadRequestError("unknown column name %s"
                                          % field.tag.encode('utf-8'))
                arc = arc_by_signature[signature]
                if not isinstance(arc, (ColumnArc, ChainArc)):
                    raise BadRequestError("expected a column or a link name;"
                                          " got %s" % field.tag.encode('utf-8'))
                index_by_arc[arc] = idx
        arcs = []
        converts = []
        if self.with_fields:
            for label in labels:
                if label.arc in index_by_arc:
                    arc = label.arc
                    idx = index_by_arc[arc]
                    field = fields[idx]
                    if (isinstance(arc, ColumnArc) and arc.link is not None
                            and isinstance(field.domain, IdentityDomain)):
                        arc = arc.link
                    if arc in arcs:
                        raise BadRequestError("duplicate field %s"
                                              % field.tag.encode('utf-8'))
                    arcs.append(arc)
                    if isinstance(arc, ColumnArc):
                        arc_domain = arc.column.domain
                    elif isinstance(arc, ChainArc):
                        joins = arc.joins
                        if not joins[0].is_direct and \
                                all(join.reverse().is_contracting
                                    for join in joins[1:]):
                            raise BadRequestError("cannot assign to link %s"
                                                  % field.tag.encode('utf-8'))
                        state = BindingState()
                        syntax = VoidSyntax()
                        scope = RootBinding(syntax)
                        state.set_root(scope)
                        seed = state.use(FreeTableRecipe(arc.target.table),
                                         syntax)
                        recipe = identify(seed)
                        if recipe is None:
                            raise BadRequestError("cannot determine"
                                                  " identity of a link")
                        identity = state.use(recipe, syntax, scope=seed)
                        arc_domain = identity.domain
                    clarify = Clarify.__invoke__(field.domain, arc_domain)
                    if clarify is None:
                        raise BadRequestError("invalid type for column %s:"
                                              " expected %s; got %s"
                                              % (field.tag.encode('utf-8'),
                                                 arc_domain,
                                                 field.domain))
                    convert = (lambda v, i=idx, c=clarify: c(v[i]))
                    converts.append(convert)
        return ExtractNodePipe(node, arcs, id_convert, converts, is_list)


class ExtractTablePipe(object):

    def __init__(self, table, columns, resolves, extracts):
        assert isinstance(table, TableEntity)
        assert isinstance(columns, listof(ColumnEntity))
        self.table = table
        self.columns = columns
        self.resolves = resolves
        self.extracts = extracts

    def __call__(self, row):
        row = [resolve(item) for item, resolve in zip(row, self.resolves)]
        return tuple(extract(row) for extract in self.extracts)


class BuildExtractTable(Utility):

    def __init__(self, node, arcs):
        assert isinstance(node, TableNode)
        assert isinstance(arcs, listof(Arc))
        self.node = node
        self.arcs = arcs

    def __call__(self):
        table = self.node.table
        resolves = []
        extract_by_column = {}
        for idx, arc in enumerate(self.arcs):
            if isinstance(arc, ColumnArc):
                column = arc.column
                if column in extract_by_column:
                    raise BadRequestError("duplicate column assignment for %s"
                                          % column.name.encode('utf-8'))
                resolve = (lambda v: v)
                extract = (lambda r, i=idx: r[i])
                resolves.append(resolve)
                extract_by_column[column] = extract
            elif isinstance(arc, ChainArc):
                resolve_pipe = BuildResolveChain.__invoke__(arc.joins)
                resolve = (lambda v, p=resolve_pipe: p(v))
                resolves.append(resolve)
                for column_idx, column in enumerate(resolve_pipe.columns):
                    if column in extract_by_column:
                        raise BadRequestError("duplicate column assignment for %s"
                                              % column.name.encode('utf-8'))
                    extract = (lambda r, i=idx, k=column_idx: r[i][k])
                    extract_by_column[column] = extract
        columns = []
        extracts = []
        for column in table.columns:
            if column in extract_by_column:
                columns.append(column)
                extracts.append(extract_by_column[column])
        return ExtractTablePipe(table, columns, resolves, extracts)


class ExecuteInsertPipe(object):

    def __init__(self, table, input_columns, output_columns, sql):
        assert isinstance(table, TableEntity)
        assert isinstance(input_columns, listof(ColumnEntity))
        assert isinstance(output_columns, listof(ColumnEntity))
        assert isinstance(sql, unicode)
        self.table = table
        self.input_columns = input_columns
        self.output_columns = output_columns
        self.sql = sql
        self.input_converts = [scramble(column.domain)
                               for column in input_columns]
        self.output_converts = [unscramble(column.domain)
                                for column in output_columns]

    def __call__(self, row):
        row = tuple(convert(item)
               for item, convert in zip(row, self.input_converts))
        with transaction() as connection:
            cursor = connection.cursor()
            cursor.execute(self.sql.encode('utf-8'), row)
            rows = cursor.fetchall()
            if len(rows) != 1:
                raise BadRequestError("unable to locate inserted row")
            [row] = rows
        return row


class BuildExecuteInsert(Utility):

    def __init__(self, table, columns):
        assert isinstance(table, TableEntity)
        assert isinstance(columns, listof(ColumnEntity))
        self.table = table
        self.columns = columns

    def __call__(self):
        table = self.table
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
        sql = serialize_insert(table, self.columns, returning_columns)
        return ExecuteInsertPipe(table, self.columns, returning_columns, sql)


class ResolveIdentityPipe(object):

    def __init__(self, profile, pipe):
        self.profile = profile
        self.pipe = pipe

    def __call__(self, row):
        product = self.pipe(row)
        data = product.data
        if len(data) != 1:
            raise BadRequestError("unable to locate inserted row")
        return data[0]


class BuildResolveIdentity(Utility):

    def __init__(self, table, columns, is_list=True):
        assert isinstance(table, TableEntity)
        assert isinstance(columns, listof(ColumnEntity))
        self.table = table
        self.columns = columns
        self.is_list = is_list

    def __call__(self):
        state = BindingState()
        syntax = VoidSyntax()
        scope = RootBinding(syntax)
        state.set_root(scope)
        scope = state.use(FreeTableRecipe(self.table), syntax)
        state.push_scope(scope)
        conditions = []
        for idx, column in enumerate(self.columns):
            column_binding = state.use(ColumnRecipe(column), syntax)
            placeholder_binding = FormulaBinding(scope,
                                                 PlaceholderSig(idx),
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
            raise BadRequestError("cannot determine table identity")
        binding = state.use(recipe, syntax)
        labels = relabel(TableArc(self.table))
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
        pipe = build_fetch(binding)
        profile = pipe.profile
        if not self.is_list:
            profile = profile.clone(domain=profile.domain.item_domain)
        return ResolveIdentityPipe(profile, pipe)


class ResolveChainPipe(object):

    def __init__(self, columns, domain, pipe):
        assert isinstance(columns, listof(ColumnEntity))
        self.columns = columns
        self.pipe = pipe
        self.leaves = domain.leaves

    def __call__(self, value):
        if value is None:
            return (None,)*len(self.columns)
        raw_values = []
        for leaf in self.leaves:
            raw_value = value
            for idx in leaf:
                raw_value = raw_value[idx]
            raw_values.append(raw_value)
        product = self.pipe(raw_values)
        data = product.data
        if len(data) != 1:
            raise BadRequestError("unable to resolve a link")
        return data[0]


class BuildResolveChain(Utility):

    def __init__(self, joins):
        self.joins = joins

    def __call__(self):
        joins = self.joins
        state = BindingState()
        syntax = VoidSyntax()
        scope = RootBinding(syntax)
        state.set_root(scope)
        seed = state.use(FreeTableRecipe(joins[-1].target), syntax)
        recipe = identify(seed)
        if recipe is None:
            raise BadRequestError("cannot determine identity of a link")
        identity = state.use(recipe, syntax, scope=seed)
        count = itertools.count()
        def make_value(domain):
            value = []
            for field in domain.labels:
                if isinstance(field, IdentityDomain):
                    item = make_value(field)
                else:
                    item = FormulaBinding(scope,
                                          PlaceholderSig(next(count)),
                                          field,
                                          syntax)
                value.append(item)
            return tuple(value)
        value = make_value(identity.domain)
        scope = LocatorBinding(scope, seed, identity, value, syntax)
        state.push_scope(scope)
        if len(joins) > 1:
            recipe = AttachedTableRecipe([join.reverse()
                                          for join in joins[:0:-1]])
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
        pipe =  build_fetch(binding)
        columns = joins[0].origin_columns[:]
        domain = identity.domain
        return ResolveChainPipe(columns, domain, pipe)


class ProduceInsert(Act):

    adapt(InsertCmd, ProduceAction)

    def __call__(self):
        with transaction() as connection:
            product = act(self.command.feed, self.action)
            extract_node = BuildExtractNode.__invoke__(product.meta)
            extract_table = BuildExtractTable.__invoke__(
                    extract_node.node, extract_node.arcs)
            execute_insert = BuildExecuteInsert.__invoke__(
                    extract_table.table, extract_table.columns)
            resolve_identity = BuildResolveIdentity.__invoke__(
                    execute_insert.table, execute_insert.output_columns,
                    extract_node.is_list)
            meta = resolve_identity.profile
            data = []
            if extract_node.is_list:
                records = product.data
            else:
                records = [product.data]
            for record in records:
                if record is None:
                    continue
                row = resolve_identity(
                        execute_insert(
                            extract_table(
                                extract_node(record))))
                data.append(row)
            if not extract_node.is_list:
                assert len(data) <= 1
                if data:
                    data = data[0]
                else:
                    data = None
            return Product(meta, data)


