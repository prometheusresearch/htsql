#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ....core.util import listof
from ....core.adapter import Utility, Adapter, adapt, adapt_many
from ....core.error import Error, PermissionError
from ....core.context import context
from ....core.connect import transaction, scramble, unscramble
from ....core.domain import (Domain, ListDomain, RecordDomain, BooleanDomain,
        IntegerDomain, FloatDomain, DecimalDomain, TextDomain, EnumDomain,
        DateDomain, TimeDomain, DateTimeDomain, IdentityDomain, UntypedDomain,
        Product, Value, ID)
from ....core.classify import normalize, classify, relabel
from ....core.model import (HomeNode, TableNode, Arc, TableArc, ColumnArc,
        ChainArc)
from ....core.entity import TableEntity, ColumnEntity
from ....core.cmd.act import Act, ProduceAction, act
from ....core.tr.translate import translate
from ....core.tr.bind import BindingState, Select
from ....core.syn.syntax import VoidSyntax, IdentifierSyntax
from ....core.tr.binding import (VoidBinding, RootBinding, FormulaBinding,
        LocateBinding, SelectionBinding, SieveBinding, AliasBinding,
        CollectBinding, FreeTableRecipe, ColumnRecipe)
from ....core.tr.signature import IsEqualSig, AndSig, PlaceholderSig
from ....core.tr.decorate import decorate
from ....core.tr.coerce import coerce
from ....core.tr.lookup import identify
from .command import InsertCmd
from ..tr.dump import serialize_insert
import itertools
import datetime
import decimal
import operator


class Clarify(Adapter):

    adapt(Domain, Domain)

    identity = staticmethod(lambda v: v)

    def __init__(self, origin_domain, domain):
        self.origin_domain = origin_domain
        self.domain = domain

    def __call__(self):
        if self.origin_domain == self.domain:
            return self.identity
        return None


class ClarifyFromUntyped(Clarify):

    adapt(UntypedDomain, Domain)

    def __call__(self):
        return self.domain.parse


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
        return self.identity


class ClarifyEnum(Clarify):

    adapt_many((EnumDomain, EnumDomain),
               (TextDomain, EnumDomain))

    def __call__(self):
        return self.domain.parse


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
            return self.identity
        if self.origin_domain.width != self.domain.width:
            return None
        group = list(enumerate(self.origin_domain.labels))
        return self.align(group, self.domain)

    def align(self, group, domain):
        converts = []
        for label in domain.labels:
            if isinstance(label, IdentityDomain):
                subgroup = []
                subwidth = 0
                while subwidth < label.width:
                    idx, entry = group.pop(0)
                    subgroup.append((idx, entry))
                    if isinstance(entry, IdentityDomain):
                        subwidth += entry.width
                    else:
                        subwidth += 1
                if subwidth > label.width:
                    return None
                if (len(subgroup) == 1 and
                        isinstance(subgroup[0][1], IdentityDomain)):
                    idx, entry = subgroup[0]
                    subgroup = list(enumerate(entry.labels))
                    convert = self.align(subgroup, label)
                    if convert is None:
                        return None
                    converts.append(lambda v, i=idx, c=convert: c(v[i]))
                else:
                    convert = self.align(subgroup, label)
                    if convert is None:
                        return None
                    converts.append(convert)
            else:
                idx, entry = group.pop(0)
                convert = Clarify.__invoke__(entry, label)
                if convert is None:
                    return None
                if convert is Clarify.identity:
                    converts.append(operator.itemgetter(idx))
                else:
                    converts.append(lambda v, i=idx, c=convert: c(v[i]))
        id_class = ID.make(domain.dump)
        return (lambda v, id_class=id_class, cs=converts:
                        id_class([c(v) for c in cs]) if v is not None else None)


class ExtractValuePipe(object):

    def __init__(self, name, from_domain, to_domain, convert, index):
        self.name = name
        self.from_domain = from_domain
        self.to_domain = to_domain
        self.convert = convert
        self.index = index

    def __call__(self, row):
        item = row[self.index]
        try:
            return self.convert(item)
        except ValueError:
            message = "Failed to adapt value of %s to %s" \
                    % (self.name, self.to_domain)
            quote = str(Value(self.from_domain, item))
            raise Error(message, quote)


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
                    tuple([convert(row) for convert in self.converts]))
        else:
            return tuple([convert(row) for convert in self.converts])


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
            raise Error("Expected a record or a list of records;"
                        " got %s" % domain)
        if is_list:
            fields = domain.item_domain.fields
        else:
            fields = domain.fields
        if self.profile.tag is None:
            raise Error("Missing table name")
        signature = (normalize(self.profile.tag), None)
        arc_by_signature = dict(((label.name, label.arity), label.arc)
                                for label in classify(HomeNode()))
        if signature not in arc_by_signature:
            raise Error("Got unknown table", self.profile.tag)
        arc = arc_by_signature[signature]
        if not isinstance(arc, TableArc):
            raise Error("Expected the name of a table", self.profile.tag)
        node = TableNode(arc.table)
        id_convert = None
        if self.with_id:
            if not fields:
                raise Error("Expected the first field to be an identity")
            id_field = fields[0]
            syntax = VoidSyntax()
            scope = RootBinding(syntax)
            state = BindingState(scope)
            seed = state.use(FreeTableRecipe(node.table), syntax)
            recipe = identify(seed)
            if recipe is None:
                raise Error("Cannot determine identity of the table")
            identity = state.use(recipe, syntax, scope=seed)
            id_domain = identity.domain
            clarify = Clarify.__invoke__(id_field.domain, id_domain)
            if clarify is None:
                raise Error("Expected the first field to be"
                            " the table identity; got %s" % id_field.domain)
            id_convert = ExtractValuePipe(signature[0], id_field.domain,
                                          id_domain, clarify, 0)
            if clarify is Clarify.identity:
                id_convert = operator.itemgetter(0)
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
                    raise Error("Expected a column or a link name", field.tag)
                arc = arc_by_signature[signature]
                if not isinstance(arc, (ColumnArc, ChainArc)):
                    raise Error("Expected a column or a link name", field.tag)
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
                        raise Error("Got duplicate field", field.tag)
                    arcs.append(arc)
                    if isinstance(arc, ColumnArc):
                        arc_domain = arc.column.domain
                    elif isinstance(arc, ChainArc):
                        joins = arc.joins
                        if not joins[0].is_direct and \
                                all(join.reverse().is_contracting
                                    for join in joins[1:]):
                            raise Error("Cannot assign to link", field.tag)
                        syntax = VoidSyntax()
                        scope = RootBinding(syntax)
                        state = BindingState(scope)
                        seed = state.use(FreeTableRecipe(arc.target.table),
                                         syntax)
                        recipe = identify(seed)
                        if recipe is None:
                            raise Error("Cannot determine identity of a link",
                                        field.tag)
                        identity = state.use(recipe, syntax, scope=seed)
                        arc_domain = identity.domain
                    clarify = Clarify.__invoke__(field.domain, arc_domain)
                    if clarify is None:
                        raise Error("Invalid type for column %s:"
                                    " expected %s; got %s"
                                    % (field.tag.encode('utf-8'),
                                       arc_domain, field.domain))
                    convert = ExtractValuePipe(label.name, field.domain,
                                               arc_domain, clarify, idx)
                    if clarify is Clarify.identity:
                        convert = operator.itemgetter(idx)
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
        row = [resolve(item) if resolve is not None else item
               for item, resolve in zip(row, self.resolves)]
        return tuple([extract(row) for extract in self.extracts])


class BuildExtractTable(Utility):

    def __init__(self, node, arcs, with_cache=False):
        assert isinstance(node, TableNode)
        assert isinstance(arcs, listof(Arc))
        self.node = node
        self.arcs = arcs
        self.with_cache = with_cache

    def __call__(self):
        table = self.node.table
        resolves = []
        extract_by_column = {}
        for idx, arc in enumerate(self.arcs):
            if isinstance(arc, ColumnArc):
                column = arc.column
                if column in extract_by_column:
                    raise Error("Duplicate column assignment for %s"
                                % column.name.encode('utf-8'))
                resolve = None
                extract = operator.itemgetter(idx)
                resolves.append(resolve)
                extract_by_column[column] = extract
            elif isinstance(arc, ChainArc):
                if self.with_cache:
                    resolve = BuildCacheChain.__invoke__(arc)
                else:
                    resolve = BuildResolveChain.__invoke__(arc)
                resolves.append(resolve)
                for column_idx, column in enumerate(resolve.columns):
                    if column in extract_by_column:
                        raise Error("Duplicate column assignment for %s"
                                    % column.name.encode('utf-8'))
                    extract = (lambda r, i=idx, k=column_idx: r[i][k])
                    extract_by_column[column] = extract
        columns = []
        extracts = []
        for column in table:
            if column in extract_by_column:
                columns.append(column)
                extracts.append(extract_by_column[column])
        return ExtractTablePipe(table, columns, resolves, extracts)


class ExecuteInsertPipe(object):

    def __init__(self, table, input_columns, output_columns, sql):
        assert isinstance(table, TableEntity)
        assert isinstance(input_columns, listof(ColumnEntity))
        assert isinstance(output_columns, listof(ColumnEntity))
        assert isinstance(sql, str)
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
        if not context.env.can_write:
            raise PermissionError("No write permissions")
        with transaction() as connection:
            cursor = connection.cursor()
            cursor.execute(self.sql.encode('utf-8'), row)
            rows = cursor.fetchall()
            if len(rows) != 1:
                raise Error("Failed to insert a record")
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
            raise Error("Table does not have a primary key")
        sql = serialize_insert(table, self.columns, returning_columns)
        return ExecuteInsertPipe(table, self.columns, returning_columns, sql)


class ResolveIdentityPipe(object):

    def __init__(self, profile, pipe):
        self.profile = profile
        self.pipe = pipe

    def __call__(self, row):
        product = self.pipe()(row)
        data = product.data
        if len(data) != 1:
            raise Error("Unable to locate the inserted record")
        return data[0]


class BuildResolveIdentity(Utility):

    def __init__(self, table, columns, is_list=True):
        assert isinstance(table, TableEntity)
        assert isinstance(columns, listof(ColumnEntity))
        self.table = table
        self.columns = columns
        self.is_list = is_list

    def __call__(self):
        syntax = VoidSyntax()
        scope = RootBinding(syntax)
        state = BindingState(scope)
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
            raise Error("Cannot determine table identity")
        binding = state.use(recipe, syntax)
        labels = relabel(TableArc(self.table))
        if labels:
            label = labels[0]
            identifier = IdentifierSyntax(label.name)
            binding = AliasBinding(binding, identifier)
        state.pop_scope()
        state.pop_scope()
        binding = Select.__invoke__(binding, state)
        domain = ListDomain(binding.domain)
        binding = CollectBinding(state.scope, binding, domain, syntax)
        pipe = translate(binding)
        profile = pipe.meta
        if not self.is_list:
            profile = profile.clone(domain=profile.domain.item_domain)
        return ResolveIdentityPipe(profile, pipe)


class ResolveChainPipe(object):

    def __init__(self, name, columns, domain, pipe):
        assert isinstance(columns, listof(ColumnEntity))
        self.name = name
        self.columns = columns
        self.pipe = pipe
        self.domain = domain

    def __call__(self, value):
        if value is None:
            return (None,)*len(self.columns)
        raw_values = []
        for leaf in self.domain.leaves:
            raw_value = value
            for idx in leaf:
                raw_value = raw_value[idx]
            raw_values.append(raw_value)
        product = self.pipe()(raw_values)
        data = product.data
        if len(data) != 1:
            quote = None
            if self.name:
                quote = "%s[%s]" % (self.name, self.domain.dump(value))
            else:
                quote = "[%s]" % self.domain.dump(value)
            raise Error("Unable to resolve a link", quote)
        return data[0]


class BuildResolveChain(Utility):

    def __init__(self, arc):
        self.arc = arc
        self.joins = arc.joins

    def __call__(self):
        target_labels = relabel(TableArc(self.arc.target.table))
        target_name = target_labels[0].name if target_labels else None
        joins = self.joins
        syntax = VoidSyntax()
        scope = RootBinding(syntax)
        state = BindingState(scope)
        seed = state.use(FreeTableRecipe(joins[-1].target), syntax)
        recipe = identify(seed)
        if recipe is None:
            raise Error("Cannot determine identity of a link", target_name)
        identity = state.use(recipe, syntax, scope=seed)
        count = itertools.count()
        def make_images(identity):
            images = []
            for field in identity.elements:
                if isinstance(field.domain, IdentityDomain):
                    images.extend(make_images(field))
                else:
                    item = FormulaBinding(scope,
                                          PlaceholderSig(next(count)),
                                          field.domain,
                                          syntax)
                    images.append((item, field))
            return images
        images = make_images(identity)
        scope = LocateBinding(scope, seed, images, None, syntax)
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
        binding = CollectBinding(state.root, binding, domain, syntax)
        pipe =  translate(binding)
        columns = joins[0].origin_columns[:]
        domain = identity.domain
        return ResolveChainPipe(target_name, columns, domain, pipe)


class CacheChainPipe(object):

    def __init__(self, name, columns, domain, pipe):
        assert isinstance(columns, listof(ColumnEntity))
        self.name = name
        self.columns = columns
        self.pipe = pipe
        self.domain = domain
        self.cache = None

    def __call__(self, value):
        if value is None:
            return (None,)*len(self.columns)
        if self.cache is None:
            self.cache = {}
            product = self.pipe()(None)
            for row in product:
                self.cache[row[0]] = row[1:]
        try:
            return self.cache[value]
        except KeyError:
            quote = None
            if self.name:
                quote = "%s[%s]" % (self.name, self.domain.dump(value))
            else:
                quote = "[%s]" % self.domain.dump(value)
            raise Error("Unable to resolve a link", quote)


class BuildCacheChain(Utility):

    def __init__(self, arc):
        self.arc = arc
        self.joins = arc.joins

    def __call__(self):
        target_labels = relabel(TableArc(self.arc.target.table))
        target_name = target_labels[0].name if target_labels else None
        joins = self.joins
        syntax = VoidSyntax()
        scope = RootBinding(syntax)
        state = BindingState(scope)
        seed = state.use(FreeTableRecipe(joins[-1].target), syntax)
        state.push_scope(seed)
        recipe = identify(seed)
        if recipe is None:
            raise Error("Cannot determine identity of a link", target_name)
        identity = state.use(recipe, syntax, scope=seed)
        elements = [identity]
        for column in joins[0].target_columns:
            binding = state.use(ColumnRecipe(column), syntax)
            elements.append(binding)
        fields = [decorate(element) for element in elements]
        domain = RecordDomain(fields)
        scope = SelectionBinding(seed, elements, domain, syntax)
        binding = Select.__invoke__(scope, state)
        domain = ListDomain(binding.domain)
        binding = CollectBinding(state.root, binding, domain, syntax)
        pipe =  translate(binding)
        columns = joins[0].origin_columns[:]
        domain = identity.domain
        return CacheChainPipe(target_name, columns, domain, pipe)


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
                record_domain = product.meta.domain.item_domain
            else:
                records = [product.data]
                record_domain = product.meta.domain
            for idx, record in enumerate(records):
                if record is None:
                    continue
                try:
                    row = resolve_identity(
                            execute_insert(
                                extract_table(
                                    extract_node(record))))
                except Error as exc:
                    if extract_node.is_list:
                        message = "While inserting record #%s" % (idx+1)
                    else:
                        message = "While inserting a record"
                    quote = record_domain.dump(record)
                    exc.wrap(message, quote)
                    raise
                data.append(row)
            if not extract_node.is_list:
                assert len(data) <= 1
                if data:
                    data = data[0]
                else:
                    data = None
            return Product(meta, data)


