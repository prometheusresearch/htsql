#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ....core.util import listof
from ....core.adapter import Utility, adapt
from ....core.context import context
from ....core.error import Error, PermissionError
from ....core.entity import TableEntity, ColumnEntity
from ....core.model import TableArc, ColumnArc, ChainArc
from ....core.classify import localize, relabel
from ....core.connect import transaction, scramble, unscramble
from ....core.domain import IdentityDomain, RecordDomain, ListDomain, Product
from ....core.cmd.fetch import translate
from ....core.cmd.act import Act, ProduceAction, act
from ....core.tr.bind import BindingState, Select
from ....core.syn.syntax import VoidSyntax
from ....core.tr.binding import (VoidBinding, RootBinding, FormulaBinding,
        LocateBinding, SelectionBinding, SieveBinding, AliasBinding,
        CollectBinding, FreeTableRecipe, ColumnRecipe)
from ....core.tr.signature import IsEqualSig, AndSig, PlaceholderSig
from ....core.tr.decorate import decorate
from ....core.tr.coerce import coerce
from ....core.tr.lookup import prescribe
from .command import MergeCmd
from .insert import (BuildExtractNode, BuildExtractTable, BuildExecuteInsert,
        BuildResolveIdentity, BuildResolveChain)
from ..tr.dump import serialize_update
import itertools


class ExtractIdentityPipe:

    def __init__(self, node, arcs, id_indices, other_indices):
        self.node = node
        self.arcs = arcs
        self.id_indices = id_indices
        self.other_indices = other_indices

    def __call__(self, row):
        return (tuple(row[idx] for idx in self.id_indices),
                tuple(row[idx] for idx in self.other_indices))


class BuildExtractIdentity(Utility):

    def __init__(self, node, arcs):
        self.node = node
        self.arcs = arcs

    def __call__(self):
        identity_arcs = localize(self.node)
        if identity_arcs is None:
            raise Error("Expected a table with identity")
        index_by_arc = {}
        for index, arc in enumerate(self.arcs):
            index_by_arc[arc] = index
            if isinstance(arc, ColumnArc) and arc.link is not None:
                index_by_arc[arc.link] = index
        id_indices = []
        for arc in identity_arcs:
            if arc not in index_by_arc:
                labels = relabel(arc)
                if not labels:
                    raise Error("Missing identity field")
                else:
                    label = labels[0]
                    raise Error("Missing identity field %s"
                                % label.name)
            index = index_by_arc[arc]
            id_indices.append(index)
        other_indices = []
        arcs = []
        for idx, arc in enumerate(self.arcs):
            if idx in id_indices:
                continue
            other_indices.append(idx)
            arcs.append(arc)
        return ExtractIdentityPipe(self.node, arcs, id_indices, other_indices)


class ResolveKeyPipe:

    def __init__(self, name, columns, domain, pipe, with_error):
        self.name = name
        self.columns = columns
        self.pipe = pipe
        self.domain = domain
        self.leaves = domain.leaves
        self.with_error = with_error

    def __call__(self, value):
        assert value is not None
        raw_values = []
        for leaf in self.leaves:
            raw_value = value
            for idx in leaf:
                raw_value = raw_value[idx]
            raw_values.append(raw_value)
        product = self.pipe()(raw_values)
        data = product.data
        assert len(data) <= 1
        if data:
            return data[0]
        if self.with_error:
            quote = None
            if self.name:
                quote = "%s[%s]" % (self.name, self.domain.dump(value))
            else:
                quote = "[%s]" % self.domain.dump(value)
            raise Error("Unable to find an entity", quote)
        return None


class BuildResolveKey(Utility):

    def __init__(self, node, arcs, with_error=True):
        self.node = node
        self.arcs = arcs
        self.table = node.table
        self.with_error = with_error

    def __call__(self):
        labels = relabel(TableArc(self.table))
        name = labels[0].name if labels else None
        syntax = VoidSyntax()
        scope = RootBinding(syntax)
        state = BindingState(scope)
        seed = state.use(FreeTableRecipe(self.table), syntax)
        column_by_link = {}
        if self.arcs is not None:
            for arc in self.arcs:
                if isinstance(arc, ColumnArc) and arc.link is not None:
                    column_by_link[arc.link] = arc
        identity_arcs = localize(self.node)
        if identity_arcs is None:
            raise Error("Expected a table with identity")
        count = itertools.count()
        def chain_arc(arc, scope):
            images = []
            recipe = prescribe(arc, scope)
            binding = state.use(recipe, syntax, scope=scope)
            identity_arcs = localize(arc.target)
            if identity_arcs:
                fields = []
                for identity_arc in identity_arcs:
                    arc_images, arc_field = chain_arc(identity_arc, binding)
                    images.extend(arc_images)
                    fields.append(arc_field)
                field = IdentityDomain(fields)
            else:
                item = FormulaBinding(scope,
                                      PlaceholderSig(next(count)),
                                      binding.domain,
                                      syntax)
                images.append((item, binding))
                field = binding.domain
            return images, field
        images = []
        fields = []
        for arc in identity_arcs:
            if arc in column_by_link:
                arc = column_by_link[arc]
            arc_images, arc_field = chain_arc(arc, seed)
            images.extend(arc_images)
            fields.append(arc_field)
        identity_domain = IdentityDomain(fields)
        scope = LocateBinding(scope, seed, images, None, syntax)
        state.push_scope(scope)
        columns = []
        if self.table.primary_key is not None:
            columns = self.table.primary_key.origin_columns
        else:
            for key in self.table.unique_keys:
                if key.is_partial:
                    continue
                if all(not column.is_nullable
                       for column in key.origin_columns):
                    rcolumns = key.origin_columns
                    break
        if not columns:
            raise Error("Table does not have a primary key")
        elements = []
        for column in columns:
            binding = state.use(ColumnRecipe(column), syntax)
            elements.append(binding)
        fields = [decorate(element) for element in elements]
        domain = RecordDomain(fields)
        scope = SelectionBinding(scope, elements, domain, syntax)
        binding = Select.__invoke__(scope, state)
        domain = ListDomain(binding.domain)
        binding = CollectBinding(state.root, binding, domain, syntax)
        pipe =  translate(binding)
        return ResolveKeyPipe(name, columns, identity_domain, pipe,
                              self.with_error)


class ExecuteUpdatePipe:

    def __init__(self, table, input_columns, key_columns,
                 output_columns, sql):
        assert isinstance(table, TableEntity)
        assert isinstance(input_columns, listof(ColumnEntity))
        assert isinstance(key_columns, listof(ColumnEntity))
        assert isinstance(output_columns, listof(ColumnEntity))
        assert isinstance(sql, str)
        self.table = table
        self.input_columns = input_columns
        self.key_columns = key_columns
        self.output_columns = output_columns
        self.sql = sql
        self.input_converts = [scramble(column.domain)
                               for column in input_columns]
        self.key_converts = [scramble(column.domain)
                             for column in key_columns]
        self.output_converts = [unscramble(column.domain)
                                for column in output_columns]

    def __call__(self, key_row, row):
        key_row = tuple(convert(item)
                        for item, convert in zip(key_row, self.key_converts))
        row = tuple(convert(item)
                    for item, convert in zip(row, self.input_converts))
        if not row:
            return key_row
        if not context.env.can_write:
            raise PermissionError("No write permissions")
        with transaction() as connection:
            cursor = connection.cursor()
            cursor.execute(self.sql, row+key_row)
            rows = cursor.fetchall()
            if len(rows) != 1:
                raise Error("Unable to locate the updated row")
            [row] = rows
        return row


class BuildExecuteUpdate(Utility):

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
        sql = serialize_update(table, self.columns, returning_columns,
                               returning_columns)
        return ExecuteUpdatePipe(table, self.columns, returning_columns,
                                 returning_columns, sql)


class ProduceMerge(Act):

    adapt(MergeCmd, ProduceAction)

    def __call__(self):
        with transaction() as connection:
            product = act(self.command.feed, self.action)
            extract_node = BuildExtractNode.__invoke__(product.meta)
            extract_table = BuildExtractTable.__invoke__(
                    extract_node.node, extract_node.arcs)
            extract_identity = BuildExtractIdentity.__invoke__(
                    extract_node.node, extract_node.arcs)
            resolve_key = BuildResolveKey.__invoke__(
                    extract_node.node, extract_node.arcs, False)
            extract_table_for_update = BuildExtractTable.__invoke__(
                    extract_identity.node, extract_identity.arcs)
            execute_insert = BuildExecuteInsert.__invoke__(
                    extract_table.table, extract_table.columns)
            execute_update = BuildExecuteUpdate.__invoke__(
                    extract_table_for_update.table,
                    extract_table_for_update.columns)
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
                    row = extract_node(record)
                    update_id, update_row = extract_identity(row)
                    key = resolve_key(update_id)
                    if key is not None:
                        row = extract_table_for_update(update_row)
                        key = execute_update(key, row)
                    else:
                        row = extract_table(row)
                        key = execute_insert(row)
                    row = resolve_identity(key)
                except Error as exc:
                    if extract_node.is_list:
                        message = "While merging record #%s" % (idx+1)
                    else:
                        message = "While merging a record"
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


