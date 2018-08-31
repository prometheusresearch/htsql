#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ....core.util import listof
from ....core.adapter import Utility, adapt
from ....core.error import Error
from ....core.connect import transaction
from ....core.domain import ListDomain, RecordDomain, BooleanDomain, Product
from ....core.entity import ColumnEntity
from ....core.model import TableNode, ColumnArc, ChainArc
from ....core.classify import classify
from ....core.cmd.act import Act, ProduceAction, act
from ....core.syn.syntax import VoidSyntax
from ....core.tr.translate import translate
from ....core.tr.bind import BindingState, Select
from ....core.tr.binding import (RootBinding, FormulaBinding, SelectionBinding,
        SieveBinding, CollectBinding, FreeTableRecipe, ColumnRecipe)
from ....core.tr.signature import IsEqualSig, AndSig, PlaceholderSig
from ....core.tr.decorate import decorate
from ....core.tr.coerce import coerce
from .command import CloneCmd
from .insert import (BuildExtractNode, BuildExtractTable, BuildResolveIdentity,
        BuildExecuteInsert)
from .merge import BuildResolveKey


class BuildExecuteClone(Utility):

    def __init__(self, node, columns):
        assert isinstance(node, TableNode)
        assert isinstance(columns, listof(ColumnEntity))
        self.node = node
        self.columns = columns

    def __call__(self):
        table = self.node.table
        condition_columns = []
        if table.primary_key is not None:
            condition_columns = table.primary_key.origin_columns
        else:
            for key in table.unique_keys:
                if key.is_partial:
                    continue
                if all(not column.is_nullable
                       for column in key.origin_columns):
                    condition_columns = key.origin_columns
                    break
        if not condition_columns:
            raise Error("Table does not have a primary key")
        output_columns = []
        for label in classify(self.node):
            if not label.is_public:
                continue
            arc = label.arc
            if isinstance(arc, ColumnArc):
                if arc.column not in output_columns:
                    output_columns.append(arc.column)
            elif isinstance(arc, ChainArc):
                for column in arc.joins[0].origin_columns:
                    if column not in output_columns:
                        output_columns.append(column)
        syntax = VoidSyntax()
        scope = RootBinding(syntax)
        state = BindingState(scope)
        scope = state.use(FreeTableRecipe(table), syntax)
        state.push_scope(scope)
        conditions = []
        for idx, column in enumerate(condition_columns):
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
        mapping = []
        elements = []
        for column in output_columns:
            if column in self.columns:
                mapping.append((0, self.columns.index(column)))
                continue
            mapping.append((1, len(elements)))
            binding = state.use(ColumnRecipe(column), syntax)
            elements.append(binding)
        fields = [decorate(element) for element in elements]
        domain = RecordDomain(fields)
        scope = SelectionBinding(scope, elements, domain, syntax)
        binding = Select.__invoke__(scope, state)
        domain = ListDomain(binding.domain)
        binding = CollectBinding(state.root, binding, domain, syntax)
        pipe = translate(binding)
        return ExecuteClonePipe(table, output_columns, pipe, mapping)


class ExecuteClonePipe:

    def __init__(self, table, columns, pipe, mapping):
        self.table = table
        self.columns = columns
        self.pipe = pipe
        self.mapping = mapping

    def __call__(self, row, key):
        product = self.pipe()(key)
        data = product.data
        if len(data) != 1:
            raise Error("Unable to locate the inserted record")
        lines = [row, data[0]]
        output = []
        for line_index, col_index in self.mapping:
            output.append(lines[line_index][col_index])
        return tuple(output)


class ProduceClone(Act):

    adapt(CloneCmd, ProduceAction)

    def __call__(self):
        with transaction() as connection:
            product = act(self.command.feed, self.action)
            extract_node = BuildExtractNode.__invoke__(product.meta,
                    with_id=True, with_fields=True)
            resolve_key = BuildResolveKey.__invoke__(
                    extract_node.node, None)
            extract_table = BuildExtractTable.__invoke__(
                    extract_node.node, extract_node.arcs)
            execute_clone = BuildExecuteClone.__invoke__(
                    extract_node.node, extract_table.columns)
            execute_insert = BuildExecuteInsert.__invoke__(
                    execute_clone.table, execute_clone.columns)
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
                    key_id, row = extract_node(record)
                    key = resolve_key(key_id)
                    row = extract_table(row)
                    row = execute_clone(row, key)
                    key = execute_insert(row)
                    row = resolve_identity(key)
                except Error as error:
                    if extract_node.is_list:
                        message = "While cloning record #%s" % (idx+1)
                    else:
                        message = "While cloning a record"
                    quote = record_domain.dump(record)
                    error.wrap(message, quote)
                    raise
                data.append(row)
            if not extract_node.is_list:
                assert len(data) <= 1
                if data:
                    data = data[0]
                else:
                    data = None
            return Product(meta, data)


