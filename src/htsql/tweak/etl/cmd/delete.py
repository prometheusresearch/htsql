#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ....core.util import listof
from ....core.adapter import Utility, adapt
from ....core.error import Error, PermissionError
from ....core.context import context
from ....core.entity import TableEntity, ColumnEntity
from ....core.connect import transaction, scramble
from ....core.domain import Product
from ....core.cmd.act import Act, ProduceAction, act
from ....core.tr.binding import VoidBinding
from ....core.tr.decorate import decorate
from .command import DeleteCmd
from .insert import BuildExtractNode
from .merge import BuildResolveKey
from ..tr.dump import serialize_delete
import itertools


class ExecuteDeletePipe(object):

    def __init__(self, table, key_columns, sql):
        assert isinstance(table, TableEntity)
        assert isinstance(key_columns, listof(ColumnEntity))
        assert isinstance(sql, str)
        self.table = table
        self.key_columns = key_columns
        self.sql = sql
        self.key_converts = [scramble(column.domain)
                             for column in key_columns]

    def __call__(self, key_row):
        key_row = tuple(convert(item)
                        for item, convert in zip(key_row, self.key_converts))
        if not context.env.can_write:
            raise PermissionError("No write permissions")
        with transaction() as connection:
            cursor = connection.cursor()
            cursor.execute(self.sql.encode('utf-8'), key_row)


class BuildExecuteDelete(Utility):

    def __init__(self, table):
        assert isinstance(table, TableEntity)
        self.table = table

    def __call__(self):
        table = self.table
        key_columns = []
        if table.primary_key is not None:
            key_columns = table.primary_key.origin_columns
        else:
            for key in table.unique_keys:
                if key.is_partial:
                    continue
                if all(not column.is_nullable
                       for column in key.origin_columns):
                    key_columns = key.origin_columns
                    break
        if not key_columns:
            raise Error("Table does not have a primary key")
        sql = serialize_delete(table, key_columns)
        return ExecuteDeletePipe(table, key_columns, sql)


class ProduceDelete(Act):

    adapt(DeleteCmd, ProduceAction)

    def __call__(self):
        with transaction() as connection:
            product = act(self.command.feed, self.action)
            extract_node = BuildExtractNode.__invoke__(product.meta,
                    with_id=True, with_fields=False)
            resolve_key = BuildResolveKey.__invoke__(
                    extract_node.node, None)
            execute_delete = BuildExecuteDelete.__invoke__(
                    extract_node.node.table)
            meta = decorate(VoidBinding())
            data = None
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
                    id_value, row = extract_node(record)
                    key = resolve_key(id_value)
                    execute_delete(key)
                except Error as error:
                    if extract_node.is_list:
                        message = "While deleting record #%s" % (idx+1)
                    else:
                        message = "While deleting a record"
                    quote = record_domain.dump(record)
                    error.wrap(message, quote)
                    raise
            return Product(meta, data)


