#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ....core.util import listof
from ....core.adapter import Utility, adapt
from ....core.error import BadRequestError
from ....core.entity import TableEntity, ColumnEntity
from ....core.connect import transaction, scramble
from ....core.cmd.fetch import Product
from ....core.cmd.act import Act, ProduceAction, produce
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
        assert isinstance(sql, unicode)
        self.table = table
        self.key_columns = key_columns
        self.sql = sql
        self.key_converts = [scramble(column.domain)
                             for column in key_columns]

    def __call__(self, key_row):
        key_row = tuple(convert(item)
                        for item, convert in zip(key_row, self.key_converts))
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
            raise BadRequestError("table does not have a primary key")
        sql = serialize_delete(table, key_columns)
        return ExecuteDeletePipe(table, key_columns, sql)


class ProduceDelete(Act):

    adapt(DeleteCmd, ProduceAction)

    def __call__(self):
        with transaction() as connection:
            product = produce(self.command.feed)
            extract_node = BuildExtractNode.__invoke__(product.meta,
                    with_id=True, with_fields=False)
            resolve_key = BuildResolveKey.__invoke__(
                    extract_node.node.table)
            execute_delete = BuildExecuteDelete.__invoke__(
                    extract_node.node.table)
            meta = decorate(VoidBinding())
            data = None
            if extract_node.is_list:
                records = product.data
            else:
                records = [product.data]
            for record in records:
                if record is None:
                    continue
                id_value, row = extract_node(record)
                key = resolve_key(id_value)
                if key is None:
                    raise BadRequestError("missing record")
                execute_delete(key)
            return Product(meta, data)


