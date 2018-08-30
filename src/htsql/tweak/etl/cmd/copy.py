#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ....core.util import listof
from ....core.adapter import adapt, Utility
from ....core.error import Error, PermissionError
from ....core.context import context
from ....core.connect import transaction
from ....core.entity import TableEntity, ColumnEntity
from ....core.domain import Product
from ....core.cmd.act import Act, ProduceAction, SafeProduceAction, act
from ....core.tr.binding import VoidBinding
from ....core.tr.decorate import decorate
from .command import CopyCmd
from .insert import BuildExtractNode, BuildExtractTable
import tempfile


class CollectCopyPipe(object):

    def __init__(self, table, columns):
        assert isinstance(table, TableEntity)
        assert isinstance(columns, listof(ColumnEntity))
        self.table = table
        self.columns = columns
        self.dumps = [column.domain.dump for column in columns]
        self.stream = tempfile.TemporaryFile()

    def __call__(self, row, str=str):
        self.stream.write(
            "\t".join([
                str(item).encode('utf-8')
                        .replace('\\', '\\\\')
                        .replace('\n', '\\n')
                        .replace('\r', '\\r')
                        .replace('\t', '\\t')
                if item is not None else '\\N'
                for item in row]) + '\n')

    def copy(self):
        if not self.stream.tell():
            return
        self.stream.seek(0)
        if not context.env.can_write:
            raise PermissionError("No write permissions")
        with transaction() as connection:
            cursor = connection.cursor()
            with cursor.guard:
                cursor = cursor.cursor
                cursor.copy_from(
                        self.stream,
                        table='"%s"' % self.table.name.encode('utf-8'),
                        columns=['"%s"' % column.name.encode('utf-8')
                                 for column in self.columns])


class BuildCollectCopy(Utility):

    def __init__(self, table, columns):
        assert isinstance(table, TableEntity)
        assert isinstance(columns, listof(ColumnEntity))
        self.table = table
        self.columns = columns

    def __call__(self):
        return CollectCopyPipe(self.table, self.columns)


class ProduceCopy(Act):

    adapt(CopyCmd, ProduceAction)

    def __call__(self):
        batch = context.app.tweak.etl.copy_limit
        with transaction() as connection:
            action = self.action.clone(batch=batch)
            product = act(self.command.feed, action)
            extract_node = BuildExtractNode.__invoke__(product.meta)
            extract_table = BuildExtractTable.__invoke__(
                    extract_node.node, extract_node.arcs,
                    with_cache=True)
            collect_copy = BuildCollectCopy.__invoke__(
                    extract_table.table, extract_table.columns)
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
                    collect_copy(
                        extract_table(
                            extract_node(record)))
                except Error as exc:
                    if extract_node.is_list:
                        message = "While copying record #%s" % (idx+1)
                    else:
                        message = "While copying a record"
                    quote = record_domain.dump(record)
                    exc.wrap(message, quote)
                    raise
            extract_node = None
            extract_table = None
            try:
                collect_copy.copy()
            except Error as exc:
                exc.wrap("While copying a batch of records", None)
                raise
        meta = decorate(VoidBinding())
        data = None
        return Product(meta, data)


