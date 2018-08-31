#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ....core.adapter import adapt
from ....core.context import context
from ....core.connect import transaction
from ....core.error import PermissionError
from ....core.cmd.act import Act, ProduceAction
from ....core.domain import Product
from ....core.tr.binding import VoidBinding
from ....core.tr.decorate import decorate
from .command import TruncateCmd
from ..tr.dump import serialize_truncate


class ProduceTruncate(Act):

    adapt(TruncateCmd, ProduceAction)

    def __call__(self):
        if not context.env.can_write:
            raise PermissionError("No write permissions")
        with transaction() as connection:
            sql = serialize_truncate(self.command.table)
            meta = decorate(VoidBinding())
            data = None
            cursor = connection.cursor()
            cursor.execute(sql)
        return Product(meta, data)


