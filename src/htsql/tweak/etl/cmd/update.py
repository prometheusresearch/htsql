#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ....core.adapter import adapt
from ....core.error import BadRequestError
from ....core.connect import transaction
from ....core.cmd.retrieve import Product
from ....core.cmd.act import Act, ProduceAction, produce
from .command import UpdateCmd
from .insert import BuildExtractNode, BuildExtractTable, BuildResolveIdentity
from .merge import BuildResolveKey, BuildExecuteUpdate


class ProduceUpdate(Act):

    adapt(UpdateCmd, ProduceAction)

    def __call__(self):
        with transaction() as connection:
            product = produce(self.command.feed)
            extract_node = BuildExtractNode.__invoke__(product.meta,
                    with_id=True, with_fields=True)
            resolve_key = BuildResolveKey.__invoke__(
                    extract_node.node.table)
            extract_table = BuildExtractTable.__invoke__(
                    extract_node.node, extract_node.arcs)
            execute_update = BuildExecuteUpdate.__invoke__(
                    extract_table.table,
                    extract_table.columns)
            resolve_identity = BuildResolveIdentity.__invoke__(
                    execute_update.table, execute_update.output_columns)
            meta = resolve_identity.profile
            data = []
            for record in product.data:
                if record is None:
                    continue
                key_id, row = extract_node(record)
                key = resolve_key(key_id)
                row = extract_table(row)
                key = execute_update(key, row)
                row = resolve_identity(key)
                data.append(row)
            return Product(meta, data)


