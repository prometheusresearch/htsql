#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ....core.adapter import adapt
from ....core.error import Error
from ....core.connect import transaction
from ....core.domain import Product
from ....core.cmd.act import Act, ProduceAction, act
from .command import UpdateCmd
from .insert import BuildExtractNode, BuildExtractTable, BuildResolveIdentity
from .merge import BuildResolveKey, BuildExecuteUpdate


class ProduceUpdate(Act):

    adapt(UpdateCmd, ProduceAction)

    def __call__(self):
        with transaction() as connection:
            product = act(self.command.feed, self.action)
            extract_node = BuildExtractNode.__invoke__(product.meta,
                    with_id=True, with_fields=True)
            resolve_key = BuildResolveKey.__invoke__(
                    extract_node.node, None)
            extract_table = BuildExtractTable.__invoke__(
                    extract_node.node, extract_node.arcs)
            execute_update = BuildExecuteUpdate.__invoke__(
                    extract_table.table,
                    extract_table.columns)
            resolve_identity = BuildResolveIdentity.__invoke__(
                    execute_update.table, execute_update.output_columns,
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
                    key = execute_update(key, row)
                    row = resolve_identity(key)
                except Error as error:
                    if extract_node.is_list:
                        message = "While updating record #%s" % (idx+1)
                    else:
                        message = "While updating a record"
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


