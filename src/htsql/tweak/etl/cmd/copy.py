#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ....core.adapter import adapt
from ....core.error import Error
from ....core.context import context
from ....core.connect import transaction
from ....core.domain import Product
from ....core.cmd.act import Act, ProduceAction, SafeProduceAction, act
from ....core.tr.binding import VoidBinding
from ....core.tr.decorate import decorate
from .command import CopyCmd
from .insert import BuildExtractNode, BuildExtractTable, BuildExecuteInsert


class ProduceCopy(Act):

    adapt(CopyCmd, ProduceAction)

    def __call__(self):
        copy_limit = context.app.tweak.etl.copy_limit
        limit = copy_limit
        offset = 0
        with transaction() as connection:
            while True:
                action = self.action.clone_to(SafeProduceAction)
                action = action.clone(cut=limit, offset=offset)
                product = act(self.command.feed, action)
                extract_node = BuildExtractNode.__invoke__(product.meta)
                extract_table = BuildExtractTable.__invoke__(
                        extract_node.node, extract_node.arcs)
                execute_insert = BuildExecuteInsert.__invoke__(
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
                        execute_insert(
                            extract_table(
                                extract_node(record)))
                    except Error, exc:
                        if extract_node.is_list:
                            message = "While copying record #%s" \
                                    % (offset+idx+1)
                        else:
                            message = "While copying a record"
                        quote = record_domain.dump(record)
                        exc.wrap(message, quote)
                        raise
                if not product or not extract_node.is_list:
                    break
                offset += copy_limit
            meta = decorate(VoidBinding())
            data = None
            return Product(meta, data)


