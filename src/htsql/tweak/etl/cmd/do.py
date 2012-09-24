#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ....core.adapter import Adapter, adapt
from ....core.connect import transaction
from ....core.domain import RecordDomain
from ....core.cmd.act import Act, ProduceAction, produce
from ....core.cmd.fetch import Product
from ....core.tr.binding import VoidBinding
from ....core.tr.decorate import decorate
from .command import DoCmd


class ProduceDo(Act):

    adapt(DoCmd, ProduceAction)

    def __call__(self):
        product = None
        with transaction():
            for command in self.command.commands:
                product = produce(command)
        return product


