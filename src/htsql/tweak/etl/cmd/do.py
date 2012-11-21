#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ....core.adapter import Adapter, adapt
from ....core.connect import transaction
from ....core.domain import IdentityDomain
from ....core.cmd.command import DefaultCmd
from ....core.cmd.act import Act, ProduceAction, act
from ....core.tr.bind import BindingState
from ....core.tr.binding import LiteralRecipe, IdentityRecipe, ClosedRecipe
from .command import DoCmd


class ProduceDo(Act):

    adapt(DoCmd, ProduceAction)

    def __call__(self):
        environment = self.action.environment.copy()
        product = None
        with transaction():
            for reference, command in self.command.blocks:
                action = self.action.clone(environment=environment)
                product = act(command, action)
                if reference is not None:
                    environment[reference] = product
        return product


