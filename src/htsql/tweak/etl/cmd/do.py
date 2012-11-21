#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ....core.adapter import Adapter, adapt
from ....core.connect import transaction
from ....core.domain import IdentityDomain
from ....core.cmd.command import DefaultCmd
from ....core.cmd.act import Act, ProduceAction, act
from ....core.tr.bind import BindingState
from ....core.tr.embed import embed
from ....core.tr.binding import LiteralRecipe, IdentityRecipe, ClosedRecipe
from .command import DoCmd


class ProduceDo(Act):

    adapt(DoCmd, ProduceAction)

    def __call__(self):
        environment = []
        parameters = self.action.parameters
        if parameters is not None:
            if isinstance(parameters, dict):
                for name in sorted(parameters):
                    value = parameters[name]
                    if isinstance(name, str):
                        name = name.decode('utf-8')
                    recipe = embed(value)
                    environment.append((name, recipe))
            else:
                environment = self.parameters[:]
        product = None
        with transaction():
            for reference, command in self.command.blocks:
                action = self.action.clone(parameters=environment)
                product = act(command, action)
                if reference is not None:
                    if (isinstance(product.meta.domain, IdentityDomain) and
                            product.data is not None):
                        def convert(domain, data):
                            items = []
                            for element, item in zip(domain.labels, data):
                                if isinstance(element, IdentityDomain):
                                    item = convert(element, item)
                                else:
                                    item = LiteralRecipe(item, element)
                                items.append(item)
                            return IdentityRecipe(items)
                        literal = convert(product.meta.domain, product.data)
                    else:
                        literal = LiteralRecipe(product.data,
                                                product.meta.domain)
                    recipe = ClosedRecipe(literal)
                    environment.append((reference, recipe))
        return product


