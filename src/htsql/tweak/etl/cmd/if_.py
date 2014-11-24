#
# Copyright (c) 2006-2014, Prometheus Research, LLC
#


from ....core.adapter import adapt
from ....core.error import Error, act_guard
from ....core.connect import transaction
from ....core.domain import UntypedDomain, BooleanDomain, Product
from ....core.cmd.act import Act, ProduceAction, act
from ....core.tr.binding import VoidBinding
from ....core.tr.decorate import decorate
from .command import IfCmd


class ProduceIf(Act):

    adapt(IfCmd, ProduceAction)

    def __call__(self):
        with transaction():
            for test, value in zip(self.command.tests, self.command.values):
                product = act(test, self.action)
                if not isinstance(product.domain, BooleanDomain):
                    with act_guard(test):
                        raise Error("Expected boolean value; got %s"
                                    % product.domain)
                if product.data:
                    return act(value, self.action)
            if self.command.else_value is not None:
                return act(self.command.else_value, self.action)
        meta = decorate(VoidBinding())
        meta = meta.clone(domain=UntypedDomain())
        return Product(meta, None)


