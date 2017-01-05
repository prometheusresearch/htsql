#
# Copyright (c) 2006-2014, Prometheus Research, LLC
#


from ....core.adapter import adapt
from ....core.error import Error, act_guard
from ....core.connect import transaction
from ....core.domain import UntypedDomain, ListDomain, RecordDomain, Value, Product
from ....core.cmd.act import Act, ProduceAction, act
from ....core.tr.binding import VoidBinding
from ....core.tr.decorate import decorate
from .command import ForCmd


class ProduceFor(Act):

    adapt(ForCmd, ProduceAction)

    def __call__(self):
        output = []
        meta = None
        with transaction():
            input = act(self.command.iterator, self.action)
            if not (isinstance(input.domain, ListDomain) or
                    isinstance(input.domain, RecordDomain) or
                    input.data is None):
                with act_guard(self.command.iterator):
                    raise Error("Expected a list value")
            if input:
                if isinstance(input.domain, ListDomain):
                    values = [
                            Value(input.domain.item_domain, item)
                            for item in input.data]
                else:
                    values = [
                            Value(field.domain, item)
                            for field, item
                                in zip(input.domain.fields, input.data)]
                for value in values:
                    environment = self.action.environment.copy()
                    environment[self.command.name] = value
                    action = self.action.clone(environment=environment)
                    product = act(self.command.body, action)
                    data = product.data
                    if data is not None:
                        if meta is None:
                            meta = product.meta
                        elif product.domain != meta.domain:
                            raise Error("Unexpected loop body type",
                                        " expected %s; got %s"
                                        % (meta.domain, product.domain))
                        output.append(data)
        if meta is None:
            meta = decorate(VoidBinding())
            meta = meta.clone(domain=UntypedDomain())
        meta = meta.clone(domain=ListDomain(meta.domain))
        return Product(meta, output)


