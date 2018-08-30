#
# Copyright (c) 2006-2016, Prometheus Research, LLC
#


from ....core.util import to_name
from ....core.adapter import adapt
from ....core.error import Error, act_guard
from ....core.connect import transaction
from ....core.domain import UntypedDomain, RecordDomain, Value, Product
from ....core.cmd.embed import Embed
from ....core.cmd.act import Act, ProduceAction, act
from ....core.tr.binding import VoidBinding
from ....core.tr.decorate import decorate
from .command import WithCmd


class ProduceWith(Act):

    adapt(WithCmd, ProduceAction)

    def __call__(self):
        with transaction():
            input = act(self.command.record, self.action)
            if not (isinstance(input.domain, RecordDomain) or
                    isinstance(input.data, dict)):
                with act_guard(self.command.record):
                    raise Error("Expected a record value")
            if input.data is not None:
                environment = self.action.environment.copy()
                if isinstance(input.data, dict):
                    for key, value in sorted(input.data.items()):
                        try:
                            if not isinstance(key, str):
                                raise TypeError
                            name = to_name(key)
                            value = Embed.__invoke__(value)
                        except TypeError:
                            with act_guard(self.command.record):
                                raise Error("Expected a record value")
                        environment[key] = value
                else:
                    for idx, field in enumerate(input.domain.fields):
                        tag = getattr(field, 'tag')
                        if tag is not None:
                            name = to_name(tag)
                            value = Value(field.domain, input.data[idx])
                            environment[name] = value
                action = self.action.clone(environment=environment)
                return act(self.command.body, action)
            else:
                meta = decorate(VoidBinding())
                meta = meta.clone(domain=UntypedDomain())
                return Product(meta, None)


