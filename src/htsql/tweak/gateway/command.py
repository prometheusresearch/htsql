#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ...core.adapter import adapt
from ...core.error import Error
from ...core.cmd.command import Command
from ...core.cmd.act import Act, Action, RenderAction, act
from ...core.cmd.summon import Summon, recognize


class GatewayCmd(Command):

    def __init__(self, instance, command, mark):
        super(GatewayCmd, self).__init__(mark)
        self.instance = instance
        self.command = command


class SummonGateway(Summon):

    instance = None

    def __call__(self):
        if len(self.arguments) != 1:
            raise Error("expected 1 argument", self.syntax.mark)
        [syntax] = self.arguments
        with self.instance:
            command = recognize(syntax)
        return GatewayCmd(self.instance, command, self.syntax.mark)


class ActGateway(Act):

    adapt(GatewayCmd, Action)

    @classmethod
    def __matches__(component, dispatch_key):
        command_type, action_type = dispatch_key
        if isinstance(action_type, RenderAction):
            return False
        return super(ActGateway, component).__matches__(dispatch_key)

    def __call__(self):
        with self.command.instance:
            return act(self.command.command, self.action)


