#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ...core.context import context
from ...core.cache import once
from ...core.adapter import adapt, call
from ...core.error import Error
from ...core.cmd.command import Command
from ...core.cmd.act import Act, Action, RenderAction, act
from ...core.cmd.summon import Summon, recognize
import weakref


@once
def get_slave_app():
    from htsql import HTSQL
    master = weakref.ref(context.app)
    slave = HTSQL(None, {'tweak.meta.slave': {'master': master}})
    return slave


class MetaCmd(Command):

    def __init__(self, command):
        self.command = command


class SummonMeta(Summon):

    call('meta')

    def __call__(self):
        if len(self.arguments) != 1:
            raise Error("Expected 1 argument")
        [syntax] = self.arguments
        slave_app = get_slave_app()
        with slave_app:
            command = recognize(syntax)
        return MetaCmd(command)


class ActMeta(Act):

    adapt(MetaCmd, Action)

    @classmethod
    def __matches__(component, dispatch_key):
        command_type, action_type = dispatch_key
        if isinstance(action_type, RenderAction):
            return False
        return super(ActMeta, component).__matches__(dispatch_key)

    def __call__(self):
        can_read = context.env.can_read
        can_write = context.env.can_write
        slave_app = get_slave_app()
        with slave_app:
            with context.env(can_read=context.env.can_read and can_read,
                             can_write=context.env.can_write and can_write):
                return act(self.command.command, self.action)


