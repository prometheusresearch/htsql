#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql import HTSQL
from htsql.context import context
from htsql.adapter import adapts, named
from htsql.cmd.command import ProducerCmd, DefaultCmd
from htsql.cmd.act import Act, ProduceAction, act
from htsql.tr.fn.bind import BindCommand
from htsql.tr.signature import UnarySig
from htsql.tr.bind import bind
from htsql.tr.syntax import QuerySyntax, SegmentSyntax
from htsql.tr.binding import CommandBinding
from htsql.tr.error import BindError
from htsql.tr.lookup import lookup_command
import weakref


def get_slave_app():
    slave_app = context.app.tweak.meta.cached_slave_app
    if slave_app is None:
        master = weakref.ref(context.app)
        slave_app = HTSQL(None, {'tweak.meta.slave': {'master': master}})
        context.app.tweak.meta.cached_slave_app = slave_app
    return slave_app


class MetaCmd(ProducerCmd):

    def __init__(self, syntax):
        self.syntax = syntax


class BindMeta(BindCommand):

    named('meta')
    signature = UnarySig

    def expand(self, op):
        if not isinstance(op, SegmentSyntax):
            raise BindError("a segment is required", op.mark)
        op = QuerySyntax(op, op.mark)
        command = MetaCmd(op)
        return CommandBinding(self.state.scope, command, self.syntax)


class ProduceMeta(Act):

    adapts(MetaCmd, ProduceAction)

    def __call__(self):
        master_app = context.app
        slave_app = get_slave_app()
        context.switch(master_app, slave_app)
        try:
            binding = bind(self.command.syntax)
            command = lookup_command(binding)
            if command is None:
                command = DefaultCmd(binding)
            product = act(command, self.action)
        finally:
            context.switch(slave_app, master_app)
        return product


