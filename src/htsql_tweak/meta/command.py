#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from __future__ import with_statement
from htsql import HTSQL
from htsql.context import context
from htsql.cache import once
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


@once
def get_slave_app():
    master = weakref.ref(context.app)
    slave = HTSQL(None, {'tweak.meta.slave': {'master': master}})
    return slave


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
        slave_app = get_slave_app()
        with slave_app:
            binding = bind(self.command.syntax)
            command = lookup_command(binding)
            if command is None:
                command = DefaultCmd(binding)
            product = act(command, self.action)
        return product


