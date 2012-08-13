#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ....core.adapter import call
from ....core.tr.error import BindError
from ....core.tr.lookup import lookup_command
from ....core.tr.decorate import decorate
from ....core.tr.binding import QueryBinding, SegmentBinding, CommandBinding
from ....core.tr.fn.bind import BindCommand
from ....core.cmd.command import DefaultCmd
from ..cmd.command import InsertCmd, UpdateCmd, DeleteCmd


class BindETL(BindCommand):

    cmd = None

    def expand(self, op):
        op = self.state.bind(op)
        feed = lookup_command(op)
        if feed is None:
            if not isinstance(op, SegmentBinding):
                raise BindError("a segment is expected", op.mark)
            profile = decorate(op)
            binding = QueryBinding(self.state.root, op, profile, op.syntax)
            feed = DefaultCmd(binding)
        command = self.cmd(feed)
        return CommandBinding(self.state.scope, command, self.syntax)


class BindInsert(BindETL):

    call('insert')
    cmd = InsertCmd


class BindUpdate(BindETL):

    call('update')
    cmd = UpdateCmd


class BindDelete(BindETL):

    call('delete')
    cmd = DeleteCmd


