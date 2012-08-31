#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ....core.adapter import call
from ....core.classify import normalize, classify
from ....core.model import HomeNode, TableArc
from ....core.entity import TableEntity
from ....core.tr.error import BindError
from ....core.tr.lookup import lookup_command
from ....core.tr.decorate import decorate
from ....core.tr.binding import QueryBinding, SegmentBinding, CommandBinding
from ....core.tr.syntax import IdentifierSyntax
from ....core.tr.fn.bind import BindCommand
from ....core.cmd.command import DefaultCmd
from ..cmd.command import InsertCmd, UpdateCmd, DeleteCmd, TruncateCmd


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


class BindTruncate(BindCommand):

    call('truncate')

    def expand(self, op):
        if not isinstance(op, IdentifierSyntax):
            raise BindError("an identifier is expected", op.mark)
        signature = (normalize(op.value), None)
        arc_by_signature = dict(((label.name, label.arity), label.arc)
                                for label in classify(HomeNode()))
        if signature not in arc_by_signature:
            raise BindError("unknown table", op.mark)
        arc = arc_by_signature[signature]
        if not isinstance(arc, TableArc):
            raise BindError("a table is expected", op.mark)
        table = arc.table
        command = TruncateCmd(table)
        return CommandBinding(self.state.scope, command, self.syntax)


