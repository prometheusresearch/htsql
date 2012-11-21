#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ....core.adapter import call
from ....core.classify import normalize, classify
from ....core.model import HomeNode, TableArc
from ....core.entity import TableEntity
from ....core.cmd.summon import Summon, recognize, RecognizeError
from ....core.syn.syntax import IdentifierSyntax, AssignSyntax, ReferenceSyntax
from ..cmd.command import (InsertCmd, MergeCmd, UpdateCmd, DeleteCmd,
        TruncateCmd, DoCmd)


class SummonETL(Summon):

    cmd = None

    def __call__(self):
        if len(self.arguments) != 1:
            raise RecognizeError("expected 1 argument", self.syntax.mark)
        [syntax] = self.arguments
        feed = recognize(syntax)
        command = self.cmd(feed, self.syntax.mark)
        return command


class SummonInsert(SummonETL):

    call('insert')
    cmd = InsertCmd


class SummonMerge(SummonETL):

    call('merge')
    cmd = MergeCmd


class SummonUpdate(SummonETL):

    call('update')
    cmd = UpdateCmd


class SummonDelete(SummonETL):

    call('delete')
    cmd = DeleteCmd


class SummonTruncate(Summon):

    call('truncate')

    def __call__(self):
        if len(self.arguments) != 1:
            raise RecognizeError("expected 1 argument", self.syntax.mark)
        [syntax] = self.arguments
        if not isinstance(syntax, IdentifierSyntax):
            raise RecognizeError("an identifier is expected", syntax.mark)
        signature = (normalize(syntax.name), None)
        arc_by_signature = dict(((label.name, label.arity), label.arc)
                                for label in classify(HomeNode()))
        if signature not in arc_by_signature:
            raise RecognizeError("unknown table", syntax.mark)
        arc = arc_by_signature[signature]
        if not isinstance(arc, TableArc):
            raise RecognizeError("a table is expected", syntax.mark)
        table = arc.table
        command = TruncateCmd(table, self.syntax.mark)
        return command


class SummonDo(Summon):

    call('do')

    def __call__(self):
        if not self.arguments:
            raise RecognizeError("expected 1 or more arguments",
                                 self.syntax.mark)
        blocks = []
        for argument in self.arguments:
            name = None
            syntax = argument
            if isinstance(argument, AssignSyntax):
                specifier = argument.larm
                if specifier.reference is None:
                    raise RecognizeError("a reference is expected",
                                         specifier.mark)
                name = specifier.reference.name
                syntax = argument.rarm
            command = recognize(syntax)
            blocks.append((name, command))
        command = DoCmd(blocks, self.syntax.mark)
        return command


