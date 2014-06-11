#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ....core.adapter import call
from ....core.error import Error, recognize_guard
from ....core.classify import normalize, classify
from ....core.model import HomeNode, TableArc
from ....core.entity import TableEntity
from ....core.cmd.summon import Summon, recognize
from ....core.syn.syntax import IdentifierSyntax, AssignSyntax, ReferenceSyntax
from ..cmd.command import (CopyCmd, InsertCmd, MergeCmd, UpdateCmd, DeleteCmd,
        TruncateCmd, DoCmd)


class SummonETL(Summon):

    cmd = None

    def __call__(self):
        if len(self.arguments) != 1:
            raise Error("Expected 1 argument")
        [syntax] = self.arguments
        feed = recognize(syntax)
        command = self.cmd(feed)
        return command


class SummonCopy(SummonETL):

    call('copy')
    cmd = CopyCmd


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
            raise Error("Expected 1 argument")
        [syntax] = self.arguments
        with recognize_guard(syntax):
            if not isinstance(syntax, IdentifierSyntax):
                raise Error("Expected an identifier")
            signature = (normalize(syntax.name), None)
            arc_by_signature = dict(((label.name, label.arity), label.arc)
                                    for label in classify(HomeNode()))
            if signature not in arc_by_signature:
                raise Error("Unknown table")
            arc = arc_by_signature[signature]
            if not isinstance(arc, TableArc):
                raise Error("Expected a table")
        table = arc.table
        command = TruncateCmd(table)
        return command


class SummonDo(Summon):

    call('do')

    def __call__(self):
        if not self.arguments:
            raise Error("expected 1 or more arguments")
        blocks = []
        for argument in self.arguments:
            name = None
            syntax = argument
            if isinstance(argument, AssignSyntax):
                specifier = argument.larm
                if specifier.reference is None:
                    with recognize_guard(specifier):
                        raise Error("Expected a reference")
                name = specifier.reference.name
                syntax = argument.rarm
            command = recognize(syntax)
            blocks.append((name, command))
        command = DoCmd(blocks)
        return command


