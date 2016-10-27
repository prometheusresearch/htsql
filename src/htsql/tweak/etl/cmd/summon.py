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
from ..cmd.command import (CopyCmd, InsertCmd, MergeCmd, UpdateCmd, CloneCmd,
        DeleteCmd, TruncateCmd, DoCmd, IfCmd, ForCmd, WithCmd)


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


class SummonClone(SummonETL):

    call('clone')
    cmd = CloneCmd


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


class SummonIf(Summon):

    call('if')

    def __call__(self):
        if len(self.arguments) < 2:
            raise Error("expected 2 or more arguments")
        tests = []
        values = []
        else_value = None
        arguments = self.arguments[:]
        while len(arguments) > 1:
            test = recognize(arguments.pop(0))
            value = recognize(arguments.pop(0))
            tests.append(test)
            values.append(value)
        if arguments:
            else_value = recognize(arguments.pop(0))
        return IfCmd(tests, values, else_value)


class SummonWhen(Summon):

    call('when')

    def __call__(self):
        if len(self.arguments) != 2:
            raise Error("expected 2 arguments")
        value = recognize(self.arguments[0])
        test = recognize(self.arguments[1])
        return IfCmd([test], [value], None)


class SummonFor(Summon):

    call('for')

    def __call__(self):
        if len(self.arguments) != 2:
            raise Error("expected 2 arguments")
        head, body = self.arguments
        if not isinstance(head, AssignSyntax):
            with recognize_guard(head):
                raise Error("Expected an assignment expression")
        specifier = head.larm
        if specifier.reference is None:
            with recognize_guard(specifier):
                raise Error("Expected a reference")
        name = specifier.reference.name
        iterator = recognize(head.rarm)
        body = recognize(body)
        return ForCmd(name, iterator, body)


class SummonWith(Summon):

    call('with')

    def __call__(self):
        if len(self.arguments) != 2:
            raise Error("expected 2 arguments")
        record, body = self.arguments
        record = recognize(record)
        body = recognize(body)
        return WithCmd(record, body)


