#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ....core.util import listof, tupleof, maybe
from ....core.cmd.command import Command
from ....core.entity import TableEntity



class ETLCmd(Command):

    def __init__(self, feed):
        assert isinstance(feed, Command)
        self.feed = feed


class CopyCmd(ETLCmd):
    pass


class InsertCmd(ETLCmd):
    pass


class MergeCmd(ETLCmd):
    pass


class UpdateCmd(ETLCmd):
    pass


class CloneCmd(ETLCmd):
    pass


class DeleteCmd(ETLCmd):
    pass


class TruncateCmd(Command):

    def __init__(self, table):
        assert isinstance(table, TableEntity)
        self.table = table


class DoCmd(Command):

    def __init__(self, blocks):
        assert isinstance(blocks, listof(tupleof(maybe(str), Command)))
        self.blocks = blocks


class IfCmd(Command):

    def __init__(self, tests, values, else_value=None):
        assert isinstance(tests, listof(Command))
        assert isinstance(values, listof(Command))
        assert len(tests) == len(values)
        assert isinstance(else_value, maybe(Command))
        self.tests = tests
        self.values = values
        self.else_value = else_value


class ForCmd(Command):

    def __init__(self, name, iterator, body):
        assert isinstance(name, str)
        assert isinstance(iterator, Command)
        assert isinstance(body, Command)
        self.name = name
        self.iterator = iterator
        self.body = body


class WithCmd(Command):

    def __init__(self, record, body):
        assert isinstance(record, Command)
        assert isinstance(body, Command)
        self.record = record
        self.body = body


