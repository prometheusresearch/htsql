#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ....core.util import listof
from ....core.cmd.command import Command, ProducerCmd
from ....core.entity import TableEntity



class ETLCmd(ProducerCmd):

    def __init__(self, feed):
        assert isinstance(feed, Command)
        self.feed = feed


class InsertCmd(ETLCmd):
    pass


class MergeCmd(ETLCmd):
    pass


class UpdateCmd(ETLCmd):
    pass


class DeleteCmd(ETLCmd):
    pass


class TruncateCmd(ProducerCmd):

    def __init__(self, table):
        assert isinstance(table, TableEntity)
        self.table = table


class DoCmd(ProducerCmd):

    def __init__(self, commands):
        assert isinstance(commands, listof(Command))
        self.commands = commands


