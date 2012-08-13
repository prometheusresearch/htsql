#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ....core.cmd.command import Command


class ETLCmd(Command):

    def __init__(self, feed):
        assert isinstance(feed, Command)
        self.feed = feed


class InsertCmd(ETLCmd):
    pass


class UpdateCmd(ETLCmd):
    pass


class DeleteCmd(ETLCmd):
    pass


