#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ..util import Printable, maybe, dictof, oneof
from ..fmt.format import Format
from ..syn.syntax import Syntax


class Command(object):
    pass


class UniversalCmd(Command):

    def __init__(self, query):
        assert isinstance(query, str)
        self.query = query


class DefaultCmd(Command):

    def __init__(self, syntax):
        assert isinstance(syntax, Syntax)
        self.syntax = syntax


class SkipCmd(Command):
    pass


class FetchCmd(Command):

    def __init__(self, syntax):
        assert isinstance(syntax, Syntax)
        self.syntax = syntax


class FormatCmd(Command):

    def __init__(self, feed, format):
        assert isinstance(feed, Command)
        assert isinstance(format, Format)
        self.feed = feed
        self.format = format


class SQLCmd(Command):

    def __init__(self, feed):
        assert isinstance(feed, Command)
        self.feed = feed


