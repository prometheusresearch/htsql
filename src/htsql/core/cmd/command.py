#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ..util import Printable, maybe, dictof, oneof
from ..fmt.format import Format
from ..syn.syntax import Syntax
from ..error import Mark, EmptyMark


class Command(object):

    def __init__(self, mark):
        assert isinstance(mark, Mark)
        self.mark = mark


class UniversalCmd(Command):

    def __init__(self, query):
        assert isinstance(query, (str, unicode))
        super(UniversalCmd, self).__init__(EmptyMark())
        self.query = query


class DefaultCmd(Command):

    def __init__(self, syntax):
        assert isinstance(syntax, Syntax)
        super(DefaultCmd, self).__init__(syntax.mark)
        self.syntax = syntax


class SkipCmd(Command):
    pass


class FetchCmd(Command):

    def __init__(self, syntax, mark):
        assert isinstance(syntax, Syntax)
        super(FetchCmd, self).__init__(mark)
        self.syntax = syntax


class FormatCmd(Command):

    def __init__(self, feed, format, mark):
        assert isinstance(feed, Command)
        assert isinstance(format, Format)
        super(FormatCmd, self).__init__(mark)
        self.format = format
        self.feed = feed


class SQLCmd(Command):

    def __init__(self, feed, mark):
        assert isinstance(feed, Command)
        super(SQLCmd, self).__init__(mark)
        self.feed = feed


