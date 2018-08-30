#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ..adapter import Adapter, Protocol, adapt, call
from ..error import Error, recognize_guard, point, MarkRef
from ..util import to_name
from ..syn.syntax import (Syntax, SkipSyntax, FunctionSyntax, PipeSyntax,
        ApplySyntax, CollectSyntax)
from ..syn.parse import parse
from ..fmt.format import (TextFormat, HTMLFormat, RawFormat, JSONFormat,
        CSVFormat, TSVFormat, XMLFormat)
from .command import SkipCmd, FetchCmd, FormatCmd, SQLCmd, DefaultCmd


class Recognize(Adapter):

    adapt(Syntax)

    def __init__(self, syntax):
        self.syntax = syntax

    def __call__(self):
        return None


class RecognizeSkip(Recognize):

    adapt(SkipSyntax)

    def __call__(self):
        command = SkipCmd()
        return point(command, self.syntax)


class RecognizeFunction(Recognize):

    adapt(FunctionSyntax)

    def __call__(self):
        with recognize_guard(self.syntax):
            command = Summon.__invoke__(self.syntax)
            return point(command, self.syntax.identifier)


class RecognizePipe(Recognize):

    adapt(PipeSyntax)

    def __call__(self):
        if self.syntax.is_flow:
            return super(RecognizePipe, self).__call__()
        with recognize_guard(self.syntax):
            command = Summon.__invoke__(self.syntax)
            return point(command, self.syntax.identifier)


class RecognizeCollect(Recognize):

    adapt(CollectSyntax)

    def __call__(self):
        return Recognize.__invoke__(self.syntax.arm)


class Summon(Protocol):

    @classmethod
    def __dispatch__(interface, syntax):
        assert isinstance(syntax, ApplySyntax)
        name = syntax.name
        if name.isalpha():
            name = to_name(name)
        return name

    @classmethod
    def __matches__(component, dispatch_key):
        return any(name == dispatch_key for name in component.__names__)

    def __init__(self, syntax):
        self.syntax = syntax
        self.name = syntax.name
        self.arguments = syntax.arguments

    def __call__(self):
        return None


class SummonFetch(Summon):

    call('fetch',
         'retrieve')

    def __call__(self):
        if len(self.arguments) != 1:
            raise Error("Expected 1 argument")
        [syntax] = self.arguments
        return FetchCmd(syntax)


class SummonFormat(Summon):

    format = None

    def __call__(self):
        if len(self.arguments) != 1:
            raise Error("Expected 1 argument")
        [syntax] = self.arguments
        feed = recognize(syntax)
        format = self.format()
        return FormatCmd(feed, format)


class SummonTxt(SummonFormat):

    call('txt')
    format = TextFormat


class SummonHTML(SummonFormat):

    call('html')
    format = HTMLFormat


class SummonRaw(SummonFormat):

    call('raw')
    format = RawFormat


class SummonJSON(SummonFormat):

    call('json')
    format = JSONFormat


class SummonCSV(SummonFormat):

    call('csv')
    format = CSVFormat


class SummonTSV(SummonFormat):

    call('tsv')
    format = TSVFormat


class SummonXML(SummonFormat):

    call('xml')
    format = XMLFormat


class SummonSQL(Summon):

    call('sql')

    def __call__(self):
        if len(self.arguments) != 1:
            raise Error("Expected 1 argument")
        [syntax] = self.arguments
        feed = recognize(syntax)
        return SQLCmd(feed)


def recognize(syntax):
    assert isinstance(syntax, (Syntax, str))
    if not isinstance(syntax, Syntax):
        syntax = parse(syntax)
    command = Recognize.__invoke__(syntax)
    if command is None:
        command = DefaultCmd(syntax)
        mark = MarkRef.get_mark(syntax)
        if mark is not None:
            point(command, mark.clone(end=mark.start))
    return command


