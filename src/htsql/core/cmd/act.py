#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ..adapter import Adapter, adapt
from ..error import Error, act_guard
from ..util import Clonable
from .command import Command, UniversalCmd, DefaultCmd, FormatCmd, FetchCmd
from .summon import recognize
from .embed import embed
from ..syn.parse import parse
from ..syn.syntax import Syntax
from ..fmt.emit import emit, emit_headers
from ..fmt.accept import accept


class UnsupportedActionError(Error):
    pass


class Action(Clonable):
    pass


class ProduceAction(Action):

    def __init__(self, environment=None, batch=None):
        self.environment = environment
        self.batch = batch


class SafeProduceAction(ProduceAction):

    def __init__(self, environment=None, cut=None, offset=None, batch=None):
        self.environment = environment
        self.cut = cut
        self.offset = offset
        self.batch = batch


class AnalyzeAction(Action):

    def __init__(self, environment=None):
        self.environment = environment


class RenderAction(Action):

    def __init__(self, environ):
        self.environ = environ


class Act(Adapter):

    adapt(Command, Action)

    def __init__(self, command, action):
        assert isinstance(command, Command)
        assert isinstance(action, Action)
        self.command = command
        self.action = action

    def __call__(self):
        raise UnsupportedActionError("unsupported action")


class ActUniversal(Act):

    adapt(UniversalCmd, Action)

    def __call__(self):
        return act(self.command.query, self.action)


class ActDefault(Act):

    adapt(DefaultCmd, Action)

    def __call__(self):
        command = FetchCmd(self.command.syntax)
        return act(command, self.action)


class RenderFormat(Act):

    adapt(FormatCmd, RenderAction)

    def __call__(self):
        format = self.command.format
        product = produce(self.command.feed)
        status = "200 OK"
        headers = emit_headers(format, product)
        body = emit(format, product)
        return (status, headers, body)


class RenderProducer(Act):

    adapt(Command, RenderAction)

    @classmethod
    def __follows__(component, other):
        return True

    def __call__(self):
        format = accept(self.action.environ)
        product = produce(self.command)
        status = "200 OK"
        headers = emit_headers(format, product)
        body = emit(format, product)
        return (status, headers, body)


def act(command, action):
    assert isinstance(command, (Command, Syntax, str))
    assert isinstance(action, Action)
    if not isinstance(command, Command):
        command = recognize(command)
    with act_guard(command):
        return Act.__invoke__(command, action)


def produce(command, environment=None, **parameters):
    environment = embed(environment, **parameters)
    action = ProduceAction(environment)
    return act(command, action)


def safe_produce(command, cut, offset=None, environment=None, **parameters):
    environment = embed(environment, **parameters)
    action = SafeProduceAction(environment, cut, offset)
    return act(command, action)


def analyze(command, environment=None, **parameters):
    environment = embed(environment, **parameters)
    action = AnalyzeAction(environment)
    return act(command, action)


def render(command, environ):
    action = RenderAction(environ)
    return act(command, action)


