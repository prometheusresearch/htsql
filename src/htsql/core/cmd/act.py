#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ..adapter import Adapter, adapt
from ..error import BadRequestError
from .command import (Command, UniversalCmd, DefaultCmd, RetrieveCmd,
                      ProducerCmd, RendererCmd)
from ..tr.lookup import lookup_command
from ..tr.parse import parse
from ..tr.bind import bind
from ..tr.embed import embed
from ..fmt.format import emit, emit_headers
from ..fmt.accept import accept


class UnsupportedActionError(BadRequestError):
    pass


class Action(object):
    pass


class ProduceAction(Action):
    pass


class SafeProduceAction(ProduceAction):

    def __init__(self, limit):
        assert isinstance(limit, int) and limit > 0
        self.limit = limit


class AnalyzeAction(Action):
    pass


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
        syntax = parse(self.command.query)
        environment = []
        if self.command.parameters is not None:
            for name in sorted(self.command.parameters):
                value = self.command.parameters[name]
                if isinstance(name, str):
                    name = name.decode('utf-8')
                recipe = embed(value)
                environment.append((name, recipe))
        binding = bind(syntax, environment=environment)
        command = lookup_command(binding)
        if command is None:
            command = DefaultCmd(binding)
        return act(command, self.action)


class ActDefault(Act):

    adapt(DefaultCmd, Action)

    def __call__(self):
        command = RetrieveCmd(self.command.binding)
        return act(command, self.action)


class RenderProducer(Act):

    adapt(ProducerCmd, RenderAction)

    def __call__(self):
        format = accept(self.action.environ)
        product = produce(self.command)
        status = "200 OK"
        headers = emit_headers(format, product)
        body = emit(format, product)
        return (status, headers, body)


class RenderRenderer(Act):

    adapt(RendererCmd, RenderAction)

    def __call__(self):
        format = self.command.format
        product = produce(self.command.producer)
        status = "200 OK"
        headers = emit_headers(format, product)
        body = emit(format, product)
        return (status, headers, body)


act = Act.__invoke__


def produce(command):
    action = ProduceAction()
    return act(command, action)


def safe_produce(command, limit):
    action = SafeProduceAction(limit)
    return act(command, action)


def analyze(command):
    action = AnalyzeAction()
    return act(command, action)


def render(command, environ):
    action = RenderAction(environ)
    return act(command, action)


