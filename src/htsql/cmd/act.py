#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from ..adapter import Adapter, adapts
from ..error import BadRequestError
from .command import (Command, UniversalCmd, DefaultCmd, RetrieveCmd,
                      ProducerCmd, RendererCmd)
from ..tr.lookup import lookup_command
from ..tr.parse import parse
from ..tr.bind import bind
from ..fmt.format import FindRenderer


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

    adapts(Command, Action)

    def __init__(self, command, action):
        assert isinstance(command, Command)
        assert isinstance(action, Action)
        self.command = command
        self.action = action

    def __call__(self):
        raise UnsupportedActionError("unsupported action")


class ActUniversal(Act):

    adapts(UniversalCmd, Action)

    def __call__(self):
        syntax = parse(self.command.query)
        binding = bind(syntax)
        command = lookup_command(binding)
        if command is None:
            command = DefaultCmd(binding)
        return act(command, self.action)


class ActDefault(Act):

    adapts(DefaultCmd, Action)

    def __call__(self):
        command = RetrieveCmd(self.command.binding)
        return act(command, self.action)


class RenderProducer(Act):

    adapts(ProducerCmd, RenderAction)

    def __call__(self):
        product = produce(self.command)
        environ = self.action.environ
        find_renderer = FindRenderer()
        accept = set([''])
        if 'HTTP_ACCEPT' in environ:
            for name in environ['HTTP_ACCEPT'].split(','):
                if ';' in name:
                    name = name.split(';', 1)[0]
                name = name.strip()
                accept.add(name)
        renderer_class = find_renderer(accept)
        assert renderer_class is not None
        renderer = renderer_class()
        return renderer.render(product)


class RenderRenderer(Act):

    adapts(RendererCmd, RenderAction)

    def __call__(self):
        product = produce(self.command.producer)
        renderer_class = self.command.format
        renderer = renderer_class()
        return renderer.render(product)


def act(command, action):
    act = Act(command, action)
    return act()


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


