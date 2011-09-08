#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from ..adapter import Adapter, adapts
from ..error import BadRequestError
from .command import Command


class Action(object):
    pass


class ProduceAction(Action):
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
        raise BadRequestError("invalid command")


def act(command, action):
    act = Act(command, action)
    return act()


def produce(command):
    action = ProduceAction()
    return act(command, action)


def render(command, environ):
    action = RenderAction(environ)
    return act(command, action)


