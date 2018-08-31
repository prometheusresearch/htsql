#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ...core.context import context
from ...core.adapter import adapt, call
from ...core.addon import Addon, Parameter
from ...core.validator import PIntVal, ChoiceVal
from ...core.error import Error
from ...core.cmd.command import Command
from ...core.cmd.summon import Summon
from ...core.cmd.act import Act, RenderAction


class HelloCmd(Command):
    pass


class SummonHello(Summon):

    call('hello')

    def __call__(self):
        if self.arguments:
            raise Error("Expected no arguments")
        return HelloCmd()


class RenderHello(Act):

    adapt(HelloCmd, RenderAction)

    def __call__(self):
        status = "200 OK"
        address = context.app.tweak.hello.address
        repeat = context.app.tweak.hello.repeat
        line = ("Hello, " + address.capitalize() + "!\n").encode('utf-8')
        headers = [('Content-Type', "text/plain; charset=UTF-8")]
        body = [line]*repeat
        return (status, headers, body)


class TweakHelloAddon(Addon):
    
    name = 'tweak.hello'
    hint = """'Hello, World!' plugin"""
    help = """
    This plugin registers command `/hello()`, which displays "Hello, World!".

    The plugin has two parameters: `address` (default: 'world') and
    `repeat` (default: 1).
    """

    parameters = [
            Parameter('repeat',
                      PIntVal(), default=1),
            Parameter('address',
                      ChoiceVal(['mom', 'home', 'world']),
                      default='world')
    ]


