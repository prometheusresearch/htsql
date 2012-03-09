#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ...core.context import context
from ...core.adapter import adapt, call
from ...core.addon import Addon, Parameter
from ...core.tr.fn.bind import BindCommand
from ...core.tr.binding import CommandBinding
from ...core.tr.signature import NullarySig
from ...core.cmd.command import Command
from ...core.cmd.act import Act, RenderAction
from ...core.validator import PIntVal, ChoiceVal


class HelloCmd(Command):
    pass


class BindHello(BindCommand):

    call('hello')
    signature = NullarySig

    def expand(self):
        command = HelloCmd()
        return CommandBinding(self.state.scope, command, self.syntax)


class RenderHello(Act):

    adapt(HelloCmd, RenderAction)

    def __call__(self):
        status = "200 OK"
        address = context.app.tweak.hello.address
        repeat = context.app.tweak.hello.repeat
        line = "Hello, " + address.capitalize() + "!\n"
        headers = [('Content-Type', "text/plain; charset=UTF-8")]
        body = [line * repeat]
        return (status, headers, body)


class TweakHelloAddon(Addon):
    
    name = 'tweak.hello'
    hint = """example plugin and command"""
    help = """
      This is an example plugin and command.  It has two parameters
      ``address`` which defaults to 'world' and ``repeat`` which
      defaults to 1.  The plugin registers a command ``/hello()``
      that prints "Hello, X!" several times.  It can be started 
      using a command line::

       htsql-ctl shell htsql_regress -E tweak.hello:address=mom,repeat=3

    """

    parameters = [
            Parameter('repeat', PIntVal(), default=1),
            Parameter('address', ChoiceVal(['mom','home','world']), 
                                 default='world')
    ]

