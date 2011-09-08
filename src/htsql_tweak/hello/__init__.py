#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.adapter import adapts, named
from htsql.addon import Addon
from htsql.tr.fn.bind import BindCommand
from htsql.tr.binding import CommandBinding
from htsql.tr.signature import NullarySig
from htsql.cmd.command import Command
from htsql.cmd.act import Act, RenderAction


class HelloCmd(Command):
    pass


class BindHello(BindCommand):

    named('hello')
    signature = NullarySig

    def expand(self):
        command = HelloCmd()
        return CommandBinding(self.state.scope, command, self.syntax)


class RenderHello(Act):

    adapts(HelloCmd, RenderAction)

    def __call__(self):
        status = "200 OK"
        headers = [('Content-Type', "text/plain; charset=UTF-8")]
        body = ["Hello, World!"]
        return (status, headers, body)


class TweakHelloAddon(Addon):

    name = 'tweak.hello'


