#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from ..adapter import adapts
from .command import (UniversalCmd, DefaultCmd, ProducerCmd, RendererCmd,
                      RetrieveCmd)
from .act import act, Act, RenderAction, produce
from ..tr.lookup import lookup_command
from ..tr.parse import parse
from ..tr.bind import bind
from ..fmt.format import FindRenderer


class RenderUniversal(Act):

    adapts(UniversalCmd, RenderAction)

    def __call__(self):
        syntax = parse(self.command.query)
        binding = bind(syntax)
        command = lookup_command(binding)
        if command is None:
            command = DefaultCmd(binding)
        return act(command, self.action)


class RenderDefault(Act):

    adapts(DefaultCmd, RenderAction)

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


