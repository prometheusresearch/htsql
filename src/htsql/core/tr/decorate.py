#
# Copyright (c) 2006-2012, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from ..adapter import Protocol, named
from ..domain import Profile
from .lookup import guess_tag, guess_header, guess_path
from .binding import Binding, VoidBinding
from .syntax import VoidSyntax


class Decorate(Protocol):

    def __init__(self, name, binding):
        assert isinstance(name, str)
        assert isinstance(binding, Binding)
        self.name = name
        self.binding = binding

    def __call__(self):
        return None


class DecorateDomain(Decorate):

    named('domain')

    def __call__(self):
        return self.binding.domain


class DecorateBinding(Decorate):

    named('binding')

    def __call__(self):
        if isinstance(self.binding, VoidBinding):
            return None
        return self.binding


class DecorateSyntax(Decorate):

    named('syntax')

    def __call__(self):
        if isinstance(self.binding.syntax, VoidSyntax):
            return None
        return self.binding.syntax


class DecorateTag(Decorate):

    named('tag')

    def __call__(self):
        return guess_tag(self.binding)


class DecorateHeader(Decorate):

    named('header')

    def __call__(self):
        return guess_header(self.binding)


class DecoratePath(Decorate):

    named('path')

    def __call__(self):
        return guess_path(self.binding)


class DecoratePlan(Decorate):

    named('plan')

    def __call__(self):
        return None


def decorate(binding):
    names = set()
    for component in Decorate.implementations():
        for name in component.names:
            names.add(name)
    decorations = {}
    for name in sorted(names):
        decorate = Decorate(name, binding)
        value = decorate()
        decorations[name] = value
    return Profile(**decorations)


