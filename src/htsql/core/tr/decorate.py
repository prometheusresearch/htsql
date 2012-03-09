#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ..adapter import Protocol, call
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

    call('domain')

    def __call__(self):
        return self.binding.domain


class DecorateBinding(Decorate):

    call('binding')

    def __call__(self):
        if isinstance(self.binding, VoidBinding):
            return None
        return self.binding


class DecorateSyntax(Decorate):

    call('syntax')

    def __call__(self):
        if isinstance(self.binding.syntax, VoidSyntax):
            return None
        return self.binding.syntax


class DecorateTag(Decorate):

    call('tag')

    def __call__(self):
        return guess_tag(self.binding)


class DecorateHeader(Decorate):

    call('header')

    def __call__(self):
        return guess_header(self.binding)


class DecoratePath(Decorate):

    call('path')

    def __call__(self):
        return guess_path(self.binding)


class DecoratePlan(Decorate):

    call('plan')

    def __call__(self):
        return None


def decorate(binding):
    decorations = {}
    for name in Decorate.__catalogue__():
        value = Decorate.__invoke__(name, binding)
        decorations[name] = value
    return Profile(**decorations)


