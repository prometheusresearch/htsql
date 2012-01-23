#
# Copyright (c) 2006-2012, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from ..adapter import Protocol, named
from ..domain import Profile
from .lookup import guess_name
from .binding import Binding


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
        return self.binding


class DecorateSyntax(Decorate):

    named('syntax')

    def __call__(self):
        return self.binding.syntax


class DecorateName(Decorate):

    named('name')

    def __call__(self):
        name = guess_name(self.binding)
        if name is not None:
            name = name.encode('utf-8')
        return name


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


