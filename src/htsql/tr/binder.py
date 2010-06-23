#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.binder`
======================

This module implements binding adapters.
"""


from ..adapter import Adapter, adapts, find_adapters
from .syntax import (Syntax, QuerySyntax, SegmentSyntax, SelectorSyntax,
                     SieveSyntax, OperatorSyntax, FunctionOperatorSyntax,
                     FunctionCallSyntax, GroupSyntax, SpecifierSyntax,
                     IdentifierSyntax, WildcardSyntax, StringSyntax,
                     NumberSyntax)
from .binding import (Binding, RootBinding, QueryBinding, SegmentBinding,
                      TableBinding, FreeTableBinding, JoinedTableBinding,
                      ColumnBinding, LiteralBinding, SieveBinding)
from .lookup import Lookup
from ..introspect import Introspect
from ..domain import UntypedStringDomain, UntypedNumberDomain
from ..error import InvalidArgumentError


class Binder(object):

    def bind(self, syntax, parent=None):
        if parent is None:
            introspect = Introspect()
            catalog = introspect()
            parent = RootBinding(catalog, syntax)
        bind = Bind(syntax, self)
        return bind.bind(parent)

    def bind_one(self, syntax, parent=None):
        if parent is None:
            introspect = Introspect()
            catalog = introspect()
            parent = RootBinding(catalog, syntax)
        bind = Bind(syntax, self)
        return bind.bind_one(parent)


class Bind(Adapter):

    adapts(Syntax, Binder)

    def __init__(self, syntax, binder):
        self.syntax = syntax
        self.binder = binder

    def bind(self, parent):
        raise InvalidArgumentError("unable to bind a node", self.syntax.mark)

    def bind_one(self, parent):
        bindings = list(self.bind(parent))
        if len(bindings) == 1:
            return bindings[0]
        if len(bindings) < 1:
            raise InvalidArgumentError("expected one node; got none",
                                       self.syntax.mark)
        if len(bindings) > 1:
            raise InvalidArgumentError("expected one node; got more",
                                       self.syntax.mark)


class BindQuery(Bind):

    adapts(QuerySyntax, Binder)

    def bind(self, parent):
        segment = None
        if self.syntax.segment is not None:
            segment = self.binder.bind_one(self.syntax.segment, parent)
        yield QueryBinding(parent, segment, self.syntax)


class BindSegment(Bind):

    adapts(SegmentSyntax, Binder)

    def bind(self, parent):
        base = parent
        if self.syntax.base is not None:
            base = self.binder.bind_one(self.syntax.base, base)
        if self.syntax.filter is not None:
            filter = self.binder.bind_one(self.syntax.filter, base)
            base_syntax = SieveSyntax(base.syntax, None, filter.syntax,
                                      base.mark)
            base_syntax = GroupSyntax(base_syntax, base.mark)
            base = SieveBinding(base, filter, base_syntax)
        if self.syntax.selector is not None:
            elements = list(self.binder.bind(self.syntax.selector, base))
        else:
            lookup = Lookup(base)
            elements = list(lookup.enumerate(base.syntax))
        yield SegmentBinding(parent, base, elements, self.syntax)


class BindSelector(Bind):

    adapts(SelectorSyntax, Binder)

    def bind(self, parent):
        for element in self.syntax.elements:
            for binding in self.binder.bind(element, parent):
                yield binding


class BindSieve(Bind):

    adapts(SieveSyntax, Binder)

    def bind(self, parent):
        base = self.binder.bind_one(self.syntax.base, parent)
        if self.syntax.filter is not None:
            filter = self.binder.bind_one(self.syntax.filter, base)
            base_syntax = SieveSyntax(base.syntax, None, filter.syntax,
                                      base.mark)
            if self.syntax.selector is not None:
                base_syntax = GroupSyntax(base_syntax, base.mark)
            base = SieveBinding(base, filter, base_syntax)
        if self.syntax.selector is not None:
            for binding in self.binder.bind(self.syntax.selector, base):
                selector = SelectorSyntax([binding.syntax], binding.mark)
                binding_syntax = SieveSyntax(self.syntax.base,
                                             selector,
                                             self.syntax.filter,
                                             binding.mark)
                binding = binding.clone(syntax=binding_syntax)
                yield binding
        else:
            yield base


class BindGroup(Bind):

    adapts(GroupSyntax, Binder)

    def bind(self, parent):
        for binding in self.binder.bind(self.syntax.expression, parent):
            binding_syntax = GroupSyntax(binding.syntax, binding.mark)
            binding = binding.clone(syntax=binding_syntax)
            yield binding


class BindSpecifier(Bind):

    adapts(SpecifierSyntax, Binder)

    def bind(self, parent):
        base = self.binder.bind_one(self.syntax.base, parent)
        for binding in self.binder.bind(self.syntax.identifier, base):
            binding_syntax = SpecifierSyntax(base.syntax, binding.syntax,
                                             binding.mark)
            binding = binding.clone(syntax=binding_syntax)
            yield binding


class BindIdentifier(Bind):

    adapts(IdentifierSyntax, Binder)

    def bind(self, parent):
        lookup = Lookup(parent)
        binding = lookup(self.syntax)
        yield binding


class BindWildcard(Bind):

    adapts(WildcardSyntax, Binder)

    def bind(self, parent):
        lookup = Lookup(parent)
        for binding in lookup.enumerate(self.syntax):
            yield binding


class BindString(Bind):

    adapts(StringSyntax, Binder)

    def bind(self, parent):
        binding = LiteralBinding(parent, self.syntax.value,
                                 UntypedStringDomain(),
                                 self.syntax)
        yield binding


class BindNumber(Bind):

    adapts(NumberSyntax, Binder)

    def bind(self, parent):
        binding = LiteralBinding(parent, self.syntax.value,
                                 UntypedNumberDomain(),
                                 self.syntax)
        yield binding


bind_adapters = find_adapters()


