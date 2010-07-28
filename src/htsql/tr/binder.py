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
                      ColumnBinding, LiteralBinding, SieveBinding,
                      CastBinding, TupleBinding)
from .lookup import Lookup
from .fn.function import FindFunction
from ..introspect import Introspect
from ..domain import (Domain, BooleanDomain, IntegerDomain, DecimalDomain,
                      FloatDomain, StringDomain, DateDomain,
                      TupleDomain, UntypedDomain, VoidDomain)
from ..error import InvalidArgumentError
from ..context import context
import decimal


class Binder(object):

    def bind(self, syntax, parent=None):
        if parent is None:
            app = context.app
            if app.cached_catalog is None:
                introspect = Introspect()
                catalog = introspect()
                app.cached_catalog = catalog
            catalog = app.cached_catalog
            parent = RootBinding(catalog, syntax)
        bind = Bind(syntax, self)
        return bind.bind(parent)

    def bind_one(self, syntax, parent=None):
        if parent is None:
            app = context.app
            if app.cached_catalog is None:
                introspect = Introspect()
                catalog = introspect()
                app.cached_catalog = catalog
            catalog = app.cached_catalog
            parent = RootBinding(catalog, syntax)
        bind = Bind(syntax, self)
        return bind.bind_one(parent)

    def find_function(self, name):
        find_function = FindFunction()
        return find_function(name, self)

    def cast(self, binding, domain, syntax=None, parent=None):
        if syntax is None:
            syntax = binding.syntax
        if parent is None:
            parent = binding.parent
        cast = Cast(binding, binding.domain, domain, self)
        return cast.cast(syntax, parent)

    def coerce(self, left_domain, right_domain=None):
        if right_domain is not None:
            coerce = BinaryCoerce(left_domain, right_domain)
            domain = coerce()
            if domain is None:
                coerce = BinaryCoerce(right_domain, left_domain)
                domain = coerce()
            return domain
        else:
            coerce = UnaryCoerce(left_domain)
            return coerce()


class UnaryCoerce(Adapter):

    adapts(Domain)

    def __init__(self, domain):
        self.domain = domain

    def __call__(self):
        return self.domain


class CoerceVoid(UnaryCoerce):

    adapts(VoidDomain)

    def __call__(self):
        return None


class CoerceTuple(UnaryCoerce):

    adapts(TupleDomain)

    def __call__(self):
        return None


class CoerceUntyped(UnaryCoerce):

    adapts(UntypedDomain)

    def __call__(self):
        return StringDomain()


class BinaryCoerce(Adapter):

    adapts(Domain, Domain)

    def __init__(self, left_domain, right_domain):
        self.left_domain = left_domain
        self.right_domain = right_domain

    def __call__(self):
        return None


class BinaryCoerceUntyped(BinaryCoerce):

    adapts(Domain, UntypedDomain)

    def __call__(self):
        return self.left_domain


class BinaryCoerceBoolean(BinaryCoerce):

    adapts(BooleanDomain, BooleanDomain)

    def __call__(self):
        return BooleanDomain()


class BinaryCoerceInteger(BinaryCoerce):

    adapts(IntegerDomain, IntegerDomain)

    def __call__(self):
        return IntegerDomain()


class BinaryCoerceFloat(BinaryCoerce):

    adapts(FloatDomain, FloatDomain)

    def __call__(self):
        return FloatDomain()


class BinaryCoerceDecimal(BinaryCoerce):

    adapts(DecimalDomain, DecimalDomain)

    def __call__(self):
        return DecimalDomain()


class BinaryCoerceString(BinaryCoerce):

    adapts(StringDomain, StringDomain)

    def __call__(self):
        return StringDomain()


class BinaryCoerceDate(BinaryCoerce):

    adapts(DateDomain, DateDomain)

    def __call__(self):
        return DateDomain()


class CoerceIntegerToDecimal(BinaryCoerce):

    adapts(IntegerDomain, DecimalDomain)

    def __call__(self):
        return DecimalDomain()


class CoerceIntegerToFloat(BinaryCoerce):

    adapts(IntegerDomain, FloatDomain)

    def __call__(self):
        return FloatDomain()


class CoerceDecimalToFloat(BinaryCoerce):

    adapts(DecimalDomain, FloatDomain)

    def __call__(self):
        return FloatDomain()


class Cast(Adapter):

    adapts(Binding, Domain, Domain, Binder)

    def __init__(self, binding, from_domain, to_domain, binder):
        self.binding = binding
        self.from_domain = from_domain
        self.to_domain = to_domain
        self.binder = binder

    def cast(self, syntax, parent):
        return CastBinding(parent, self.binding, self.to_domain, syntax)


class CastBooleanToBoolean(Cast):

    adapts(Binding, BooleanDomain, BooleanDomain, Binder)

    def cast(self, syntax, parent):
        return self.binding


class CastTupleToBoolean(Cast):

    adapts(Binding, TupleDomain, BooleanDomain, Binder)

    def cast(self, syntax, parent):
        return TupleBinding(self.binding)


class CastStringToString(Cast):

    adapts(Binding, StringDomain, StringDomain, Binder)

    def cast(self, syntax, parent):
        return self.binding


class CastIntegerToInteger(Cast):

    adapts(Binding, IntegerDomain, IntegerDomain, Binder)

    def cast(self, syntax, parent):
        return self.binding


class CastDecimalToDecimal(Cast):

    adapts(Binding, DecimalDomain, DecimalDomain, Binder)

    def cast(self, syntax, parent):
        return self.binding


class CastFloatToFloat(Cast):

    adapts(Binding, FloatDomain, FloatDomain, Binder)

    def cast(self, syntax, parent):
        return self.binding


class CastDateToDate(Cast):

    adapts(Binding, DateDomain, DateDomain, Binder)

    def cast(self, syntax, parent):
        return self.binding


class CastLiteral(Cast):

    adapts(LiteralBinding, UntypedDomain, Domain, Binder)

    def cast(self, syntax, parent):
        try:
            value = self.to_domain.parse(self.binding.value)
        except ValueError, exc:
            raise InvalidArgumentError("cannot cast a value: %s" % exc,
                                       syntax.mark)
        return LiteralBinding(parent, value, self.to_domain, syntax)


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
            bare_elements = list(self.binder.bind(self.syntax.selector, base))
        else:
            lookup = Lookup(base)
            bare_elements = list(lookup.enumerate(base.syntax))
        elements = []
        for element in bare_elements:
            domain = self.binder.coerce(element.domain)
            if domain is None:
                raise InvalidArgumentError("invalid type", element.mark)
            if domain is not element.domain:
                element = self.binder.cast(element, domain)
            elements.append(element)
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


class BindOperator(Bind):

    adapts(OperatorSyntax, Binder)

    def bind(self, parent):
        name = self.syntax.symbol
        if self.syntax.left is None:
            name = name+'_'
        if self.syntax.right is None:
            name = '_'+name
        function = self.binder.find_function(name)
        return function.bind_operator(self.syntax, parent)


class BindFunctionOperator(Bind):

    adapts(FunctionOperatorSyntax, Binder)

    def bind(self, parent):
        name = self.syntax.identifier.value
        function = self.binder.find_function(name)
        return function.bind_function_operator(self.syntax, parent)


class BindFunctionCall(Bind):

    adapts(FunctionCallSyntax, Binder)

    def bind(self, parent):
        if self.syntax.base is not None:
            parent = self.binder.bind_one(self.syntax.base, parent)
        name = self.syntax.identifier.value
        function = self.binder.find_function(name)
        return function.bind_function_call(self.syntax, parent)


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
                                 UntypedDomain(),
                                 self.syntax)
        yield binding


class BindNumber(Bind):

    adapts(NumberSyntax, Binder)

    def bind(self, parent):
        value = self.syntax.value
        if 'e' in value or 'E' in value:
            domain = FloatDomain()
            value = float(value)
            if str(value) in ['inf', '-inf', 'nan']:
                raise InvalidArgumentError("invalid float value",
                                           self.syntax.mark)
        elif '.' in value:
            domain = DecimalDomain()
            value = decimal.Decimal(value)
        else:
            domain = IntegerDomain()
            value = int(value)
            if not (-2**63 <= value < 2**63):
                raise InvalidArgumentError("invalid integer value",
                                           self.syntax.mark)
        binding = LiteralBinding(parent, value, domain, self.syntax)
        yield binding


bind_adapters = find_adapters()


