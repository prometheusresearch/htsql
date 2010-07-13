#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.fn.function`
===========================

This module implements HTSQL functions.
"""


from ...adapter import Adapter, Utility, adapts, adapts_none, find_adapters
from ...error import InvalidArgumentError
from ...domain import (Domain, UntypedDomain, BooleanDomain, StringDomain,
                       IntegerDomain, DecimalDomain, FloatDomain, DateDomain)
from ..binding import (LiteralBinding, FunctionBinding,
                       EqualityBinding, InequalityBinding,
                       ConjunctionBinding, DisjunctionBinding, NegationBinding)


class named(str):

    name_registry = {}

    class __metaclass__(type):

        def __getitem__(cls, key):
            if key in cls.name_registry:
                return cls.name_registry[key]
            key_type = type(key, (cls,), {})
            cls.name_registry.setdefault(key, key_type)
            return cls.name_registry[key]

    def __new__(cls):
        return super(named, cls).__new__(cls, cls.__name__)


class FindFunction(Utility):

    def __call__(self, name, binder):
        name = named[name]()
        function = Function(name, binder)
        return function


class Function(Adapter):

    adapts(str)

    def __init__(self, name, binder):
        self.name = str(name)
        self.binder = binder

    def bind_operator(self, syntax, parent):
        raise InvalidArgumentError("unknown operator %s" % self.name,
                                   syntax.mark)

    def bind_function_operator(self, syntax, parent):
        raise InvalidArgumentError("unknown function %s" % self.name,
                                   syntax.identifier.mark)

    def bind_function_call(self, syntax, parent):
        raise InvalidArgumentError("unknown function %s" % self.name,
                                   syntax.identifier.mark)


class Parameter(object):

    def __init__(self, name, domain_class=Domain,
                 is_mandatory=True, is_list=False):
        assert isinstance(name, str)
        assert issubclass(domain_class, Domain)
        assert isinstance(is_mandatory, bool)
        assert isinstance(is_list, bool)
        self.name = name
        self.domain_class = domain_class
        self.is_mandatory = is_mandatory
        self.is_list = is_list


class ProperFunction(Function):

    adapts_none()

    parameters = []

    def bind_operator(self, syntax, parent):
        arguments = []
        if syntax.left is not None:
            arguments.append(syntax.left)
        if syntax.right is not None:
            arguments.append(syntax.right)
        keywords = self.bind_arguments(arguments, parent, syntax.mark)
        return self.correlate(syntax=syntax, parent=parent, **keywords)

    def bind_function_operator(self, syntax, parent):
        arguments = [syntax.left, syntax.right]
        keywords = self.bind_arguments(arguments, parent)
        return self.correlate(syntax=syntax, parent=parent, **keywords)

    def bind_function_call(self, syntax, parent):
        arguments = syntax.arguments
        keywords = self.bind_arguments(arguments, parent, syntax.mark)
        return self.correlate(syntax=syntax, parent=parent, **keywords)

    def bind_arguments(self, arguments, parent, mark):
        arguments = [list(self.binder.bind(argument, parent))
                     for argument in arguments]
        return self.check_arguments(arguments, mark)

    def check_arguments(self, arguments, mark):
        arguments = arguments[:]
        keywords = {}
        for idx, parameter in enumerate(self.parameters):
            value = None
            if not arguments:
                if parameter.is_mandatory:
                    raise InvalidArgumentError("missing argument %s"
                                               % parameter.name, mark)
            elif parameter.is_list:
                value = []
                if len(arguments) > 1 and idx == len(self.parameters)-1:
                    while arguments:
                        argument = arguments.pop(0)
                        if len(argument) != 1:
                            raise InvalidArgumentError("invalid argument %s"
                                                       % parameter.name, mark)
                        value.append(argument[0])
                else:
                    argument = arguments.pop(0)
                    if parameter.is_mandatory and not argument:
                        raise InvalidArgumentError("missing argument %s"
                                                   % parameter.name, mark)
                    value = argument[:]
                for argument in value:
                    if not isinstance(argument.domain, parameter.domain_class):
                        raise InvalidArgumentError("unexpected argument type",
                                                   argument.mark)
            else:
                argument = arguments.pop(0)
                if len(argument) == 0:
                    if parameter.is_mandatory:
                        raise InvalidArgumentError("missing argument %s"
                                                   % parameter.name, mark)
                    value = None
                elif len(argument) == 1:
                    value = argument[0]
                    if not isinstance(value.domain, parameter.domain_class):
                        raise InvalidArgumentError("unexpected argument type",
                                                   value.mark)
                else:
                    raise InvalidArgumentError("too many arguments",
                                               argument[1].mark)
            keywords[parameter.name] = value
        if arguments:
            raise InvalidArgumentError("unexpected argument",
                                       arguments[0].mark)
        return keywords


class ProperMethod(ProperFunction):

    adapts_none()

    def bind_arguments(self, arguments, parent, mark):
        arguments = [parent] + [list(self.binder.bind(argument, parent))
                                for argument in arguments]
        return self.check_arguments(arguments, mark)


class NullFunction(ProperFunction):

    adapts(named['null'])

    def correlate(self, syntax, parent):
        yield LiteralBinding(parent, None, UntypedDomain(), syntax)


class TrueFunction(ProperFunction):

    adapts(named['true'])

    def correlate(self, syntax, parent):
        yield LiteralBinding(parent, True, BooleanDomain(), syntax)


class FalseFunction(ProperFunction):

    adapts(named['false'])

    def correlate(self, syntax, parent):
        yield LiteralBinding(parent, False, BooleanDomain(), syntax)


class CastFunction(ProperFunction):

    parameters = [
            Parameter('argument'),
    ]
    output_domain = None

    def correlate(self, argument, syntax, parent):
        yield self.binder.cast(argument, self.output_domain, syntax, parent)


class BooleanCastFunction(CastFunction):

    adapts(named['boolean'])
    output_domain = BooleanDomain()


class StringCastFunction(CastFunction):

    adapts(named['string'])
    output_domain = StringDomain()


class IntegerCastFunction(CastFunction):

    adapts(named['integer'])
    output_domain = IntegerDomain()


class DecimalCastFunction(CastFunction):

    adapts(named['decimal'])
    output_domain = DecimalDomain()


class FloatCastFunction(CastFunction):

    adapts(named['float'])
    output_domain = FloatDomain()


class DateCastFunction(CastFunction):

    adapts(named['date'])
    output_domain = DateDomain()


class EqualityOperator(ProperFunction):

    adapts(named['_=_'])

    parameters = [
            Parameter('left_argument'),
            Parameter('right_argument'),
    ]

    def correlate(self, left_argument, right_argument, syntax, parent):
        domain = self.binder.coerce(left_argument.domain,
                                    right_argument.domain)
        if domain is None:
            raise InvalidArgumentError("invalid arguments", syntax.mark)
        domain = self.binder.coerce(domain)
        if domain is None:
            raise InvalidArgumentError("invalid arguments", syntax.mark)
        left_argument = self.binder.cast(left_argument, domain)
        right_argument = self.binder.cast(right_argument, domain)
        yield EqualityBinding(parent, left_argument, right_argument, syntax)


class InequalityOperator(ProperFunction):

    adapts(named['_!=_'])

    parameters = [
            Parameter('left_argument'),
            Parameter('right_argument'),
    ]

    def correlate(self, left_argument, right_argument, syntax, parent):
        domain = self.binder.coerce(left_argument.domain,
                                    right_argument.domain)
        if domain is None:
            raise InvalidArgumentError("invalid arguments", syntax.mark)
        domain = self.binder.coerce(domain)
        if domain is None:
            raise InvalidArgumentError("invalid arguments", syntax.mark)
        left_argument = self.binder.cast(left_argument, domain)
        right_argument = self.binder.cast(right_argument, domain)
        yield InequalityBinding(parent, left_argument, right_argument, syntax)


class ConjunctionOperator(ProperFunction):

    adapts(named['_&_'])

    parameters = [
            Parameter('left_argument'),
            Parameter('right_argument'),
    ]

    def correlate(self, left_argument, right_argument, syntax, parent):
        left_argument = self.binder.cast(left_argument, BooleanDomain())
        right_argument = self.binder.cast(right_argument, BooleanDomain())
        yield ConjunctionBinding(parent,
                                 [left_argument, right_argument], syntax)


class DisjunctionOperator(ProperFunction):

    adapts(named['_|_'])

    parameters = [
            Parameter('left_argument'),
            Parameter('right_argument'),
    ]

    def correlate(self, left_argument, right_argument, syntax, parent):
        left_argument = self.binder.cast(left_argument, BooleanDomain())
        right_argument = self.binder.cast(right_argument, BooleanDomain())
        yield DisjunctionBinding(parent,
                                 [left_argument, right_argument], syntax)


class AdditionOperator(ProperFunction):

    adapts(named['_+_'])

    parameters = [
            Parameter('left_argument'),
            Parameter('right_argument'),
    ]

    def correlate(self, left_argument, right_argument, syntax, parent):
        Implementation = Add.realize(left_argument.domain,
                                     right_argument.domain)
        addition = Implementation(left_argument, right_argument,
                                  self.binder, syntax, parent)
        yield addition()


class Add(Adapter):

    adapts(Domain, Domain)

    def __init__(self, left_argument, right_argument, binder, syntax, parent):
        self.left_argument = left_argument
        self.right_argument = right_argument
        self.binder = binder
        self.syntax = syntax
        self.parent = parent

    def __call__(self):
        raise InvalidArgumentError("unexpected argument types",
                                   self.syntax.mark)


class ConcatenateBinding(FunctionBinding):

    def __init__(self, parent, left_argument, right_argument, syntax):
        super(ConcatenateBinding, self).__init__(parent, StringDomain(), syntax,
                                                 left_argument=left_argument,
                                                 right_argument=right_argument)


class Concatenate(Add):

    adapts_none()

    def __call__(self):
        left_argument = self.binder.cast(self.left_argument, StringDomain(),
                                         parent=self.parent)
        right_argument = self.binder.cast(self.right_argument, StringDomain(),
                                          parent=self.parent)
        return ConcatenateBinding(self.parent, left_argument, right_argument,
                                  self.syntax)


class ConcatenateStringToString(Concatenate):

    adapts(StringDomain, StringDomain)


class ConcatenateStringToUntyped(Concatenate):

    adapts(StringDomain, UntypedDomain)


class ConcatenateUntypedToString(Concatenate):

    adapts(UntypedDomain, StringDomain)


class ConcatenateUntypedToUntyped(Concatenate):

    adapts(UntypedDomain, UntypedDomain)


function_adapters = find_adapters()


