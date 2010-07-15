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
from ..encoder import Encoder, Encode
from ..code import FunctionExpression, AggregateUnit
from ..compiler import Compiler, Evaluate
from ..frame import FunctionPhrase
from ..serializer import Serializer, Format, Serialize


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
            Parameter('left'),
            Parameter('right'),
    ]

    def correlate(self, left, right, syntax, parent):
        domain = self.binder.coerce(left.domain,
                                    right.domain)
        if domain is None:
            raise InvalidArgumentError("invalid arguments", syntax.mark)
        domain = self.binder.coerce(domain)
        if domain is None:
            raise InvalidArgumentError("invalid arguments", syntax.mark)
        left = self.binder.cast(left, domain)
        right = self.binder.cast(right, domain)
        yield EqualityBinding(parent, left, right, syntax)


class InequalityOperator(ProperFunction):

    adapts(named['_!=_'])

    parameters = [
            Parameter('left'),
            Parameter('right'),
    ]

    def correlate(self, left, right, syntax, parent):
        domain = self.binder.coerce(left.domain,
                                    right.domain)
        if domain is None:
            raise InvalidArgumentError("invalid arguments", syntax.mark)
        domain = self.binder.coerce(domain)
        if domain is None:
            raise InvalidArgumentError("invalid arguments", syntax.mark)
        left = self.binder.cast(left, domain)
        right = self.binder.cast(right, domain)
        yield InequalityBinding(parent, left, right, syntax)


class ConjunctionOperator(ProperFunction):

    adapts(named['_&_'])

    parameters = [
            Parameter('left'),
            Parameter('right'),
    ]

    def correlate(self, left, right, syntax, parent):
        left = self.binder.cast(left, BooleanDomain())
        right = self.binder.cast(right, BooleanDomain())
        yield ConjunctionBinding(parent, [left, right], syntax)


class DisjunctionOperator(ProperFunction):

    adapts(named['_|_'])

    parameters = [
            Parameter('left'),
            Parameter('right'),
    ]

    def correlate(self, left, right, syntax, parent):
        left = self.binder.cast(left, BooleanDomain())
        right = self.binder.cast(right, BooleanDomain())
        yield DisjunctionBinding(parent, [left, right], syntax)


class AdditionOperator(ProperFunction):

    adapts(named['_+_'])

    parameters = [
            Parameter('left'),
            Parameter('right'),
    ]

    def correlate(self, left, right, syntax, parent):
        Implementation = Add.realize(left.domain, right.domain)
        addition = Implementation(left, right, self.binder, syntax, parent)
        yield addition()


class Add(Adapter):

    adapts(Domain, Domain)

    def __init__(self, left, right, binder, syntax, parent):
        self.left = left
        self.right = right
        self.binder = binder
        self.syntax = syntax
        self.parent = parent

    def __call__(self):
        raise InvalidArgumentError("unexpected argument types",
                                   self.syntax.mark)


class ConcatenationBinding(FunctionBinding):

    def __init__(self, parent, left, right, syntax):
        super(ConcatenationBinding, self).__init__(parent, StringDomain(),
                                                   syntax,
                                                   left=left, right=right)


class ConcatenationExpression(FunctionExpression):

    def __init__(self, left, right, mark):
        super(ConcatenationExpression, self).__init__(StringDomain(), mark,
                                                      left=left, right=right)


class ConcatenationPhrase(FunctionPhrase):

    def __init__(self, left, right, mark):
        super(ConcatenationPhrase, self).__init__(StringDomain(), False, mark,
                                                  left=left, right=right)


class Concatenate(Add):

    adapts_none()

    def __call__(self):
        left = self.binder.cast(self.left, StringDomain(),
                                parent=self.parent)
        right = self.binder.cast(self.right, StringDomain(),
                                 parent=self.parent)
        return ConcatenationBinding(self.parent, left, right, self.syntax)


class ConcatenateStringToString(Concatenate):

    adapts(StringDomain, StringDomain)


class ConcatenateStringToUntyped(Concatenate):

    adapts(StringDomain, UntypedDomain)


class ConcatenateUntypedToString(Concatenate):

    adapts(UntypedDomain, StringDomain)


class ConcatenateUntypedToUntyped(Concatenate):

    adapts(UntypedDomain, UntypedDomain)


class EncodeConcatenation(Encode):

    adapts(ConcatenationBinding, Encoder)

    def encode(self):
        left = self.encoder.encode(self.binding.left)
        right = self.encoder.encode(self.binding.right)
        return ConcatenationExpression(left, right, self.binding.mark)


class EvaluateConcatenation(Evaluate):

    adapts(ConcatenationExpression, Compiler)

    def evaluate(self, references):
        left = self.compiler.evaluate(self.expression.left, references)
        right = self.compiler.evaluate(self.expression.right, references)
        return ConcatenationPhrase(left, right, self.expression.mark)


class FormatFunctions(Format):

    def concat_op(self, left, right):
        return "(%s || %s)" % (left, right)

    def count_fn(self, condition):
        return "COUNT(NULLIF(%s, FALSE))" % condition

    def count_wrapper(self, aggregate):
        return "COALESCE(%s, 0)" % aggregate


class SerializeConcatenation(Serialize):

    adapts(ConcatenationPhrase, Serializer)

    def serialize(self):
        left = self.serializer.serialize(self.phrase.left)
        right = self.serializer.serialize(self.phrase.right)
        return self.format.concat_op(left, right)


class CountFunction(ProperFunction):

    adapts(named['count'])

    parameters = [
            Parameter('condition'),
    ]

    def correlate(self, condition, syntax, parent):
        condition = self.binder.cast(condition, BooleanDomain())
        yield CountBinding(parent, condition, syntax)


class CountBinding(FunctionBinding):

    def __init__(self, parent, condition, syntax):
        super(CountBinding, self).__init__(parent, IntegerDomain(), syntax,
                                           condition=condition)


class CountExpression(FunctionExpression):

    def __init__(self, condition, mark):
        super(CountExpression, self).__init__(IntegerDomain(), mark,
                                              condition=condition)

class CountWrapperExpression(FunctionExpression):

    def __init__(self, aggregate, mark):
        super(CountWrapperExpression, self).__init__(IntegerDomain(), mark,
                                                     aggregate=aggregate)


class CountPhrase(FunctionPhrase):

    def __init__(self, condition, mark):
        super(CountPhrase, self).__init__(IntegerDomain(), True, mark,
                                          condition=condition)


class CountWrapperPhrase(FunctionPhrase):

    def __init__(self, aggregate, mark):
        super(CountWrapperPhrase, self).__init__(IntegerDomain(), False, mark,
                                                 aggregate=aggregate)


class EncodeCount(Encode):

    adapts(CountBinding, Encoder)

    def encode(self):
        condition = self.encoder.encode(self.binding.condition)
        function = CountExpression(condition, self.binding.mark)
        space = self.encoder.relate(self.binding.parent)
        plural_units = [unit for unit in condition.get_units()
                             if not space.spans(unit.space)]
        if not plural_units:
            raise InvalidArgumentError("a plural expression is required",
                                       condition.mark)
        plural_spaces = []
        for unit in plural_units:
            if any(plural_space.dominates(unit.space)
                   for plural_space in plural_spaces):
                continue
            plural_spaces = [plural_space
                             for plural_space in plural_spaces
                             if not unit.space.dominates(plural_space)]
            plural_spaces.append(unit.space)
        if len(plural_spaces) > 1:
            raise InvalidArgumentError("invalid plural expression",
                                       condition.mark)
        plural_space = plural_spaces[0]
        if not plural_space.spans(space):
            raise InvalidArgumentError("invalid plural expression",
                                       condition.mark)
        aggregate = AggregateUnit(function, plural_space, space, function.mark)
        wrapper = CountWrapperExpression(aggregate, aggregate.mark)
        return wrapper


class EvaluateCount(Evaluate):

    adapts(CountExpression, Compiler)

    def evaluate(self, references):
        condition = self.compiler.evaluate(self.expression.condition,
                                           references)
        return CountPhrase(condition, self.expression.mark)


class EvaluateCountWrapper(Evaluate):

    adapts(CountWrapperExpression, Compiler)

    def evaluate(self, references):
        aggregate = self.compiler.evaluate(self.expression.aggregate,
                                           references)
        return CountWrapperPhrase(aggregate, self.expression.mark)


class SerializeCount(Serialize):

    adapts(CountPhrase, Serializer)

    def serialize(self):
        condition = self.serializer.serialize(self.phrase.condition)
        return self.format.count_fn(condition)


class SerializeCountWrapper(Serialize):

    adapts(CountWrapperPhrase, Serializer)

    def serialize(self):
        aggregate = self.serializer.serialize(self.phrase.aggregate)
        return self.format.count_wrapper(aggregate)


function_adapters = find_adapters()


