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
            Parameter('expression'),
    ]
    output_domain = None

    def correlate(self, expression, syntax, parent):
        yield self.binder.cast(expression, self.output_domain, syntax, parent)


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


class NegationOperator(ProperFunction):

    adapts(named['!_'])

    parameters = [
            Parameter('term'),
    ]

    def correlate(self, term, syntax, parent):
        term = self.binder.cast(term, BooleanDomain())
        yield NegationBinding(parent, term, syntax)


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


class GenericBinding(FunctionBinding):

    function = None

    @classmethod
    def factory(cls, function):
        name = function.__name__ + 'Binding'
        binding_class = type(name, (cls,), {'function': function})
        return binding_class


class GenericExpression(FunctionExpression):

    function = None

    @classmethod
    def factory(cls, function):
        name = function.__name__ + 'Expression'
        expression_class = type(name, (cls,), {'function': function})
        return expression_class


class GenericPhrase(FunctionPhrase):

    function = None

    @classmethod
    def factory(cls, function):
        name = function.__name__ + 'Phrase'
        phrase_class = type(name, (cls,), {'function': function})
        return phrase_class


class GenericEncode(Encode):

    adapts_none()

    function = None
    binding_class = None
    expression_class = None

    @classmethod
    def factory(cls, function, binding_class, expression_class):
        name = 'Encode' + function.__name__
        signature = (binding_class, Encoder)
        encode_class = type(name, (cls,),
                            {'function': function,
                             'signature': signature,
                             'binding_class': binding_class,
                             'expression_class': expression_class})
        return encode_class

    def encode(self):
        arguments = {}
        for name in sorted(self.binding.arguments):
            value = self.binding.arguments[name]
            if isinstance(value, list):
                value = [self.encoder.encode(item) for item in value]
            elif value is not None:
                value = self.encoder.encode(value)
            arguments[name] = value
        return self.expression_class(self.binding.domain, self.binding.mark,
                                     **arguments)


class GenericAggregateEncode(Encode):

    adapts_none()

    function = None
    binding_class = None
    expression_class = None
    wrapper_class = None

    @classmethod
    def factory(cls, function, binding_class, expression_class, wrapper_class):
        name = 'Encode' + function.__name__
        signature = (binding_class, Encoder)
        encode_class = type(name, (cls,),
                            {'function': function,
                             'signature': signature,
                             'binding_class': binding_class,
                             'expression_class': expression_class,
                             'wrapper_class': wrapper_class})
        return encode_class

    def encode(self):
        expression = self.encoder.encode(self.binding.expression)
        expression = self.expression_class(self.binding.domain,
                                           self.binding.mark,
                                           expression=expression)
        space = self.encoder.relate(self.binding.parent)
        plural_units = [unit for unit in expression.get_units()
                             if not space.spans(unit.space)]
        if not plural_units:
            raise InvalidArgumentError("a plural expression is required",
                                       expression.mark)
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
                                       expression.mark)
        plural_space = plural_spaces[0]
        if not plural_space.spans(space):
            raise InvalidArgumentError("invalid plural expression",
                                       expression.mark)
        aggregate = AggregateUnit(expression, plural_space, space,
                                  expression.mark)
        wrapper = self.wrapper_class(self.binding.domain, self.binding.mark,
                                     aggregate=aggregate)
        return wrapper


class GenericEvaluate(Evaluate):

    adapts_none()

    function = None
    expression_class = None
    phrase_class = None
    is_null_regular = True
    is_nullable = True

    @classmethod
    def factory(cls, function, expression_class, phrase_class,
                is_null_regular=True, is_nullable=True):
        name = 'Evaluate' + function.__name__
        signature = (expression_class, Compiler)
        evaluate_class = type(name, (cls,),
                              {'function': function,
                               'signature': signature,
                               'expression_class': expression_class,
                               'phrase_class': phrase_class,
                               'is_null_regular': is_null_regular,
                               'is_nullable': is_nullable})
        return evaluate_class

    def evaluate(self, references):
        is_nullable = self.is_nullable
        if self.is_null_regular:
            is_nullable = False
        arguments = {}
        for name in sorted(self.expression.arguments):
            value = self.expression.arguments[name]
            if isinstance(value, list):
                value = [self.compiler.evaluate(item, references)
                         for item in value]
                if self.is_null_regular:
                    for item in value:
                        is_nullable = is_nullable or item.is_nullable
            elif value is not None:
                value = self.compiler.evaluate(value, references)
                if self.is_null_regular:
                    is_nullable = is_nullable or value.is_nullable
            arguments[name] = value
        return self.phrase_class(self.expression.domain, is_nullable,
                                 self.expression.mark, **arguments)


class GenericSerialize(Serialize):

    adapts_none()

    function = None
    phrase_class = None
    template = None

    @classmethod
    def factory(cls, function, phrase_class, template):
        name = 'Serialize' + function.__name__
        signature = (phrase_class, Serializer)
        serialize_class = type(name, (cls,),
                               {'function': function,
                                'signature': signature,
                                'phrase_class': phrase_class,
                                'template': template})
        return serialize_class

    def serialize(self):
        arguments = {}
        for name in sorted(self.phrase.arguments):
            value = self.phrase.arguments[name]
            if isinstance(value, list):
                value = [self.serializer.serialize(item) for item in value]
            elif value is not None:
                value = self.serializer.serialize(value)
            arguments[name] = value
        return self.template % arguments


ConcatenationBinding = GenericBinding.factory(AdditionOperator)
ConcatenationExpression = GenericExpression.factory(AdditionOperator)
ConcatenationPhrase = GenericPhrase.factory(AdditionOperator)


EncodeConcatenation = GenericEncode.factory(AdditionOperator,
        ConcatenationBinding, ConcatenationExpression)
EvaluateConcatenation = GenericEvaluate.factory(AdditionOperator,
        ConcatenationExpression, ConcatenationPhrase,
        is_null_regular=False, is_nullable=False)
SerializeConcatenation = GenericSerialize.factory(AdditionOperator,
        ConcatenationPhrase,
        "(COALESCE(%(left)s, '') || COALESCE(%(right)s, ''))")


class Concatenate(Add):

    adapts_none()

    def __call__(self):
        left = self.binder.cast(self.left, StringDomain(),
                                parent=self.parent)
        right = self.binder.cast(self.right, StringDomain(),
                                 parent=self.parent)
        return ConcatenationBinding(self.parent, StringDomain(), self.syntax,
                                    left=left, right=right)


class ConcatenateStringToString(Concatenate):

    adapts(StringDomain, StringDomain)


class ConcatenateStringToUntyped(Concatenate):

    adapts(StringDomain, UntypedDomain)


class ConcatenateUntypedToString(Concatenate):

    adapts(UntypedDomain, StringDomain)


class ConcatenateUntypedToUntyped(Concatenate):

    adapts(UntypedDomain, UntypedDomain)


class FormatFunctions(Format):

    def concat_op(self, left, right):
        return "(%s || %s)" % (left, right)

    def count_fn(self, condition):
        return "COUNT(NULLIF(%s, FALSE))" % condition

    def count_wrapper(self, aggregate):
        return "COALESCE(%s, 0)" % aggregate


class CountFunction(ProperFunction):

    adapts(named['count'])

    parameters = [
            Parameter('condition'),
    ]

    def correlate(self, condition, syntax, parent):
        expression = self.binder.cast(condition, BooleanDomain())
        yield CountBinding(parent, IntegerDomain(), syntax,
                           expression=expression)


CountBinding = GenericBinding.factory(CountFunction)
CountExpression = GenericExpression.factory(CountFunction)
CountWrapperExpression = GenericExpression.factory(CountFunction)
CountPhrase = GenericPhrase.factory(CountFunction)
CountWrapperPhrase = GenericPhrase.factory(CountFunction)


EncodeCount = GenericAggregateEncode.factory(CountFunction,
        CountBinding, CountExpression, CountWrapperExpression)
EvaluateCount = GenericEvaluate.factory(CountFunction,
        CountExpression, CountPhrase)
EvaluateCountWrapper = GenericEvaluate.factory(CountFunction,
        CountWrapperExpression, CountWrapperPhrase)
SerializeCount = GenericSerialize.factory(CountFunction,
        CountPhrase, "COUNT(NULLIF(%(expression)s, FALSE))")
SerializeCountWrapper = GenericSerialize.factory(CountFunction,
        CountWrapperPhrase, "COALESCE(%(aggregate)s, 0)")


class ExistsFunction(ProperFunction):

    adapts(named['exists'])

    parameters = [
            Parameter('condition'),
    ]

    def correlate(self, condition, syntax, parent):
        expression = self.binder.cast(condition, BooleanDomain())
        yield ExistsBinding(parent, BooleanDomain(), syntax,
                            expression=expression)


ExistsBinding = GenericBinding.factory(ExistsFunction)
ExistsExpression = GenericExpression.factory(ExistsFunction)
ExistsWrapperExpression = GenericExpression.factory(ExistsFunction)
ExistsPhrase = GenericPhrase.factory(ExistsFunction)
ExistsWrapperPhrase = GenericPhrase.factory(ExistsFunction)


EncodeExists = GenericAggregateEncode.factory(ExistsFunction,
        ExistsBinding, ExistsExpression, ExistsWrapperExpression)
EvaluateExists = GenericEvaluate.factory(ExistsFunction,
        ExistsExpression, ExistsPhrase)
EvaluateExistsWrapper = GenericEvaluate.factory(ExistsFunction,
        ExistsWrapperExpression, ExistsWrapperPhrase)
SerializeExists = GenericSerialize.factory(ExistsFunction,
        ExistsPhrase, "BOOL_OR(%(expression)s IS TRUE)")
SerializeExistsWrapper = GenericSerialize.factory(ExistsFunction,
        ExistsWrapperPhrase, "COALESCE(%(aggregate)s, FALSE)")


class EveryFunction(ProperFunction):

    adapts(named['every'])

    parameters = [
            Parameter('condition'),
    ]

    def correlate(self, condition, syntax, parent):
        expression = self.binder.cast(condition, BooleanDomain())
        yield EveryBinding(parent, BooleanDomain(), syntax,
                           expression=expression)


EveryBinding = GenericBinding.factory(EveryFunction)
EveryExpression = GenericExpression.factory(EveryFunction)
EveryWrapperExpression = GenericExpression.factory(EveryFunction)
EveryPhrase = GenericPhrase.factory(EveryFunction)
EveryWrapperPhrase = GenericPhrase.factory(EveryFunction)


EncodeEvery = GenericAggregateEncode.factory(EveryFunction,
        EveryBinding, EveryExpression, EveryWrapperExpression)
EvaluateEvery = GenericEvaluate.factory(EveryFunction,
        EveryExpression, EveryPhrase)
EvaluateEveryWrapper = GenericEvaluate.factory(EveryFunction,
        EveryWrapperExpression, EveryWrapperPhrase)
SerializeEvery = GenericSerialize.factory(EveryFunction,
        EveryPhrase, "BOOL_AND(%(expression)s IS TRUE)")
SerializeEveryWrapper = GenericSerialize.factory(EveryFunction,
        EveryWrapperPhrase, "COALESCE(%(aggregate)s, TRUE)")


class MinFunction(ProperFunction):

    adapts(named['min'])

    parameters = [
            Parameter('expression'),
    ]

    def correlate(self, expression, syntax, parent):
        Implementation = Min.realize(expression.domain)
        function = Implementation(expression, self.binder, syntax, parent)
        yield function()


class Min(Adapter):

    adapts(Domain)

    def __init__(self, expression, binder, syntax, parent):
        self.expression = expression
        self.binder = binder
        self.syntax = syntax
        self.parent = parent

    def __call__(self):
        expression = self.expression
        return MinBinding(self.parent, expression.domain, self.syntax,
                          expression=expression)


class MinString(Min):

    adapts(StringDomain)


class MinInteger(Min):

    adapts(IntegerDomain)


class MinDecimal(Min):

    adapts(DecimalDomain)


class MinFloat(Min):

    adapts(FloatDomain)


class MinDate(Min):

    adapts(DateDomain)


MinBinding = GenericBinding.factory(MinFunction)
MinExpression = GenericExpression.factory(MinFunction)
MinWrapperExpression = GenericExpression.factory(MinFunction)
MinPhrase = GenericPhrase.factory(MinFunction)
MinWrapperPhrase = GenericPhrase.factory(MinFunction)


EncodeMin = GenericAggregateEncode.factory(MinFunction,
        MinBinding, MinExpression, MinWrapperExpression)
EvaluateMin = GenericEvaluate.factory(MinFunction,
        MinExpression, MinPhrase)
EvaluateMinWrapper = GenericEvaluate.factory(MinFunction,
        MinWrapperExpression, MinWrapperPhrase)
SerializeMin = GenericSerialize.factory(MinFunction,
        MinPhrase, "MIN(%(expression)s)")
SerializeMinWrapper = GenericSerialize.factory(MinFunction,
        MinWrapperPhrase, "%(aggregate)s")


class MaxFunction(ProperFunction):

    adapts(named['max'])

    parameters = [
            Parameter('expression'),
    ]

    def correlate(self, expression, syntax, parent):
        Implementation = Max.realize(expression.domain)
        function = Implementation(expression, self.binder, syntax, parent)
        yield function()


class Max(Adapter):

    adapts(Domain)

    def __init__(self, expression, binder, syntax, parent):
        self.expression = expression
        self.binder = binder
        self.syntax = syntax
        self.parent = parent

    def __call__(self):
        expression = self.expression
        return MaxBinding(self.parent, expression.domain, self.syntax,
                          expression=expression)


class MaxString(Max):

    adapts(StringDomain)


class MaxInteger(Max):

    adapts(IntegerDomain)


class MaxDecimal(Max):

    adapts(DecimalDomain)


class MaxFloat(Max):

    adapts(FloatDomain)


class MaxDate(Max):

    adapts(DateDomain)


MaxBinding = GenericBinding.factory(MaxFunction)
MaxExpression = GenericExpression.factory(MaxFunction)
MaxWrapperExpression = GenericExpression.factory(MaxFunction)
MaxPhrase = GenericPhrase.factory(MaxFunction)
MaxWrapperPhrase = GenericPhrase.factory(MaxFunction)


EncodeMax = GenericAggregateEncode.factory(MaxFunction,
        MaxBinding, MaxExpression, MaxWrapperExpression)
EvaluateMax = GenericEvaluate.factory(MaxFunction,
        MaxExpression, MaxPhrase)
EvaluateMaxWrapper = GenericEvaluate.factory(MaxFunction,
        MaxWrapperExpression, MaxWrapperPhrase)
SerializeMax = GenericSerialize.factory(MaxFunction,
        MaxPhrase, "MAX(%(expression)s)")
SerializeMaxWrapper = GenericSerialize.factory(MaxFunction,
        MaxWrapperPhrase, "%(aggregate)s")


function_adapters = find_adapters()


