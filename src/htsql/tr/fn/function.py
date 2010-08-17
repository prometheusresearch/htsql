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


from ...adapter import (Adapter, Utility, adapts, adapts_none,
                        find_adapters, weights)
from ...error import InvalidArgumentError
from ...domain import (Domain, UntypedDomain, BooleanDomain, StringDomain,
                       NumberDomain, IntegerDomain, DecimalDomain, FloatDomain,
                       DateDomain)
from ..binding import (LiteralBinding, OrderedBinding, FunctionBinding,
                       EqualityBinding, InequalityBinding,
                       TotalEqualityBinding, TotalInequalityBinding,
                       ConjunctionBinding, DisjunctionBinding, NegationBinding)
from ..encoder import Encoder, Encode
from ..code import (FunctionExpression, NegationExpression, AggregateUnit,
                    CorrelatedUnit, LiteralExpression, ScreenSpace)
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
        keywords = self.bind_arguments(arguments, parent, syntax.mark)
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
        while arguments:
            argument = arguments.pop(0)
            if argument:
                raise InvalidArgumentError("unexpected argument",
                                           argument[0].mark)
        return keywords


class ProperMethod(ProperFunction):

    adapts_none()

    def bind_arguments(self, arguments, parent, mark):
        arguments = [[parent]] + [list(self.binder.bind(argument, parent))
                                  for argument in arguments]
        return self.check_arguments(arguments, mark)


class LimitMethod(ProperMethod):

    adapts(named['limit'])

    parameters = [
            Parameter('this'),
            Parameter('limit', IntegerDomain),
            Parameter('offset', IntegerDomain, is_mandatory=False),
    ]

    def correlate(self, this, limit, offset, syntax, parent):
        if not (isinstance(limit, LiteralBinding) and
                (limit.value is None or limit.value >= 0)):
            raise InvalidArgumentError("expected a non-negative integer",
                                       limit.mark)
        if not (offset is None or
                (isinstance(offset, LiteralBinding) and
                 (offset.value is None or offset.value >= 0))):
            raise InvalidArgumentError("expected a non-negative integer",
                                       offset.mark)
        limit = limit.value
        if offset is not None:
            offset = offset.value
        yield OrderedBinding(parent, [], limit, offset, syntax)


class OrderMethod(ProperMethod):

    adapts(named['order'])

    parameters = [
            Parameter('this'),
            Parameter('order', is_list=True),
    ]

    def correlate(self, this, order, syntax, parent):
        bindings = order
        order = []
        for binding in bindings:
            domain = self.binder.coerce(binding.domain)
            if domain is None:
                raise InvalidArgumentError("unexpected type",
                                           binding.mark)
            binding = self.binder.cast(binding, domain)
            order.append((binding, +1))
        yield OrderedBinding(parent, order, None, None, syntax)


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

    def bind_function_call(self, syntax, parent):
        if len(syntax.arguments) > 1:
            constructor = self.binder.find_function(self.name+'!')
            return constructor.bind_function_call(syntax, parent)
        return super(DateCastFunction, self).bind_function_call(syntax, parent)


class DateConstructor(ProperFunction):

    adapts(named['date!'])

    parameters = [
            Parameter('year', IntegerDomain),
            Parameter('month', IntegerDomain),
            Parameter('day', IntegerDomain),
    ]

    def correlate(self, year, month, day, syntax, parent):
        yield DateConstructorBinding(parent, DateDomain(), syntax,
                                     year=year, month=month, day=day)


class EqualityOperator(ProperFunction):

    adapts(named['='])

    parameters = [
            Parameter('left'),
            Parameter('right'),
    ]

    def correlate(self, left, right, syntax, parent):
        domain = self.binder.coerce(left.domain,
                                    right.domain)
        if domain is None:
            raise InvalidArgumentError("incompatible types",
                                       syntax.mark)
        domain = self.binder.coerce(domain)
        if domain is None:
            raise InvalidArgumentError("incompatible types",
                                       syntax.mark)
        left = self.binder.cast(left, domain)
        right = self.binder.cast(right, domain)
        yield EqualityBinding(parent, left, right, syntax)


class InequalityOperator(ProperFunction):

    adapts(named['!='])

    parameters = [
            Parameter('left'),
            Parameter('right'),
    ]

    def correlate(self, left, right, syntax, parent):
        domain = self.binder.coerce(left.domain,
                                    right.domain)
        if domain is None:
            raise InvalidArgumentError("incompatible types",
                                       syntax.mark)
        domain = self.binder.coerce(domain)
        if domain is None:
            raise InvalidArgumentError("incompatible types",
                                       syntax.mark)
        left = self.binder.cast(left, domain)
        right = self.binder.cast(right, domain)
        yield InequalityBinding(parent, left, right, syntax)


class TotalEqualityOperator(ProperFunction):

    adapts(named['=='])

    parameters = [
            Parameter('left'),
            Parameter('right'),
    ]

    def correlate(self, left, right, syntax, parent):
        domain = self.binder.coerce(left.domain,
                                    right.domain)
        if domain is None:
            raise InvalidArgumentError("incompatible types",
                                       syntax.mark)
        domain = self.binder.coerce(domain)
        if domain is None:
            raise InvalidArgumentError("incompatible types",
                                       syntax.mark)
        left = self.binder.cast(left, domain)
        right = self.binder.cast(right, domain)
        yield TotalEqualityBinding(parent, left, right, syntax)


class TotalInequalityOperator(ProperFunction):

    adapts(named['!=='])

    parameters = [
            Parameter('left'),
            Parameter('right'),
    ]

    def correlate(self, left, right, syntax, parent):
        domain = self.binder.coerce(left.domain,
                                    right.domain)
        if domain is None:
            raise InvalidArgumentError("incompatible types",
                                       syntax.mark)
        domain = self.binder.coerce(domain)
        if domain is None:
            raise InvalidArgumentError("incompatible types",
                                       syntax.mark)
        left = self.binder.cast(left, domain)
        right = self.binder.cast(right, domain)
        yield TotalInequalityBinding(parent, left, right, syntax)


class ConjunctionOperator(ProperFunction):

    adapts(named['&'])

    parameters = [
            Parameter('left'),
            Parameter('right'),
    ]

    def correlate(self, left, right, syntax, parent):
        left = self.binder.cast(left, BooleanDomain())
        right = self.binder.cast(right, BooleanDomain())
        yield ConjunctionBinding(parent, [left, right], syntax)


class DisjunctionOperator(ProperFunction):

    adapts(named['|'])

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


class ComparisonOperator(ProperFunction):

    adapts_none()

    parameters = [
            Parameter('left'),
            Parameter('right'),
    ]

    direction = None

    def correlate(self, left, right, syntax, parent):
        domain = self.binder.coerce(left.domain, right.domain)
        if domain is None:
            raise InvalidArgumentError("incompatible types", syntax.mark)
        domain = self.binder.coerce(domain)
        if domain is None:
            raise InvalidArgumentError("incompatible types", syntax.mark)
        compare = Compare(domain, left, right, self.direction,
                          self.binder, syntax, parent)
        yield compare()


class LessThanOperator(ComparisonOperator):

    adapts(named['<'])
    direction = '<'


class LessThanOrEqualOperator(ComparisonOperator):

    adapts(named['<='])
    direction = '<='


class GreaterThanOperator(ComparisonOperator):

    adapts(named['>'])
    direction = '>'


class GreaterThanOrEqualOperator(ComparisonOperator):

    adapts(named['>='])
    direction = '>='


class Compare(Adapter):

    adapts(Domain)

    def __init__(self, domain, left, right, direction, binder, syntax, parent):
        self.domain = domain
        self.left = binder.cast(left, domain)
        self.right = binder.cast(right, domain)
        self.direction = direction
        self.binder = binder
        self.syntax = syntax
        self.parent = parent

    def __call__(self):
        raise InvalidArgumentError("unexpected argument types",
                                   self.syntax.mark)


class CompareStrings(Compare):

    adapts(StringDomain)

    def __call__(self):
        return ComparisonBinding(self.parent, BooleanDomain(), self.syntax,
                                 left=self.left, right=self.right,
                                 direction=self.direction)


class CompareNumbers(Compare):

    adapts(NumberDomain)

    def __call__(self):
        return ComparisonBinding(self.parent, BooleanDomain(), self.syntax,
                                 left=self.left, right=self.right,
                                 direction=self.direction)


class UnaryPlusOperator(ProperFunction):

    adapts(named['+_'])

    parameters = [
            Parameter('value'),
    ]

    def correlate(self, value, syntax, parent):
        Implementation = UnaryPlus.realize(value.domain)
        plus = Implementation(value, self.binder, syntax, parent)
        yield plus()


class UnaryMinusOperator(ProperFunction):

    adapts(named['-_'])

    parameters = [
            Parameter('value'),
    ]

    def correlate(self, value, syntax, parent):
        Implementation = UnaryMinus.realize(value.domain)
        minus = Implementation(value, self.binder, syntax, parent)
        yield minus()


class SubtractionOperator(ProperFunction):

    adapts(named['-'])

    parameters = [
            Parameter('left'),
            Parameter('right'),
    ]

    def correlate(self, left, right, syntax, parent):
        Implementation = Subtract.realize(left.domain, right.domain)
        subtract = Implementation(left, right, self.binder, syntax, parent)
        yield subtract()


class AdditionOperator(ProperFunction):

    adapts(named['+'])

    parameters = [
            Parameter('left'),
            Parameter('right'),
    ]

    def correlate(self, left, right, syntax, parent):
        Implementation = Add.realize(left.domain, right.domain)
        add = Implementation(left, right, self.binder, syntax, parent)
        yield add()


class SubtractionOperator(ProperFunction):

    adapts(named['-'])

    parameters = [
            Parameter('left'),
            Parameter('right'),
    ]

    def correlate(self, left, right, syntax, parent):
        Implementation = Subtract.realize(left.domain, right.domain)
        subtract = Implementation(left, right, self.binder, syntax, parent)
        yield subtract()


class MultiplicationOperator(ProperFunction):

    adapts(named['*'])

    parameters = [
            Parameter('left'),
            Parameter('right'),
    ]

    def correlate(self, left, right, syntax, parent):
        Implementation = Multiply.realize(left.domain, right.domain)
        multiply = Implementation(left, right, self.binder, syntax, parent)
        yield multiply()


class DivisionOperator(ProperFunction):

    adapts(named['/'])

    parameters = [
            Parameter('left'),
            Parameter('right'),
    ]

    def correlate(self, left, right, syntax, parent):
        Implementation = Divide.realize(left.domain, right.domain)
        divide = Implementation(left, right, self.binder, syntax, parent)
        yield divide()


class UnaryPlus(Adapter):

    adapts(Domain)

    def __init__(self, value, binder, syntax, parent):
        self.value = value
        self.binder = binder
        self.syntax = syntax
        self.parent = parent

    def __call__(self):
        raise InvalidArgumentError("unexpected argument type",
                                   self.syntax.mark)


class UnaryMinus(Adapter):

    adapts(Domain)

    def __init__(self, value, binder, syntax, parent):
        self.value = value
        self.binder = binder
        self.syntax = syntax
        self.parent = parent

    def __call__(self):
        raise InvalidArgumentError("unexpected argument type",
                                   self.syntax.mark)


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


class Subtract(Adapter):

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


class Multiply(Adapter):

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


class Divide(Adapter):

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
        for parameter in self.function.parameters:
            value = self.binding.arguments[parameter.name]
            if not parameter.is_mandatory and value is None:
                argument = None
            elif parameter.is_list:
                argument = [self.encoder.encode(item) for item in value]
            else:
                argument = self.encoder.encode(value)
            arguments[parameter.name] = argument
        for name in sorted(self.binding.arguments):
            if name not in arguments:
                arguments[name] = self.binding.arguments[name]
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
                                     expression=aggregate)
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
        children = []
        for parameter in self.function.parameters:
            value = self.expression.arguments[parameter.name]
            if not parameter.is_mandatory and value is None:
                argument = None
            elif parameter.is_list:
                argument = [self.compiler.evaluate(item, references)
                            for item in value]
                children.extend(argument)
                is_nullable = is_nullable or any(item.is_nullable
                                                 for item in argument)
            else:
                argument = self.compiler.evaluate(value, references)
                children.append(argument)
                is_nullable = is_nullable or argument.is_nullable
            arguments[parameter.name] = argument
        for name in sorted(self.expression.arguments):
            if name not in arguments:
                arguments[name] = self.expression.arguments[name]
        return self.phrase_class(self.expression.domain, is_nullable,
                                 children, self.expression.mark, **arguments)


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
        for parameter in self.function.parameters:
            value = self.phrase.arguments[parameter.name]
            if not parameter.is_mandatory and value is None:
                argument = None
            elif parameter.is_list:
                argument = [self.serializer.serialize(item) for item in value]
            else:
                argument = self.serializer.serialize(value)
            arguments[parameter.name] = argument
        for name in sorted(self.phrase.arguments):
            if name not in arguments:
                arguments[name] = self.phrase.arguments[name]
        return self.template % arguments


DateConstructorBinding = GenericBinding.factory(DateConstructor)
DateConstructorExpression = GenericExpression.factory(DateConstructor)
DateConstructorPhrase = GenericPhrase.factory(DateConstructor)


EncodeDateConstructor = GenericEncode.factory(DateConstructor,
        DateConstructorBinding, DateConstructorExpression)
EvaluateDateConstructor = GenericEvaluate.factory(DateConstructor,
        DateConstructorExpression, DateConstructorPhrase)
SerializeDateConstructor = GenericSerialize.factory(DateConstructor,
        DateConstructorPhrase,
        "CAST(LPAD(CAST(%(year)s AS TEXT), 4, '0') || '-' ||"
        " LPAD(CAST(%(month)s AS TEXT), 2, '0') || '-' ||"
        " LPAD(CAST(%(day)s AS TEXT), 2, '0') AS DATE)")


ComparisonBinding = GenericBinding.factory(ComparisonOperator)
ComparisonExpression = GenericExpression.factory(ComparisonOperator)
ComparisonPhrase = GenericPhrase.factory(ComparisonOperator)


EncodeComparison = GenericEncode.factory(ComparisonOperator,
        ComparisonBinding, ComparisonExpression)
EvaluateComparison = GenericEvaluate.factory(ComparisonOperator,
        ComparisonExpression, ComparisonPhrase)
SerializeComparison = GenericSerialize.factory(ComparisonOperator,
        ComparisonPhrase, "(%(left)s %(direction)s %(right)s)")


ConcatenationBinding = GenericBinding.factory(AdditionOperator)
ConcatenationExpression = GenericExpression.factory(AdditionOperator)
ConcatenationPhrase = GenericPhrase.factory(AdditionOperator)


EncodeConcatenation = GenericEncode.factory(AdditionOperator,
        ConcatenationBinding, ConcatenationExpression)
EvaluateConcatenation = GenericEvaluate.factory(AdditionOperator,
        ConcatenationExpression, ConcatenationPhrase,
        is_null_regular=False, is_nullable=False)


class SerializeConcatenation(Serialize):

    adapts(ConcatenationPhrase, Serializer)

    def serialize(self):
        left = self.serializer.serialize(self.phrase.left)
        if self.phrase.left.is_nullable:
            left = self.format.concat_wrapper(left)
        right = self.serializer.serialize(self.phrase.right)
        if self.phrase.right.is_nullable:
            right = self.format.concat_wrapper(right)
        return self.format.concat_op(left, right)


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


UnaryPlusBinding = GenericBinding.factory(UnaryPlusOperator)
UnaryPlusExpression = GenericExpression.factory(UnaryPlusOperator)
UnaryPlusPhrase = GenericPhrase.factory(UnaryPlusOperator)


EncodeUnaryPlus = GenericEncode.factory(UnaryPlusOperator,
        UnaryPlusBinding, UnaryPlusExpression)
EvaluateUnaryPlus = GenericEvaluate.factory(UnaryPlusOperator,
        UnaryPlusExpression, UnaryPlusPhrase)
SerializeUnaryPlus = GenericSerialize.factory(UnaryPlusOperator,
        UnaryPlusPhrase, "(+ %(value)s)")


class UnaryPlusForNumber(UnaryPlus):

    adapts(NumberDomain)

    def __call__(self):
        return UnaryPlusBinding(self.parent, self.value.domain, self.syntax,
                                value=self.value)


UnaryMinusBinding = GenericBinding.factory(UnaryMinusOperator)
UnaryMinusExpression = GenericExpression.factory(UnaryMinusOperator)
UnaryMinusPhrase = GenericPhrase.factory(UnaryMinusOperator)


EncodeUnaryMinus = GenericEncode.factory(UnaryMinusOperator,
        UnaryMinusBinding, UnaryMinusExpression)
EvaluateUnaryMinus = GenericEvaluate.factory(UnaryMinusOperator,
        UnaryMinusExpression, UnaryMinusPhrase)
SerializeUnaryMinus = GenericSerialize.factory(UnaryMinusOperator,
        UnaryMinusPhrase, "(- %(value)s)")


class UnaryMinusForNumber(UnaryMinus):

    adapts(NumberDomain)

    def __call__(self):
        return UnaryMinusBinding(self.parent, self.value.domain, self.syntax,
                                value=self.value)


AdditionBinding = GenericBinding.factory(AdditionOperator)
AdditionExpression = GenericExpression.factory(AdditionOperator)
AdditionPhrase = GenericPhrase.factory(AdditionOperator)


EncodeAddition = GenericEncode.factory(AdditionOperator,
        AdditionBinding, AdditionExpression)
EvaluateAddition = GenericEvaluate.factory(AdditionOperator,
        AdditionExpression, AdditionPhrase)
SerializeAddition = GenericSerialize.factory(AdditionOperator,
        AdditionPhrase, "(%(left)s + %(right)s)")


class AddNumbers(Add):

    adapts_none()
    domain = None

    def __call__(self):
        left = self.binder.cast(self.left, self.domain,
                                parent=self.parent)
        right = self.binder.cast(self.right, self.domain,
                                 parent=self.parent)
        return AdditionBinding(self.parent, self.domain, self.syntax,
                               left=left, right=right)


class AddIntegerToInteger(AddNumbers):

    adapts(IntegerDomain, IntegerDomain)
    domain = IntegerDomain()


class AddIntegerToDecimal(AddNumbers):

    adapts(IntegerDomain, DecimalDomain)
    domain = DecimalDomain()


class AddDecimalToInteger(AddNumbers):

    adapts(DecimalDomain, IntegerDomain)
    domain = DecimalDomain()


class AddDecimalToDecimal(AddNumbers):

    adapts(DecimalDomain, DecimalDomain)
    domain = DecimalDomain()


class AddIntegerToFloat(AddNumbers):

    adapts(IntegerDomain, FloatDomain)
    domain = FloatDomain()


class AddDecimalToFloat(AddNumbers):

    adapts(DecimalDomain, FloatDomain)
    domain = FloatDomain()


class AddFloatToInteger(AddNumbers):

    adapts(FloatDomain, IntegerDomain)
    domain = FloatDomain()


class AddFloatToDecimal(AddNumbers):

    adapts(FloatDomain, DecimalDomain)
    domain = FloatDomain()


class AddFloatToFloat(AddNumbers):

    adapts(FloatDomain, FloatDomain)
    domain = FloatDomain()


class AddDateToInteger(Add):

    adapts(DateDomain, IntegerDomain)

    def __call__(self):
        return AdditionBinding(self.parent, DateDomain(), self.syntax,
                               left=self.left, right=self.right)



SubtractionBinding = GenericBinding.factory(SubtractionOperator)
SubtractionExpression = GenericExpression.factory(SubtractionOperator)
SubtractionPhrase = GenericPhrase.factory(SubtractionOperator)


EncodeSubtraction = GenericEncode.factory(SubtractionOperator,
        SubtractionBinding, SubtractionExpression)
EvaluateSubtraction = GenericEvaluate.factory(SubtractionOperator,
        SubtractionExpression, SubtractionPhrase)
SerializeSubtraction = GenericSerialize.factory(SubtractionOperator,
        SubtractionPhrase, "(%(left)s - %(right)s)")


class SubtractNumbers(Subtract):

    adapts_none()
    domain = None

    def __call__(self):
        left = self.binder.cast(self.left, self.domain,
                                parent=self.parent)
        right = self.binder.cast(self.right, self.domain,
                                 parent=self.parent)
        return SubtractionBinding(self.parent, self.domain, self.syntax,
                                  left=left, right=right)


class SubtractIntegerFromInteger(SubtractNumbers):

    adapts(IntegerDomain, IntegerDomain)
    domain = IntegerDomain()


class SubtractIntegerFromDecimal(SubtractNumbers):

    adapts(DecimalDomain, IntegerDomain)
    domain = DecimalDomain()


class SubtractDecimalFromInteger(SubtractNumbers):

    adapts(IntegerDomain, DecimalDomain)
    domain = DecimalDomain()


class SubtractDecimalToDecimal(SubtractNumbers):

    adapts(DecimalDomain, DecimalDomain)
    domain = DecimalDomain()


class SubtractIntegerFromFloat(SubtractNumbers):

    adapts(FloatDomain, IntegerDomain)
    domain = FloatDomain()


class SubtractDecimalFromFloat(SubtractNumbers):

    adapts(FloatDomain, DecimalDomain)
    domain = FloatDomain()


class SubtractFloatFromInteger(SubtractNumbers):

    adapts(IntegerDomain, FloatDomain)
    domain = FloatDomain()


class SubtractFloatFromDecimal(SubtractNumbers):

    adapts(DecimalDomain, FloatDomain)
    domain = FloatDomain()


class SubtractFloatFromFloat(SubtractNumbers):

    adapts(FloatDomain, FloatDomain)
    domain = FloatDomain()


class SubtractIntegerFromDate(Subtract):

    adapts(DateDomain, IntegerDomain)

    def __call__(self):
        return SubtractionBinding(self.parent, DateDomain(), self.syntax,
                                  left=self.left, right=self.right)


class SubtractDateFromDate(Subtract):

    adapts(DateDomain, DateDomain)

    def __call__(self):
        return SubtractionBinding(self.parent, IntegerDomain(), self.syntax,
                                  left=self.left, right=self.right)


MultiplicationBinding = GenericBinding.factory(MultiplicationOperator)
MultiplicationExpression = GenericExpression.factory(MultiplicationOperator)
MultiplicationPhrase = GenericPhrase.factory(MultiplicationOperator)


EncodeMultiplication = GenericEncode.factory(MultiplicationOperator,
        MultiplicationBinding, MultiplicationExpression)
EvaluateMultiplication = GenericEvaluate.factory(MultiplicationOperator,
        MultiplicationExpression, MultiplicationPhrase)
SerializeMultiplication = GenericSerialize.factory(MultiplicationOperator,
        MultiplicationPhrase, "(%(left)s * %(right)s)")


class MultiplyNumbers(Multiply):

    adapts_none()
    domain = None

    def __call__(self):
        left = self.binder.cast(self.left, self.domain,
                                parent=self.parent)
        right = self.binder.cast(self.right, self.domain,
                                 parent=self.parent)
        return MultiplicationBinding(self.parent, self.domain, self.syntax,
                                     left=left, right=right)


class MultiplyIntegerByInteger(MultiplyNumbers):

    adapts(IntegerDomain, IntegerDomain)
    domain = IntegerDomain()


class MultiplyIntegerByDecimal(MultiplyNumbers):

    adapts(IntegerDomain, DecimalDomain)
    domain = DecimalDomain()


class MultiplyDecimalByInteger(MultiplyNumbers):

    adapts(DecimalDomain, IntegerDomain)
    domain = DecimalDomain()


class MultiplyDecimalByDecimal(MultiplyNumbers):

    adapts(DecimalDomain, DecimalDomain)
    domain = DecimalDomain()


class MultiplyIntegerByFloat(MultiplyNumbers):

    adapts(IntegerDomain, FloatDomain)
    domain = FloatDomain()


class MultiplyDecimalByFloat(MultiplyNumbers):

    adapts(DecimalDomain, FloatDomain)
    domain = FloatDomain()


class MultiplyFloatByInteger(MultiplyNumbers):

    adapts(FloatDomain, IntegerDomain)
    domain = FloatDomain()


class MultiplyFloatByDecimal(MultiplyNumbers):

    adapts(FloatDomain, DecimalDomain)
    domain = FloatDomain()


class MultiplyFloatByFloat(MultiplyNumbers):

    adapts(FloatDomain, FloatDomain)
    domain = FloatDomain()


DivisionBinding = GenericBinding.factory(DivisionOperator)
DivisionExpression = GenericExpression.factory(DivisionOperator)
DivisionPhrase = GenericPhrase.factory(DivisionOperator)


EncodeDivision = GenericEncode.factory(DivisionOperator,
        DivisionBinding, DivisionExpression)
EvaluateDivision = GenericEvaluate.factory(DivisionOperator,
        DivisionExpression, DivisionPhrase)
SerializeDivision = GenericSerialize.factory(DivisionOperator,
        DivisionPhrase, "(%(left)s / %(right)s)")


class DivideNumbers(Divide):

    adapts_none()
    domain = None

    def __call__(self):
        left = self.binder.cast(self.left, self.domain,
                                parent=self.parent)
        right = self.binder.cast(self.right, self.domain,
                                 parent=self.parent)
        return DivisionBinding(self.parent, self.domain, self.syntax,
                               left=left, right=right)


class DivideIntegerByInteger(DivideNumbers):

    adapts(IntegerDomain, IntegerDomain)
    domain = DecimalDomain()


class DivideIntegerByDecimal(DivideNumbers):

    adapts(IntegerDomain, DecimalDomain)
    domain = DecimalDomain()


class DivideDecimalByInteger(DivideNumbers):

    adapts(DecimalDomain, IntegerDomain)
    domain = DecimalDomain()


class DivideDecimalByDecimal(DivideNumbers):

    adapts(DecimalDomain, DecimalDomain)
    domain = DecimalDomain()


class DivideIntegerByFloat(DivideNumbers):

    adapts(IntegerDomain, FloatDomain)
    domain = FloatDomain()


class DivideDecimalByFloat(DivideNumbers):

    adapts(DecimalDomain, FloatDomain)
    domain = FloatDomain()


class DivideFloatByInteger(DivideNumbers):

    adapts(FloatDomain, IntegerDomain)
    domain = FloatDomain()


class DivideFloatByDecimal(DivideNumbers):

    adapts(FloatDomain, DecimalDomain)
    domain = FloatDomain()


class DivideFloatByFloat(DivideNumbers):

    adapts(FloatDomain, FloatDomain)
    domain = FloatDomain()


class RoundFunction(ProperFunction):

    adapts(named['round'])

    parameters = [
            Parameter('value'),
            Parameter('digits', IntegerDomain, is_mandatory=False),
    ]

    def correlate(self, value, digits, syntax, parent):
        Implementation = Round.realize(value.domain)
        round = Implementation(value, digits, self.binder, syntax, parent)
        yield round()


class Round(Adapter):

    adapts(Domain)

    def __init__(self, value, digits, binder, syntax, parent):
        self.value = value
        self.digits = digits
        self.binder = binder
        self.syntax = syntax
        self.parent = parent

    def __call__(self):
        raise InvalidArgumentError("unexpected argument types",
                                   self.syntax.mark)


class RoundDecimal(Round):

    adapts_none()

    def __call__(self):
        value = self.binder.cast(self.value, DecimalDomain(),
                                 parent=self.parent)
        digits = self.digits
        if digits is None:
            digits = LiteralBinding(self.parent, 0, IntegerDomain(),
                                    self.syntax)
        return RoundBinding(self.parent, DecimalDomain(), self.syntax,
                            value=value, digits=digits)


class RoundDecimalFromInteger(RoundDecimal):

    adapts(IntegerDomain)


class RoundDecimalFromDecimal(RoundDecimal):

    adapts(DecimalDomain)


class RoundFloat(Round):

    adapts(FloatDomain)

    def __call__(self):
        if self.digits is not None:
            raise InvalidArgumentError("unexpected argument", self.digits.mark)
        return RoundBinding(self.parent, FloatDomain(), self.syntax,
                            value=self.value, digits=None)


RoundBinding = GenericBinding.factory(RoundFunction)
RoundExpression = GenericExpression.factory(RoundFunction)
RoundPhrase = GenericPhrase.factory(RoundFunction)


EncodeRound = GenericEncode.factory(RoundFunction,
        RoundBinding, RoundExpression)
EvaluateRound = GenericEvaluate.factory(RoundFunction,
        RoundExpression, RoundPhrase)


class SerializeRound(Serialize):

    adapts(RoundPhrase, Serializer)

    def serialize(self):
        value = self.serializer.serialize(self.phrase.value)
        digits = None
        if self.phrase.digits is not None:
            digits = self.serializer.serialize(self.phrase.digits)
        return self.format.round_fn(value, digits)


class IsNullFunction(ProperFunction):

    adapts(named['is_null'])

    parameters = [
            Parameter('expression'),
    ]

    def correlate(self, expression, syntax, parent):
        domain = self.binder.coerce(expression.domain)
        if domain is None:
            raise InvalidArgumentError("unexpected domain",
                                       expression.mark)
        expression = self.binder.cast(expression, domain)
        yield IsNullBinding(parent, BooleanDomain(), syntax,
                            expression=expression)


IsNullBinding = GenericBinding.factory(IsNullFunction)
IsNullExpression = GenericExpression.factory(IsNullFunction)
IsNullPhrase = GenericPhrase.factory(IsNullFunction)


EncodeIsNull = GenericEncode.factory(IsNullFunction,
        IsNullBinding, IsNullExpression)
EvaluateIsNull = GenericEvaluate.factory(IsNullFunction,
        IsNullExpression, IsNullPhrase,
        is_null_regular=False, is_nullable=False)
SerializeIsNull = GenericSerialize.factory(IsNullFunction,
        IsNullPhrase, "(%(expression)s IS NULL)")


class NullIfMethod(ProperMethod):

    adapts(named['null_if'])

    parameters = [
            Parameter('this'),
            Parameter('expressions', is_list=True),
    ]

    def correlate(self, this, expressions, syntax, parent):
        domain = this.domain
        for expression in expressions:
            domain = self.binder.coerce(domain, expression.domain)
            if domain is None:
                raise InvalidArgumentError("unexpected domain",
                                           expression.mark)
        domain = self.binder.coerce(domain)
        if domain is None:
            raise InvalidArgumentError("inexpected domain",
                                       this.mark)
        this = self.binder.cast(this, domain)
        expressions = [self.binder.cast(expression, domain)
                       for expression in expressions]
        yield NullIfBinding(parent, domain, syntax,
                            this=this, expressions=expressions)


NullIfBinding = GenericBinding.factory(NullIfMethod)
NullIfExpression = GenericExpression.factory(NullIfMethod)
NullIfPhrase = GenericPhrase.factory(NullIfMethod)


EncodeNullIf = GenericEncode.factory(NullIfMethod,
        NullIfBinding, NullIfExpression)
EvaluateNullIf = GenericEvaluate.factory(NullIfMethod,
        NullIfExpression, NullIfPhrase,
        is_null_regular=False)


class SerializeNullIf(Serialize):

    adapts(NullIfPhrase, Serializer)

    def serialize(self):
        left = self.serializer.serialize(self.phrase.this)
        for expression in self.phrase.expressions:
            right = self.serializer.serialize(expression)
            left = self.format.nullif_fn(left, right)
        return left


class IfNullMethod(ProperMethod):

    adapts(named['if_null'])

    parameters = [
            Parameter('this'),
            Parameter('expressions', is_list=True),
    ]

    def correlate(self, this, expressions, syntax, parent):
        domain = this.domain
        for expression in expressions:
            domain = self.binder.coerce(domain, expression.domain)
            if domain is None:
                raise InvalidArgumentError("unexpected domain",
                                           expression.mark)
        domain = self.binder.coerce(domain)
        if domain is None:
            raise InvalidArgumentError("inexpected domain",
                                       this.mark)
        this = self.binder.cast(this, domain)
        expressions = [self.binder.cast(expression, domain)
                       for expression in expressions]
        yield IfNullBinding(parent, domain, syntax,
                            this=this, expressions=expressions)


IfNullBinding = GenericBinding.factory(IfNullMethod)
IfNullExpression = GenericExpression.factory(IfNullMethod)
IfNullPhrase = GenericPhrase.factory(IfNullMethod)


EncodeIfNull = GenericEncode.factory(IfNullMethod,
        IfNullBinding, IfNullExpression)
EvaluateIfNull = GenericEvaluate.factory(IfNullMethod,
        IfNullExpression, IfNullPhrase)


class SerializeIfNull(Serialize):

    adapts(IfNullPhrase, Serializer)

    def serialize(self):
        arguments = [self.serializer.serialize(self.phrase.this)]
        for expression in self.phrase.expressions:
            arguments.append(self.serializer.serialize(expression))
        return self.format.coalesce_fn(arguments)


class IfFunction(ProperFunction):

    adapts(named['if'])

    parameters = [
            Parameter('conditions', is_list=True),
            Parameter('values', is_list=True),
    ]

    def bind_arguments(self, arguments, parent, mark):
        conditions = []
        values = []
        for index, argument in enumerate(arguments):
            argument = self.binder.bind_one(argument, parent)
            if (index % 2 == 0) and index < len(arguments)-1:
                conditions.append(argument)
            else:
                values.append(argument)
        arguments = [conditions, values]
        return self.check_arguments(arguments, mark)

    def correlate(self, conditions, values, syntax, parent):
        conditions = [self.binder.cast(condition, BooleanDomain())
                      for condition in conditions]
        domain = values[0].domain
        for value in values[1:]:
            domain = self.binder.coerce(domain, value.domain)
            if domain is None:
                raise InvalidArgumentError("unexpected domain", value.mark)
        domain = self.binder.coerce(domain)
        if domain is None:
            raise InvalidArgumentError("unexpected domain", syntax.mark)
        values = [self.binder.cast(value, domain) for value in values]
        yield IfBinding(parent, domain, syntax,
                        conditions=conditions, values=values)


IfBinding = GenericBinding.factory(IfFunction)
IfExpression = GenericExpression.factory(IfFunction)
IfPhrase = GenericPhrase.factory(IfFunction)


EncodeIf = GenericEncode.factory(IfFunction,
        IfBinding, IfExpression)
EvaluateIf = GenericEvaluate.factory(IfFunction,
        IfExpression, IfPhrase,
        is_null_regular=False)


class SerializeIf(Serialize):

    adapts(IfPhrase, Serializer)

    def serialize(self):
        conditions = [self.serializer.serialize(condition)
                      for condition in self.phrase.conditions]
        values = [self.serializer.serialize(value)
                  for value in self.phrase.values]
        return self.format.if_fn(conditions, values)


class SwitchFunction(ProperFunction):

    adapts(named['switch'])

    parameters = [
            Parameter('token'),
            Parameter('items', is_list=True),
            Parameter('values', is_list=True),
    ]

    def bind_arguments(self, arguments, parent, mark):
        if not arguments:
            return self.check_arguments([], mark)
        token = self.binder.bind_one(arguments[0], parent)
        items = []
        values = []
        for index, argument in enumerate(arguments[1:]):
            argument = self.binder.bind_one(argument, parent)
            if (index % 2 == 0) and index < len(arguments)-2:
                items.append(argument)
            else:
                values.append(argument)
        arguments = [[token], items, values]
        return self.check_arguments(arguments, mark)

    def correlate(self, token, items, values, syntax, parent):
        token_domain = token.domain
        for item in items:
            token_domain = self.binder.coerce(token_domain, item.domain)
            if token_domain is None:
                raise InvalidArgumentError("unexpected domain", item.mark)
        token_domain = self.binder.coerce(token_domain)
        if token_domain is None:
            raise InvalidArgumentError("unexpected domain", token.mark)
        token = self.binder.cast(token, token_domain)
        items = [self.binder.cast(item, token_domain) for item in items]
        domain = values[0].domain
        for value in values[1:]:
            domain = self.binder.coerce(domain, value.domain)
            if domain is None:
                raise InvalidArgumentError("unexpected domain", value.mark)
        domain = self.binder.coerce(domain)
        if domain is None:
            raise InvalidArgumentError("unexpected domain", syntax.mark)
        values = [self.binder.cast(value, domain) for value in values]
        yield SwitchBinding(parent, domain, syntax,
                            token=token, items=items, values=values)


SwitchBinding = GenericBinding.factory(SwitchFunction)
SwitchExpression = GenericExpression.factory(SwitchFunction)
SwitchPhrase = GenericPhrase.factory(SwitchFunction)


EncodeSwitch = GenericEncode.factory(SwitchFunction,
        SwitchBinding, SwitchExpression)
EvaluateSwitch = GenericEvaluate.factory(SwitchFunction,
        SwitchExpression, SwitchPhrase,
        is_null_regular=False)


class SerializeSwitch(Serialize):

    adapts(SwitchPhrase, Serializer)

    def serialize(self):
        token = self.serializer.serialize(self.phrase.token)
        items = [self.serializer.serialize(item)
                 for item in self.phrase.items]
        values = [self.serializer.serialize(value)
                  for value in self.phrase.values]
        return self.format.switch_fn(token, items, values)


class LengthMethod(ProperMethod):

    adapts(named['length'])

    parameters = [
            Parameter('this')
    ]

    def correlate(self, this, syntax, parent):
        Implementation = Length.realize(this.domain)
        length = Implementation(this, self.binder, syntax, parent)
        yield length()


class Length(Adapter):

    adapts(Domain)

    def __init__(self, this, binder, syntax, parent):
        self.this = this
        self.binder = binder
        self.syntax = syntax
        self.parent = parent

    def __call__(self):
        raise InvalidArgumentError("unexpected type", self.syntax.mark)


class TextLength(Length):

    adapts_none()

    def __call__(self):
        this = self.binder.cast(self.this, StringDomain())
        return TextLengthBinding(self.parent, IntegerDomain(), self.syntax,
                                 this=this)


class TextLengthOfString(TextLength):

    adapts(StringDomain)


class TextLengthOfUntyped(TextLength):

    adapts(UntypedDomain)


TextLengthBinding = GenericBinding.factory(LengthMethod)
TextLengthExpression = GenericExpression.factory(LengthMethod)
TextLengthPhrase = GenericPhrase.factory(LengthMethod)


EncodeTextLength = GenericEncode.factory(LengthMethod,
        TextLengthBinding, TextLengthExpression)
EvaluateTextLength = GenericEvaluate.factory(LengthMethod,
        TextLengthExpression, TextLengthPhrase)
SerializeTextLength = GenericSerialize.factory(LengthMethod,
        TextLengthPhrase, "CHARACTER_LENGTH(%(this)s)")


class ContainsOperator(ProperFunction):

    adapts(named['~'])

    parameters = [
            Parameter('left'),
            Parameter('right'),
    ]

    def correlate(self, left, right, syntax, parent):
        Implementation = Contains.realize(left.domain, right.domain)
        length = Implementation(left, right, self.binder, syntax, parent)
        yield length()


class Contains(Adapter):

    adapts(Domain, Domain)

    def __init__(self, left, right, binder, syntax, parent):
        self.left = left
        self.right = right
        self.binder = binder
        self.syntax = syntax
        self.parent = parent

    def __call__(self):
        raise InvalidArgumentError("unexpected types", self.syntax.mark)


class ContainsStrings(Contains):

    adapts_none()

    def __call__(self):
        left = self.binder.cast(self.left, StringDomain(),
                                parent=self.parent)
        right = self.binder.cast(self.right, StringDomain(),
                                 parent=self.parent)
        return ContainsBinding(self.parent, BooleanDomain(), self.syntax,
                               left=left, right=right)


class ContainsStringInString(ContainsStrings):

    adapts(StringDomain, StringDomain)


class ContainsUntypedInString(ContainsStrings):

    adapts(StringDomain, UntypedDomain)


class ContainsStringInUntyped(ContainsStrings):

    adapts(UntypedDomain, StringDomain)


class ContainsUntypedInUntyped(ContainsStrings):

    adapts(UntypedDomain, UntypedDomain)


ContainsBinding = GenericBinding.factory(ContainsOperator)
ContainsExpression = GenericExpression.factory(ContainsOperator)
ContainsPhrase = GenericPhrase.factory(ContainsOperator)


EncodeContains = GenericEncode.factory(ContainsOperator,
        ContainsBinding, ContainsExpression)
EvaluateContains = GenericEvaluate.factory(ContainsOperator,
        ContainsExpression, ContainsPhrase)
SerializeContains = GenericSerialize.factory(ContainsOperator,
        ContainsPhrase, "(POSITION(LOWER(%(right)s) IN LOWER(%(left)s)) > 0)")


class FormatFunctions(Format):

    weights(0)

    def concat_op(self, left, right):
        return "(%s || %s)" % (left, right)

    def concat_wrapper(self, expr):
        return "COALESCE(%s, '')"

    def count_fn(self, condition):
        return "COUNT(NULLIF(%s, FALSE))" % condition

    def count_wrapper(self, aggregate):
        return "COALESCE(%s, 0)" % aggregate

    def nullif_fn(self, left, right):
        return "NULLIF(%s, %s)" % (left, right)

    def coalesce_fn(self, arguments):
        return "COALESCE(%s)" % ", ".join(arguments)

    def round_fn(self, value, digits=None):
        if digits is None:
            return "ROUND(%s)" % value
        else:
            return "ROUND(%s, %s)" % (value, digits)

    def if_fn(self, predicates, values):
        assert len(predicates) >= 1
        assert len(values)-1 <= len(predicates) <= len(values)
        default = None
        if len(predicates) == len(values)-1:
            default = values.pop()
        chunks = []
        chunks.append('CASE')
        for predicate, value in zip(predicates, values):
            chunks.append('WHEN')
            chunks.append(predicate)
            chunks.append('THEN')
            chunks.append(value)
        if default is not None:
            chunks.append('ELSE')
            chunks.append(default)
        chunks.append('END')
        return "(%s)" % ' '.join(chunks)

    def switch_fn(self, token, items, values):
        assert len(items) >= 1
        assert len(values)-1 <= len(items) <= len(values)
        default = None
        if len(items) == len(values)-1:
            default = values.pop()
        chunks = []
        chunks.append('CASE')
        chunks.append(token)
        for item, value in zip(items, values):
            chunks.append('WHEN')
            chunks.append(item)
            chunks.append('THEN')
            chunks.append(value)
        if default is not None:
            chunks.append('ELSE')
            chunks.append(default)
        chunks.append('END')
        return "(%s)" % ' '.join(chunks)


class CountFunction(ProperFunction):

    adapts(named['count'])

    parameters = [
            Parameter('expression'),
    ]

    def correlate(self, expression, syntax, parent):
        expression = self.binder.cast(expression, BooleanDomain())
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
        CountWrapperPhrase, "COALESCE(%(expression)s, 0)")


class ExistsFunction(ProperFunction):

    adapts(named['exists'])

    parameters = [
            Parameter('expression'),
    ]

    def correlate(self, expression, syntax, parent):
        expression = self.binder.cast(expression, BooleanDomain())
        yield ExistsBinding(parent, BooleanDomain(), syntax,
                            expression=expression)


ExistsBinding = GenericBinding.factory(ExistsFunction)
ExistsWrapperExpression = GenericExpression.factory(ExistsFunction)
ExistsWrapperPhrase = GenericPhrase.factory(ExistsFunction)


class EveryFunction(ProperFunction):

    adapts(named['every'])

    parameters = [
            Parameter('expression'),
    ]

    def correlate(self, expression, syntax, parent):
        expression = self.binder.cast(expression, BooleanDomain())
        yield EveryBinding(parent, BooleanDomain(), syntax,
                           expression=expression)


EveryBinding = GenericBinding.factory(EveryFunction)
EveryWrapperExpression = GenericExpression.factory(EveryFunction)
EveryWrapperPhrase = GenericPhrase.factory(EveryFunction)


class EncodeExistsEvery(Encode):

    adapts_none()
    is_exists = False
    is_every = False

    def encode(self):
        expression = self.encoder.encode(self.binding.expression)
        if self.is_every:
            expression = NegationExpression(expression, expression.mark)
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
        plural_space = ScreenSpace(plural_space, expression, self.binding.mark)
        expression = LiteralExpression(True, BooleanDomain(),
                                       self.binding.mark)
        aggregate = CorrelatedUnit(expression, plural_space, space,
                                   self.binding.mark)
        if self.is_exists:
            wrapper = ExistsWrapperExpression(self.binding.domain,
                                              self.binding.mark,
                                              expression=aggregate)
        if self.is_every:
            wrapper = EveryWrapperExpression(self.binding.domain,
                                             self.binding.mark,
                                             expression=aggregate)
        return wrapper


class EncodeExists(EncodeExistsEvery):

    adapts(ExistsBinding, Encoder)
    is_exists = True


EvaluateExistsWrapper = GenericEvaluate.factory(ExistsFunction,
        ExistsWrapperExpression, ExistsWrapperPhrase)
SerializeExistsWrapper = GenericSerialize.factory(ExistsFunction,
        ExistsWrapperPhrase, "EXISTS(%(expression)s)")


class EncodeEvery(EncodeExistsEvery):

    adapts(EveryBinding, Encoder)
    is_every = True


EvaluateEveryWrapper = GenericEvaluate.factory(EveryFunction,
        EveryWrapperExpression, EveryWrapperPhrase)
SerializeEveryWrapper = GenericSerialize.factory(EveryFunction,
        EveryWrapperPhrase, "NOT EXISTS(%(expression)s)")


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
        MinWrapperPhrase, "%(expression)s")


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
        MaxWrapperPhrase, "%(expression)s")


function_adapters = find_adapters()


