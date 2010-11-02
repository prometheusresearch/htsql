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


from ...adapter import (Adapter, Utility, Protocol, adapts, adapts_none,
                        adapts_many, named)
from ...error import InvalidArgumentError
from ...domain import (Domain, UntypedDomain, BooleanDomain, StringDomain,
                       NumberDomain, IntegerDomain, DecimalDomain, FloatDomain,
                       DateDomain)
from ..syntax import NumberSyntax, StringSyntax, IdentifierSyntax
from ..binding import (LiteralBinding, SortBinding, FunctionBinding,
                       EqualityBinding, TotalEqualityBinding,
                       ConjunctionBinding, DisjunctionBinding, NegationBinding,
                       CastBinding, WrapperBinding, TitleBinding,
                       DirectionBinding)
from ..encode import Encode
from ..code import (FunctionCode, NegationCode, ScalarUnit, AggregateUnit,
                    CorrelatedUnit, LiteralCode, FilteredSpace)
from ..compile import Evaluate
from ..frame import FunctionPhrase
from ..serializer import Serializer, Format, Serialize
from ..coerce import coerce
from ..lookup import lookup


class Function(Protocol):

    def __init__(self, syntax, state):
        self.syntax = syntax
        self.state = state
        self.mark = syntax.mark

    @classmethod
    def dispatch(self, syntax, *args, **kwds):
        return syntax.name

    def __call__(self):
        raise InvalidArgumentError("unknown function or operator %s"
                                   % self.syntax.name, self.mark)


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

    parameters = []

    def __call__(self):
        keywords = self.bind_arguments()
        return self.correlate(**keywords)

    def bind_arguments(self):
        arguments = [list(self.state.bind_all(argument))
                     for argument in self.syntax.arguments]
        return self.check_arguments(arguments)

    def check_arguments(self, arguments):
        arguments = arguments[:]
        keywords = {}
        for idx, parameter in enumerate(self.parameters):
            value = None
            if not arguments:
                if parameter.is_mandatory:
                    raise InvalidArgumentError("missing argument %s"
                                               % parameter.name, self.mark)
            elif parameter.is_list:
                value = []
                if len(arguments) > 1 and idx == len(self.parameters)-1:
                    while arguments:
                        argument = arguments.pop(0)
                        if len(argument) != 1:
                            raise InvalidArgumentError("invalid argument %s"
                                                       % parameter.name,
                                                       self.mark)
                        value.append(argument[0])
                else:
                    argument = arguments.pop(0)
                    if parameter.is_mandatory and not argument:
                        raise InvalidArgumentError("missing argument %s"
                                                   % parameter.name,
                                                   self.mark)
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
                                                   % parameter.name,
                                                   self.mark)
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

    def bind_arguments(self):
        arguments = ([[self.state.base]] +
                     [list(self.state.bind_all(argument))
                      for argument in self.syntax.arguments])
        return self.check_arguments(arguments)


class RootFunction(Function):

    named('root')

    def __call__(self):
        if len(self.syntax.arguments) != 0:
            raise InvalidArgumentError("unexpected arguments",
                                       self.syntax.mark)
        yield WrapperBinding(self.state.root, self.syntax)


class ThisFunction(Function):

    named('this')

    def __call__(self):
        if len(self.syntax.arguments) != 0:
            raise InvalidArgumentError("unexpected arguments",
                                       self.syntax.mark)
        yield WrapperBinding(self.state.base, self.syntax)


class CrossFunction(Function):

    named('cross')

    def __call__(self):
        if len(self.syntax.arguments) < 1:
            raise InvalidArgumentError("an argument expected",
                                       self.syntax.mark)
        elif len(self.syntax.arguments) > 1:
            raise InvalidArgumentError("unexpected arguments",
                                       self.syntax.mark)
        argument = self.syntax.arguments[0]
        if not isinstance(argument, IdentifierSyntax):
            raise InvalidArgumentError("an identifier expected",
                                       argument.mark)
        binding = lookup(self.state.root, argument)
        if binding is None:
            raise InvalidArgumentError("unknown identifier",
                                       argument.mark)
        binding = binding.clone(base=self.state.base)
        yield WrapperBinding(binding, self.syntax)


class AsFunction(ProperFunction):

    named('as')

    parameters = [
            Parameter('base'),
            Parameter('title', StringDomain),
    ]

    def bind_arguments(self):
        if len(self.syntax.arguments) != 2:
            raise InvalidArgumentError("expected two arguments",
                                       self.syntax.mark)
        base = self.state.bind(self.syntax.arguments[0])
        title_syntax = self.syntax.arguments[1]
        if not isinstance(title_syntax, (StringSyntax, IdentifierSyntax)):
            raise InvalidArgumentError("expected a string literal"
                                       " or an identifier",
                                       title_syntax.mark)
        return {'base': base, 'title': title_syntax.value}

    def correlate(self, base, title):
        yield TitleBinding(base, title, self.syntax)


class AscOrderFunction(ProperFunction):

    named('_+')

    parameters = [
            Parameter('base', is_mandatory=False),
    ]

    def correlate(self, base):
        yield DirectionBinding(base, +1, self.syntax)


class DescOrderFunction(ProperFunction):

    named('_-')

    parameters = [
            Parameter('base', is_mandatory=False),
    ]

    def correlate(self, base):
        yield DirectionBinding(base, -1, self.syntax)


class LimitMethod(ProperMethod):

    named('limit')

    parameters = [
            Parameter('this'),
            Parameter('limit', IntegerDomain),
            Parameter('offset', IntegerDomain, is_mandatory=False),
    ]

    def bind_arguments(self):
        if not (1 <= len(self.syntax.arguments) <= 2):
            raise InvalidArgumentError("expected one or two arguments",
                                       self.syntax.mark)
        values = []
        for argument in self.syntax.arguments:
            if not isinstance(argument, NumberSyntax):
                raise InvalidArgumentError("expected a non-negative integer",
                                           argument.mark)
            try:
                value = int(argument.value)
            except ValueError:
                raise InvalidArgumentError("expected a non-negative integer",
                                           argument.mark)
            if not (value >= 0):
                raise InvalidArgumentError("expected a non-negative integer",
                                           argument.mark)
            values.append(value)
        if len(values) == 1:
            limit = values[0]
            offset = None
        else:
            limit, offset = values
        return {'this': self.state.base, 'limit': limit, 'offset': offset}

    def correlate(self, this, limit, offset):
        yield SortBinding(this, [], limit, offset, self.syntax)


class OrderMethod(ProperMethod):

    named('sort')

    parameters = [
            Parameter('this'),
            Parameter('order', is_list=True),
    ]

    def correlate(self, this, order):
        bindings = order
        order = []
        for binding in bindings:
            domain = coerce(binding.domain)
            if domain is None:
                raise InvalidArgumentError("unexpected type",
                                           binding.mark)
            binding = CastBinding(binding, domain, binding.syntax)
            order.append(binding)
        yield SortBinding(this, order, None, None, self.syntax)


class NullFunction(ProperFunction):

    named('null')

    def correlate(self):
        yield LiteralBinding(None, UntypedDomain(), self.syntax)


class TrueFunction(ProperFunction):

    named('true')

    def correlate(self):
        yield LiteralBinding(True, coerce(BooleanDomain()), self.syntax)


class FalseFunction(ProperFunction):

    named('false')

    def correlate(self):
        yield LiteralBinding(False, coerce(BooleanDomain()), self.syntax)


class CastFunction(ProperFunction):

    parameters = [
            Parameter('op'),
    ]
    output_domain = None

    def correlate(self, op):
        domain = coerce(self.output_domain)
        yield CastBinding(op, domain, self.syntax)


class BooleanCastFunction(CastFunction):

    named('boolean')
    output_domain = BooleanDomain()


class StringCastFunction(CastFunction):

    named('string')
    output_domain = StringDomain()


class IntegerCastFunction(CastFunction):

    named('integer')
    output_domain = IntegerDomain()


class DecimalCastFunction(CastFunction):

    named('decimal')
    output_domain = DecimalDomain()


class FloatCastFunction(CastFunction):

    named('float')
    output_domain = FloatDomain()


class DateCastFunction(CastFunction):

    named('date')
    output_domain = DateDomain()

    def __call__(self):
        if len(self.syntax.arguments) > 1:
            constructor = DateConstructor(self.syntax, self.state)
            return constructor()
        return super(DateCastFunction, self).__call__()


class DateConstructor(ProperFunction):

    named('date!')

    parameters = [
            Parameter('year', IntegerDomain),
            Parameter('month', IntegerDomain),
            Parameter('day', IntegerDomain),
    ]

    def correlate(self, year, month, day):
        yield DateConstructorBinding(coerce(DateDomain()), self.syntax,
                                     year=year, month=month, day=day)


class EqualityOperator(ProperFunction):

    named('=')

    parameters = [
            Parameter('left'),
            Parameter('right'),
    ]

    def correlate(self, left, right):
        domain = coerce(left.domain, right.domain)
        if domain is None:
            raise InvalidArgumentError("incompatible types",
                                       self.syntax.mark)
        left = CastBinding(left, domain, left.syntax)
        right = CastBinding(right, domain, right.syntax)
        yield EqualityBinding(left, right, self.syntax)


class InequalityOperator(ProperFunction):

    named('!=')

    parameters = [
            Parameter('left'),
            Parameter('right'),
    ]

    def correlate(self, left, right):
        domain = coerce(left.domain, right.domain)
        if domain is None:
            raise InvalidArgumentError("incompatible types",
                                       self.syntax.mark)
        left = CastBinding(left, domain, left.syntax)
        right = CastBinding(right, domain, right.syntax)
        yield NegationBinding(EqualityBinding(left, right, self.syntax),
                              self.syntax)


class TotalEqualityOperator(ProperFunction):

    named('==')

    parameters = [
            Parameter('left'),
            Parameter('right'),
    ]

    def correlate(self, left, right):
        domain = coerce(left.domain, right.domain)
        if domain is None:
            raise InvalidArgumentError("incompatible types",
                                       self.syntax.mark)
        left = CastBinding(left, domain, left.syntax)
        right = CastBinding(right, domain, right.syntax)
        yield TotalEqualityBinding(left, right, self.syntax)


class TotalInequalityOperator(ProperFunction):

    named('!==')

    parameters = [
            Parameter('left'),
            Parameter('right'),
    ]

    def correlate(self, left, right):
        domain = coerce(left.domain, right.domain)
        if domain is None:
            raise InvalidArgumentError("incompatible types",
                                       self.syntax.mark)
        left = CastBinding(left, domain, left.syntax)
        right = CastBinding(right, domain, right.syntax)
        yield NegationBinding(TotalEqualityBinding(left, right, self.syntax),
                              self.syntax)


class ConjunctionOperator(ProperFunction):

    named('&')

    parameters = [
            Parameter('left'),
            Parameter('right'),
    ]

    def correlate(self, left, right):
        left = CastBinding(left, coerce(BooleanDomain()), left.syntax)
        right = CastBinding(right, coerce(BooleanDomain()), right.syntax)
        yield ConjunctionBinding([left, right], self.syntax)


class DisjunctionOperator(ProperFunction):

    named('|')

    parameters = [
            Parameter('left'),
            Parameter('right'),
    ]

    def correlate(self, left, right):
        left = CastBinding(left, coerce(BooleanDomain()), left.syntax)
        right = CastBinding(right, coerce(BooleanDomain()), right.syntax)
        yield DisjunctionBinding([left, right], self.syntax)


class NegationOperator(ProperFunction):

    named('!_')

    parameters = [
            Parameter('term'),
    ]

    def correlate(self, term):
        term = CastBinding(term, coerce(BooleanDomain()), term.syntax)
        yield NegationBinding(term, self.syntax)


class ComparisonOperator(ProperFunction):

    parameters = [
            Parameter('left'),
            Parameter('right'),
    ]

    direction = None

    def correlate(self, left, right):
        domain = coerce(left.domain, right.domain)
        if domain is None:
            raise InvalidArgumentError("incompatible types", self.syntax.mark)
        compare = Compare(domain, left, right, self.direction,
                          self.state, self.syntax)
        yield compare()


class LessThanOperator(ComparisonOperator):

    named('<')
    direction = '<'


class LessThanOrEqualOperator(ComparisonOperator):

    named('<=')
    direction = '<='


class GreaterThanOperator(ComparisonOperator):

    named('>')
    direction = '>'


class GreaterThanOrEqualOperator(ComparisonOperator):

    named('>=')
    direction = '>='


class Compare(Adapter):

    adapts(Domain)

    def __init__(self, domain, left, right, direction, state, syntax):
        self.domain = domain
        self.left = CastBinding(left, domain, left.syntax)
        self.right = CastBinding(right, domain, right.syntax)
        self.direction = direction
        self.state = state
        self.syntax = syntax

    def __call__(self):
        raise InvalidArgumentError("unexpected argument types",
                                   self.syntax.mark)


class CompareStrings(Compare):

    adapts(StringDomain)

    def __call__(self):
        return ComparisonBinding(coerce(BooleanDomain()), self.syntax,
                                 left=self.left, right=self.right,
                                 direction=self.direction)


class CompareNumbers(Compare):

    adapts(NumberDomain)

    def __call__(self):
        return ComparisonBinding(coerce(BooleanDomain()), self.syntax,
                                 left=self.left, right=self.right,
                                 direction=self.direction)


class UnaryPlusOperator(ProperFunction):

    named('+_')

    parameters = [
            Parameter('value'),
    ]

    def correlate(self, value):
        Implementation = UnaryPlus.realize((type(value.domain),))
        plus = Implementation(value, self.state, self.syntax)
        yield plus()


class UnaryMinusOperator(ProperFunction):

    named('-_')

    parameters = [
            Parameter('value'),
    ]

    def correlate(self, value):
        Implementation = UnaryMinus.realize((type(value.domain),))
        minus = Implementation(value, self.state, self.syntax)
        yield minus()


class SubtractionOperator(ProperFunction):

    named('-')

    parameters = [
            Parameter('left'),
            Parameter('right'),
    ]

    def correlate(self, left, right):
        signature = (type(left.domain), type(right.domain))
        Implementation = Subtract.realize(signature)
        subtract = Implementation(left, right, self.state, self.syntax)
        yield subtract()


class AdditionOperator(ProperFunction):

    named('+')

    parameters = [
            Parameter('left'),
            Parameter('right'),
    ]

    def correlate(self, left, right):
        signature = (type(left.domain), type(right.domain))
        Implementation = Add.realize(signature)
        add = Implementation(left, right, self.state, self.syntax)
        yield add()


class SubtractionOperator(ProperFunction):

    named('-')

    parameters = [
            Parameter('left'),
            Parameter('right'),
    ]

    def correlate(self, left, right):
        signature = (type(left.domain), type(right.domain))
        Implementation = Subtract.realize(signature)
        subtract = Implementation(left, right, self.state, self.syntax)
        yield subtract()


class MultiplicationOperator(ProperFunction):

    named('*')

    parameters = [
            Parameter('left'),
            Parameter('right'),
    ]

    def correlate(self, left, right):
        signature = (type(left.domain), type(right.domain))
        Implementation = Multiply.realize(signature)
        multiply = Implementation(left, right, self.state, self.syntax)
        yield multiply()


class DivisionOperator(ProperFunction):

    named('/')

    parameters = [
            Parameter('left'),
            Parameter('right'),
    ]

    def correlate(self, left, right):
        signature = (type(left.domain), type(right.domain))
        Implementation = Divide.realize(signature)
        divide = Implementation(left, right, self.state, self.syntax)
        yield divide()


class UnaryPlus(Adapter):

    adapts(Domain)

    def __init__(self, value, state, syntax):
        self.value = value
        self.state = state
        self.syntax = syntax

    def __call__(self):
        raise InvalidArgumentError("unexpected argument type",
                                   self.syntax.mark)


class UnaryMinus(Adapter):

    adapts(Domain)

    def __init__(self, value, state, syntax):
        self.value = value
        self.state = state
        self.syntax = syntax

    def __call__(self):
        raise InvalidArgumentError("unexpected argument type",
                                   self.syntax.mark)


class Add(Adapter):

    adapts(Domain, Domain)

    def __init__(self, left, right, state, syntax):
        self.left = left
        self.right = right
        self.state = state
        self.syntax = syntax

    def __call__(self):
        raise InvalidArgumentError("unexpected argument types",
                                   self.syntax.mark)


class Subtract(Adapter):

    adapts(Domain, Domain)

    def __init__(self, left, right, state, syntax):
        self.left = left
        self.right = right
        self.state = state
        self.syntax = syntax

    def __call__(self):
        raise InvalidArgumentError("unexpected argument types",
                                   self.syntax.mark)


class Multiply(Adapter):

    adapts(Domain, Domain)

    def __init__(self, left, right, state, syntax):
        self.left = left
        self.right = right
        self.state = state
        self.syntax = syntax

    def __call__(self):
        raise InvalidArgumentError("unexpected argument types",
                                   self.syntax.mark)


class Divide(Adapter):

    adapts(Domain, Domain)

    def __init__(self, left, right, state, syntax):
        self.left = left
        self.right = right
        self.state = state
        self.syntax = syntax

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


class GenericExpression(FunctionCode):

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
        signature = (binding_class,)
        encode_class = type(name, (cls,),
                            {'function': function,
                             'signatures': [signature],
                             'binding_class': binding_class,
                             'expression_class': expression_class})
        return encode_class

    def __call__(self):
        arguments = {}
        for parameter in self.function.parameters:
            value = self.binding.arguments[parameter.name]
            if not parameter.is_mandatory and value is None:
                argument = None
            elif parameter.is_list:
                argument = [self.state.encode(item) for item in value]
            else:
                argument = self.state.encode(value)
            arguments[parameter.name] = argument
        for name in sorted(self.binding.arguments):
            if name not in arguments:
                arguments[name] = self.binding.arguments[name]
        return self.expression_class(self.binding.domain, self.binding,
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
        signature = (binding_class,)
        encode_class = type(name, (cls,),
                            {'function': function,
                             'signatures': [signature],
                             'binding_class': binding_class,
                             'expression_class': expression_class,
                             'wrapper_class': wrapper_class})
        return encode_class

    def __call__(self):
        op = self.state.encode(self.binding.op)
        op = self.expression_class(self.binding.domain,
                                   self.binding, op=op)
        space = self.state.relate(self.binding.base)
        plural_units = [unit for unit in op.units
                             if not space.spans(unit.space)]
        if not plural_units:
            raise InvalidArgumentError("a plural operand is required", op.mark)
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
            raise InvalidArgumentError("invalid plural operand", op.mark)
        plural_space = plural_spaces[0]
        if not plural_space.spans(space):
            raise InvalidArgumentError("invalid plural operand", op.mark)
        aggregate = AggregateUnit(op, plural_space, space, self.binding)
        wrapper = self.wrapper_class(self.binding.domain, self.binding,
                                     op=aggregate)
        wrapper = ScalarUnit(wrapper, space, self.binding)
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
        signature = (expression_class,)
        evaluate_class = type(name, (cls,),
                              {'function': function,
                               'signatures': [signature],
                               'expression_class': expression_class,
                               'phrase_class': phrase_class,
                               'is_null_regular': is_null_regular,
                               'is_nullable': is_nullable})
        return evaluate_class

    def __call__(self):
        is_nullable = self.is_nullable
        if self.is_null_regular:
            is_nullable = False
        arguments = {}
        children = []
        for parameter in self.function.parameters:
            value = self.code.arguments[parameter.name]
            if not parameter.is_mandatory and value is None:
                argument = None
            elif parameter.is_list:
                argument = [self.state.evaluate(item)
                            for item in value]
                children.extend(argument)
                is_nullable = is_nullable or any(item.is_nullable
                                                 for item in argument)
            else:
                argument = self.state.evaluate(value)
                children.append(argument)
                is_nullable = is_nullable or argument.is_nullable
            arguments[parameter.name] = argument
        for name in sorted(self.code.arguments):
            if name not in arguments:
                arguments[name] = self.code.arguments[name]
        return self.phrase_class(self.code.domain, is_nullable,
                                 self.code, **arguments)


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
                                'signatures': [signature],
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

    adapts_many((StringDomain, StringDomain),
                (StringDomain, UntypedDomain),
                (UntypedDomain, StringDomain),
                (UntypedDomain, UntypedDomain))

    def __call__(self):
        left = CastBinding(self.left, coerce(StringDomain()),
                           self.left.syntax)
        right = CastBinding(self.right, coerce(StringDomain()),
                            self.right.syntax)
        return ConcatenationBinding(coerce(StringDomain()), self.syntax,
                                    left=left, right=right)


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
        return UnaryPlusBinding(self.value.domain, self.syntax,
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
        return UnaryMinusBinding(self.value.domain, self.syntax,
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
        domain = coerce(self.domain)
        left = CastBinding(self.left, domain, self.left.syntax)
        right = CastBinding(self.right, domain, self.right.syntax)
        return AdditionBinding(domain, self.syntax, left=left, right=right)


class AddInteger(AddNumbers):

    adapts(IntegerDomain, IntegerDomain)
    domain = IntegerDomain()


class AddDecimal(AddNumbers):

    adapts_many((DecimalDomain, DecimalDomain),
                (DecimalDomain, IntegerDomain),
                (IntegerDomain, DecimalDomain))
    domain = DecimalDomain()


class AddFloat(AddNumbers):

    adapts_many((FloatDomain, FloatDomain),
                (FloatDomain, DecimalDomain),
                (FloatDomain, IntegerDomain),
                (DecimalDomain, FloatDomain),
                (IntegerDomain, FloatDomain))
    domain = FloatDomain()


class AddDateToInteger(Add):

    adapts(DateDomain, IntegerDomain)

    def __call__(self):
        return AdditionBinding(coerce(DateDomain()), self.syntax,
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
        left = CastBinding(self.left, self.domain, self.syntax)
        right = CastBinding(self.right, self.domain, self.syntax)
        return SubtractionBinding(self.domain, self.syntax,
                                  left=left, right=right)


class SubtractInteger(SubtractNumbers):

    adapts(IntegerDomain, IntegerDomain)
    domain = IntegerDomain()


class SubtractDecimal(SubtractNumbers):

    adapts_many((DecimalDomain, DecimalDomain),
                (DecimalDomain, IntegerDomain),
                (IntegerDomain, DecimalDomain))
    domain = DecimalDomain()


class SubtractFloat(SubtractNumbers):

    adapts_many((FloatDomain, FloatDomain),
                (FloatDomain, DecimalDomain),
                (FloatDomain, IntegerDomain),
                (DecimalDomain, FloatDomain),
                (IntegerDomain, FloatDomain))
    domain = FloatDomain()


class SubtractIntegerFromDate(Subtract):

    adapts(DateDomain, IntegerDomain)

    def __call__(self):
        return SubtractionBinding(coerce(DateDomain()), self.syntax,
                                  left=self.left, right=self.right)


class SubtractDateFromDate(Subtract):

    adapts(DateDomain, DateDomain)

    def __call__(self):
        return SubtractionBinding(coerce(IntegerDomain()), self.syntax,
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
        left = CastBinding(self.left, self.domain, self.syntax)
        right = CastBinding(self.right, self.domain, self.syntax)
        return MultiplicationBinding(coerce(self.domain), self.syntax,
                                     left=left, right=right)


class MultiplyInteger(MultiplyNumbers):

    adapts(IntegerDomain, IntegerDomain)
    domain = IntegerDomain()


class MultiplyDecimal(MultiplyNumbers):

    adapts_many((DecimalDomain, DecimalDomain),
                (DecimalDomain, IntegerDomain),
                (IntegerDomain, DecimalDomain))
    domain = DecimalDomain()


class MultiplyFloat(MultiplyNumbers):

    adapts_many((FloatDomain, FloatDomain),
                (FloatDomain, DecimalDomain),
                (FloatDomain, IntegerDomain),
                (DecimalDomain, FloatDomain),
                (IntegerDomain, FloatDomain))
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
        left = CastBinding(self.left, self.domain, self.syntax)
        right = CastBinding(self.right, self.domain, self.syntax)
        return DivisionBinding(coerce(self.domain), self.syntax,
                               left=left, right=right)


class DivideDecimal(DivideNumbers):

    adapts_many((DecimalDomain, DecimalDomain),
                (DecimalDomain, IntegerDomain),
                (IntegerDomain, DecimalDomain),
                (IntegerDomain, IntegerDomain))
    domain = DecimalDomain()


class DivideFloat(DivideNumbers):

    adapts_many((FloatDomain, FloatDomain),
                (FloatDomain, DecimalDomain),
                (FloatDomain, IntegerDomain),
                (DecimalDomain, FloatDomain),
                (IntegerDomain, FloatDomain))
    domain = FloatDomain()


class RoundFunction(ProperFunction):

    named('round')

    parameters = [
            Parameter('value'),
            Parameter('digits', IntegerDomain, is_mandatory=False),
    ]

    def correlate(self, value, digits):
        Implementation = Round.realize((type(value.domain),))
        round = Implementation(value, digits, self.state, self.syntax)
        yield round()


class Round(Adapter):

    adapts(Domain)

    def __init__(self, value, digits, state, syntax):
        self.value = value
        self.digits = digits
        self.state = state
        self.syntax = syntax

    def __call__(self):
        raise InvalidArgumentError("unexpected argument types",
                                   self.syntax.mark)


class RoundDecimal(Round):

    adapts_many((IntegerDomain,),
                (DecimalDomain,))

    def __call__(self):
        value = CastBinding(self.value, coerce(DecimalDomain()), self.syntax)
        digits = self.digits
        if digits is None:
            digits = LiteralBinding(0, coerce(IntegerDomain()),
                                    self.syntax)
        return RoundBinding(coerce(DecimalDomain()), self.syntax,
                            value=value, digits=digits)


class RoundFloat(Round):

    adapts(FloatDomain)

    def __call__(self):
        if self.digits is not None:
            raise InvalidArgumentError("unexpected argument", self.digits.mark)
        return RoundBinding(coerce(FloatDomain()), self.syntax,
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

    named('is_null')

    parameters = [
            Parameter('op'),
    ]

    def correlate(self, op):
        domain = coerce(op.domain)
        if domain is None:
            raise InvalidArgumentError("unexpected domain",
                                       op.mark)
        op = CastBinding(op, domain, self.syntax)
        yield IsNullBinding(coerce(BooleanDomain()), self.syntax, op=op)


IsNullBinding = GenericBinding.factory(IsNullFunction)
IsNullExpression = GenericExpression.factory(IsNullFunction)
IsNullPhrase = GenericPhrase.factory(IsNullFunction)


EncodeIsNull = GenericEncode.factory(IsNullFunction,
        IsNullBinding, IsNullExpression)
EvaluateIsNull = GenericEvaluate.factory(IsNullFunction,
        IsNullExpression, IsNullPhrase,
        is_null_regular=False, is_nullable=False)
SerializeIsNull = GenericSerialize.factory(IsNullFunction,
        IsNullPhrase, "(%(op)s IS NULL)")


class NullIfMethod(ProperMethod):

    named('null_if')

    parameters = [
            Parameter('this'),
            Parameter('ops', is_list=True),
    ]

    def correlate(self, this, ops):
        domain = coerce(this.domain, *(op.domain for op in ops))
        if domain is None:
            raise InvalidArgumentError("unexpected domain", op.mark)
        this = CastBinding(this, domain, this.syntax)
        ops = [CastBinding(op, domain, op.syntax) for op in ops]
        yield NullIfBinding(domain, self.syntax, this=this, ops=ops)


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
        for op in self.phrase.ops:
            right = self.serializer.serialize(op)
            left = self.format.nullif_fn(left, right)
        return left


class IfNullMethod(ProperMethod):

    named('if_null')

    parameters = [
            Parameter('this'),
            Parameter('ops', is_list=True),
    ]

    def correlate(self, this, ops):
        domain = coerce(this.domain, *(op.domain for op in ops))
        if domain is None:
            raise InvalidArgumentError("unexpected domain", op.mark)
        this = CastBinding(this, domain, this.syntax)
        ops = [CastBinding(op, domain, op.syntax) for op in ops]
        yield IfNullBinding(domain, self.syntax, this=this, ops=ops)


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
        for op in self.phrase.ops:
            arguments.append(self.serializer.serialize(op))
        return self.format.coalesce_fn(arguments)


class IfFunction(ProperFunction):

    named('if')

    parameters = [
            Parameter('conditions', is_list=True),
            Parameter('values', is_list=True),
    ]

    def bind_arguments(self):
        conditions = []
        values = []
        for index, argument in enumerate(self.syntax.arguments):
            argument = self.state.bind(argument)
            if (index % 2 == 0) and index < len(self.syntax.arguments)-1:
                conditions.append(argument)
            else:
                values.append(argument)
        arguments = [conditions, values]
        return self.check_arguments(arguments)

    def correlate(self, conditions, values):
        conditions = [CastBinding(condition, coerce(BooleanDomain()),
                                  condition.syntax)
                      for condition in conditions]
        domain = coerce(*(value.domain for value in values))
        if domain is None:
            raise InvalidArgumentError("unexpected domain", syntax.mark)
        values = [CastBinding(value, domain, value.syntax)
                  for value in values]
        yield IfBinding(domain, self.syntax,
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

    named('switch')

    parameters = [
            Parameter('token'),
            Parameter('items', is_list=True),
            Parameter('values', is_list=True),
    ]

    def bind_arguments(self):
        if not self.syntax.arguments:
            return self.check_arguments([])
        token = self.state.bind(self.syntax.arguments[0])
        items = []
        values = []
        for index, argument in enumerate(self.syntax.arguments[1:]):
            argument = self.state.bind(argument)
            if (index % 2 == 0) and index < len(self.syntax.arguments)-2:
                items.append(argument)
            else:
                values.append(argument)
        arguments = [[token], items, values]
        return self.check_arguments(arguments)

    def correlate(self, token, items, values):
        token_domain = coerce(token.domain, *(item.domain for item in items))
        if token_domain is None:
            raise InvalidArgumentError("unexpected domain", token.mark)
        token = CastBinding(token, token_domain, token.syntax)
        items = [CastBinding(item, token_domain, item.syntax)
                 for item in items]
        domain = coerce(*(value.domain for value in values))
        if domain is None:
            raise InvalidArgumentError("unexpected domain", self.syntax.mark)
        values = [CastBinding(value, domain, value.syntax) for value in values]
        yield SwitchBinding(domain, self.syntax,
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

    named('length')

    parameters = [
            Parameter('this')
    ]

    def correlate(self, this):
        Implementation = Length.realize((type(this.domain),))
        length = Implementation(this, self.state, self.syntax)
        yield length()


class Length(Adapter):

    adapts(Domain)

    def __init__(self, this, state, syntax):
        self.this = this
        self.state = state
        self.syntax = syntax

    def __call__(self):
        raise InvalidArgumentError("unexpected type", self.syntax.mark)


class TextLength(Length):

    adapts_many((StringDomain,),
                (UntypedDomain,))

    def __call__(self):
        this = CastBinding(self.this, coerce(StringDomain()), self.this.syntax)
        return TextLengthBinding(coerce(IntegerDomain()), self.syntax,
                                 this=this)


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

    named('~')

    parameters = [
            Parameter('left'),
            Parameter('right'),
    ]

    def correlate(self, left, right):
        signature = (type(left.domain), type(right.domain))
        Implementation = Contains.realize(signature)
        contains = Implementation(left, right, self.state, self.syntax)
        yield contains()


class NotContainsOperator(ProperFunction):

    named('!~')

    parameters = [
            Parameter('left'),
            Parameter('right'),
    ]

    def correlate(self, left, right):
        signature = (type(left.domain), type(right.domain))
        Implementation = Contains.realize(signature)
        contains = Implementation(left, right, self.state, self.syntax)
        yield NegationBinding(contains(), self.syntax)


class Contains(Adapter):

    adapts(Domain, Domain)

    def __init__(self, left, right, state, syntax):
        self.left = left
        self.right = right
        self.state = state
        self.syntax = syntax

    def __call__(self):
        raise InvalidArgumentError("unexpected types", self.syntax.mark)


class ContainsStrings(Contains):

    adapts_many((StringDomain, StringDomain),
                (StringDomain, UntypedDomain),
                (UntypedDomain, StringDomain),
                (UntypedDomain, UntypedDomain))

    def __call__(self):
        left = CastBinding(self.left, coerce(StringDomain()),
                           self.left.syntax)
        right = CastBinding(self.right, coerce(StringDomain()),
                            self.right.syntax)
        return ContainsBinding(coerce(BooleanDomain()), self.syntax,
                               left=left, right=right)


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

    @classmethod
    def dominates(cls, component):
        return True

    def concat_op(self, left, right):
        return "(%s || %s)" % (left, right)

    def concat_wrapper(self, expr):
        return "COALESCE(%s, '')" % expr

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

    named('count')

    parameters = [
            Parameter('op'),
    ]

    def correlate(self, op):
        op = CastBinding(op, coerce(BooleanDomain()), op.syntax)
        yield CountBinding(coerce(IntegerDomain()), self.syntax,
                           base=self.state.base, op=op)


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
        CountPhrase, "COUNT(NULLIF(%(op)s, FALSE))")
SerializeCountWrapper = GenericSerialize.factory(CountFunction,
        CountWrapperPhrase, "COALESCE(%(op)s, 0)")


class ExistsFunction(ProperFunction):

    named('exists')

    parameters = [
            Parameter('op'),
    ]

    def correlate(self, op):
        op = CastBinding(op, coerce(BooleanDomain()), op.syntax)
        yield ExistsBinding(coerce(BooleanDomain()), self.syntax,
                            base=self.state.base, op=op)


ExistsBinding = GenericBinding.factory(ExistsFunction)
ExistsWrapperExpression = GenericExpression.factory(ExistsFunction)
ExistsWrapperPhrase = GenericPhrase.factory(ExistsFunction)


class EveryFunction(ProperFunction):

    named('every')

    parameters = [
            Parameter('op'),
    ]

    def correlate(self, op):
        op = CastBinding(op, coerce(BooleanDomain()), op.syntax)
        yield EveryBinding(coerce(BooleanDomain()), self.syntax,
                           base=self.state.base, op=op)


EveryBinding = GenericBinding.factory(EveryFunction)
EveryWrapperExpression = GenericExpression.factory(EveryFunction)
EveryWrapperPhrase = GenericPhrase.factory(EveryFunction)


class EncodeExistsEvery(Encode):

    adapts_none()
    is_exists = False
    is_every = False

    def __call__(self):
        op = self.state.encode(self.binding.op)
        if self.is_every:
            op = NegationCode(op, self.binding)
        space = self.state.relate(self.binding.base)
        plural_units = [unit for unit in op.units
                             if not space.spans(unit.space)]
        if not plural_units:
            raise InvalidArgumentError("a plural operand is required", op.mark)
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
            raise InvalidArgumentError("invalid plural operand", op.mark)
        plural_space = plural_spaces[0]
        if not plural_space.spans(space):
            raise InvalidArgumentError("invalid plural operand", op.mark)
        plural_space = FilteredSpace(plural_space, op, self.binding)
        op = LiteralCode(True, BooleanDomain(), self.binding)
        aggregate = CorrelatedUnit(op, plural_space, space,
                                   self.binding)
        if self.is_exists:
            wrapper = ExistsWrapperExpression(self.binding.domain,
                                              self.binding,
                                              op=aggregate)
        if self.is_every:
            wrapper = EveryWrapperExpression(self.binding.domain,
                                             self.binding,
                                             op=aggregate)
        wrapper = ScalarUnit(wrapper, space, self.binding)
        return wrapper


class EncodeExists(EncodeExistsEvery):

    adapts(ExistsBinding)
    is_exists = True


EvaluateExistsWrapper = GenericEvaluate.factory(ExistsFunction,
        ExistsWrapperExpression, ExistsWrapperPhrase)
SerializeExistsWrapper = GenericSerialize.factory(ExistsFunction,
        ExistsWrapperPhrase, "EXISTS(%(op)s)")


class EncodeEvery(EncodeExistsEvery):

    adapts(EveryBinding)
    is_every = True


EvaluateEveryWrapper = GenericEvaluate.factory(EveryFunction,
        EveryWrapperExpression, EveryWrapperPhrase)
SerializeEveryWrapper = GenericSerialize.factory(EveryFunction,
        EveryWrapperPhrase, "NOT EXISTS(%(op)s)")


class MinFunction(ProperFunction):

    named('min')

    parameters = [
            Parameter('op'),
    ]

    def correlate(self, op):
        Implementation = Min.realize((type(op.domain),))
        function = Implementation(op, self.state, self.syntax)
        yield function()


class Min(Adapter):

    adapts(Domain)

    def __init__(self, op, state, syntax):
        self.op = op
        self.state = state
        self.syntax = syntax

    def __call__(self):
        op = self.op
        return MinBinding(op.domain, self.syntax,
                          base=self.state.base, op=op)


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
        MinPhrase, "MIN(%(op)s)")
SerializeMinWrapper = GenericSerialize.factory(MinFunction,
        MinWrapperPhrase, "%(op)s")


class MaxFunction(ProperFunction):

    named('max')

    parameters = [
            Parameter('op'),
    ]

    def correlate(self, op):
        Implementation = Max.realize((type(op.domain),))
        function = Implementation(op, self.state, self.syntax)
        yield function()


class Max(Adapter):

    adapts(Domain)

    def __init__(self, op, state, syntax):
        self.op = op
        self.state = state
        self.syntax = syntax

    def __call__(self):
        op = self.op
        return MaxBinding(op.domain, self.syntax,
                          base=self.state.base, op=op)


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
        MaxPhrase, "MAX(%(op)s)")
SerializeMaxWrapper = GenericSerialize.factory(MaxFunction,
        MaxWrapperPhrase, "%(op)s")


class SumFunction(ProperFunction):

    named('sum')

    parameters = [
            Parameter('op'),
    ]

    def correlate(self, op):
        Implementation = Sum.realize((type(op.domain),))
        function = Implementation(op, self.state, self.syntax)
        yield function()


class Sum(Adapter):

    adapts(Domain)

    def __init__(self, op, state, syntax):
        self.op = op
        self.state = state
        self.syntax = syntax

    def __call__(self):
        op = self.op
        return SumBinding(op.domain, self.syntax,
                          base=self.state.base, op=op)


class SumInteger(Sum):

    adapts(IntegerDomain)


class SumDecimal(Sum):

    adapts(DecimalDomain)


class SumFloat(Sum):

    adapts(FloatDomain)


SumBinding = GenericBinding.factory(SumFunction)
SumExpression = GenericExpression.factory(SumFunction)
SumWrapperExpression = GenericExpression.factory(SumFunction)
SumPhrase = GenericPhrase.factory(SumFunction)
SumWrapperPhrase = GenericPhrase.factory(SumFunction)


EncodeSum = GenericAggregateEncode.factory(SumFunction,
        SumBinding, SumExpression, SumWrapperExpression)
EvaluateSum = GenericEvaluate.factory(SumFunction,
        SumExpression, SumPhrase)
EvaluateSumWrapper = GenericEvaluate.factory(SumFunction,
        SumWrapperExpression, SumWrapperPhrase)
SerializeSum = GenericSerialize.factory(SumFunction,
        SumPhrase, "SUM(%(op)s)")
SerializeSumWrapper = GenericSerialize.factory(SumFunction,
        SumWrapperPhrase, "%(op)s")


class AvgFunction(ProperFunction):

    named('avg')

    parameters = [
            Parameter('op'),
    ]

    def correlate(self, op):
        Implementation = Avg.realize((type(op.domain),))
        function = Implementation(op, self.state, self.syntax)
        yield function()


class Avg(Adapter):

    adapts(Domain)

    domain = None

    def __init__(self, op, state, syntax):
        self.op = op
        self.state = state
        self.syntax = syntax

    def __call__(self):
        op = CastBinding(self.op, self.domain,
                                 self.op.syntax)
        return AvgBinding(op.domain, self.syntax,
                          base=self.state.base, op=op)


class AvgDecimal(Avg):

    adapts_many(IntegerDomain, DecimalDomain)

    domain = DecimalDomain()


class AvgFloat(Avg):

    adapts(FloatDomain)

    domain = FloatDomain()


AvgBinding = GenericBinding.factory(AvgFunction)
AvgExpression = GenericExpression.factory(AvgFunction)
AvgWrapperExpression = GenericExpression.factory(AvgFunction)
AvgPhrase = GenericPhrase.factory(AvgFunction)
AvgWrapperPhrase = GenericPhrase.factory(AvgFunction)


EncodeAvg = GenericAggregateEncode.factory(AvgFunction,
        AvgBinding, AvgExpression, AvgWrapperExpression)
EvaluateAvg = GenericEvaluate.factory(AvgFunction,
        AvgExpression, AvgPhrase)
EvaluateAvgWrapper = GenericEvaluate.factory(AvgFunction,
        AvgWrapperExpression, AvgWrapperPhrase)
SerializeAvg = GenericSerialize.factory(AvgFunction,
        AvgPhrase, "AVG(%(op)s)")
SerializeAvgWrapper = GenericSerialize.factory(AvgFunction,
        AvgWrapperPhrase, "%(op)s")


def call(syntax, state, base=None):
    if base is not None:
        state.push_base(base)
    function = Function(syntax, state)
    bindings = list(function())
    if base is not None:
        state.pop_base()
    return bindings


