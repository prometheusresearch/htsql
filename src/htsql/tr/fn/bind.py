#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.fn.bind`
=======================
"""


from ...util import aresubclasses
from ...adapter import Adapter, Component, adapts, adapts_many, named
from ...domain import (Domain, UntypedDomain, BooleanDomain, StringDomain,
                       NumberDomain, IntegerDomain, DecimalDomain, FloatDomain,
                       DateDomain, EnumDomain)
from ..syntax import NumberSyntax, StringSyntax, IdentifierSyntax
from ..binding import (LiteralBinding, SortBinding, SieveBinding,
                       FormulaBinding, CastBinding, WrapperBinding,
                       TitleBinding, DirectionBinding, Binding)
from ..bind import BindByName, BindingState
from ..error import BindError
from ..coerce import coerce
from ..lookup import lookup
from .signature import (Signature, ThisSig, RootSig, DirectSig, FiberSig, AsSig,
                        SortDirectionSig, LimitSig, SortSig, NullSig, TrueSig,
                        FalseSig, CastSig, DateSig, IsEqualSig, IsInSig,
                        IsTotallyEqualSig, AndSig, OrSig, NotSig, CompareSig,
                        AddSig, ConcatenateSig, DateIncrementSig,
                        SubtractSig, DateDecrementSig, DateDifferenceSig,
                        MultiplySig, DivideSig, IsNullSig, NullIfSig,
                        IfNullSig, IfSig, SwitchSig, KeepPolaritySig,
                        ReversePolaritySig, RoundSig, RoundToSig, LengthSig,
                        ContainsSig, ExistsSig, EverySig, BinarySig,
                        UnarySig, CountSig, MinSig, MaxSig, SumSig, AvgSig)
import sys


class BindFunction(BindByName):

    signature = None

    def match(self):
        assert self.signature is not None
        operands = self.syntax.arguments[:]
        arguments = {}
        for index, slot in enumerate(self.signature.slots):
            name = slot.name
            value = None
            if not operands:
                if slot.is_mandatory:
                    raise BindError("missing argument %s" % name,
                                    self.syntax.mark)
                if not slot.is_singular:
                    value = []
            elif slot.is_singular:
                value = operands.pop(0)
            else:
                if index == len(self.signature.slots)-1:
                    value = operands[:]
                    operands[:] = []
                else:
                    value = [operands.pop(0)]
            arguments[name] = value
        if operands:
            raise BindError("unexpected argument", operands[0].mark)
        return arguments

    def bind(self):
        assert self.signature is not None
        arguments = self.match()
        bound_arguments = {}
        for slot in self.signature.slots:
            name = slot.name
            value = arguments[name]
            bound_value = None
            if slot.is_singular:
                if value is not None:
                    bound_values = self.state.bind_all(value)
                    if len(bound_values) > 1:
                        raise BindError("unexpected list argument",
                                        value.mark)
                    if slot.is_mandatory and not bound_values:
                        raise BindError("unexpected empty argument",
                                        value.mark)
                    if bound_values:
                        [bound_value] = bound_values
            else:
                if len(value) > 1:
                    bound_value = [self.state.bind(item) for item in value]
                elif len(value) == 1:
                    [value] = value
                    bound_value = self.state.bind_all(value)
                    if slot.is_mandatory and not bound_value:
                        raise BindError("missing argument %s" % name,
                                        value.mark)
                else:
                    bound_value = []
            bound_arguments[name] = bound_value
        return bound_arguments

    def correlate(self, **arguments):
        raise NotImplementedError()

    def __call__(self):
        arguments = self.bind()
        yield self.correlate(**arguments)


class BindMacro(BindFunction):

    def expand(self, **arguments):
        raise NotImplementedError()

    def __call__(self):
        arguments = self.match()
        return self.expand(**arguments)


class BindRoot(BindMacro):

    named('root')
    signature = RootSig

    def expand(self):
        yield WrapperBinding(self.state.root, self.syntax)


class BindThis(BindMacro):

    named('this')
    signature = ThisSig

    def expand(self):
        yield WrapperBinding(self.state.base, self.syntax)


class BindDirect(BindMacro):

    named('direct')
    signature = DirectSig

    def expand(self, table):
        if not isinstance(table, IdentifierSyntax):
            raise BindError("an identifier expected", table.mark)
        binding = lookup(self.state.root, table)
        if binding is None:
            raise InvalidArgumentError("unknown identifier", table.mark)
        binding = binding.clone(base=self.state.base)
        yield WrapperBinding(binding, self.syntax)


class BindFiber(BindMacro):

    named('fiber')
    signature = FiberSig

    def expand(self, table, image, counterimage=None):
        if not isinstance(table, IdentifierSyntax):
            raise BindError("an identifier expected", table.mark)
        binding = lookup(self.state.root, table)
        if binding is None:
            raise InvalidArgumentError("unknown identifier", table.mark)
        binding = binding.clone(base=self.state.base)
        parent = self.state.bind(image)
        if counterimage is None:
            counterimage = image
        child = self.state.bind(counterimage, base=binding)
        domain = coerce(parent.domain, child.domain)
        if domain is None:
            raise InvalidArgumentError("incompatible images",
                                       self.syntax.mark)
        parent = CastBinding(parent, domain, parent.syntax)
        child = CastBinding(child, domain, child.syntax)
        condition = FormulaBinding(IsEqualSig(+1), coerce(BooleanDomain()),
                                   self.syntax, lop=parent, rop=child)
        yield SieveBinding(binding, condition, self.syntax)


class BindAs(BindMacro):

    named('as')
    signature = AsSig

    def expand(self, base, title):
        if not isinstance(title, (StringSyntax, IdentifierSyntax)):
            raise BindError("expected a string literal or an identifier",
                            title.mark)
        base = self.state.bind(base)
        yield TitleBinding(base, title.value, self.syntax)


class BindDirectionBase(BindMacro):

    signature = SortDirectionSig
    direction = None

    def expand(self, base):
        for base in self.state.bind_all(base):
            yield DirectionBinding(base, self.direction, self.syntax)


class BindAscDir(BindDirectionBase):

    named('_+')
    direction = +1


class BindDescDir(BindDirectionBase):

    named('_-')
    direction = -1


class BindLimit(BindMacro):

    named('limit')
    signature = LimitSig

    def parse(self, argument):
        try:
            if not isinstance(argument, NumberSyntax):
                raise ValueError
            value = int(argument.value)
            if not (value >= 0):
                raise ValueError
        except ValueError:
            raise BindError("expected a non-negative integer", argument.mark)
        return value

    def expand(self, limit, offset=None):
        limit = self.parse(limit)
        if offset is not None:
            offset = self.parse(offset)
        yield SortBinding(self.state.base, [], limit, offset, self.syntax)


class BindSort(BindMacro):

    named('sort')
    signature = SortSig

    def expand(self, order):
        bindings = []
        for item in order:
            for binding in self.state.bind_all(item):
                domain = coerce(binding.domain)
                if domain is None:
                    raise BindError("incompatible expression type",
                                    binding.mark)
                binding = CastBinding(binding, domain, binding.syntax)
                bindings.append(binding)
        yield SortBinding(self.state.base, bindings, None, None, self.syntax)


class BindNull(BindMacro):

    named('null')
    signature = NullSig

    def expand(self):
        yield LiteralBinding(None, UntypedDomain(), self.syntax)


class BindTrue(BindMacro):

    named('true')
    signature = TrueSig

    def expand(self):
        yield LiteralBinding(True, coerce(BooleanDomain()), self.syntax)


class BindFalse(BindMacro):

    named('false')
    signature = FalseSig

    def expand(self):
        yield LiteralBinding(False, coerce(BooleanDomain()), self.syntax)


class BindCast(BindFunction):

    signature = CastSig
    codomain = None

    def correlate(self, base):
        domain = coerce(self.codomain)
        return CastBinding(base, domain, self.syntax)


class BindBooleanCast(BindCast):

    named('boolean')
    codomain = BooleanDomain()


class BindStringCast(BindCast):

    named('string')
    codomain = StringDomain()


class BindIntegerCast(BindCast):

    named('integer')
    codomain = IntegerDomain()


class BindDecimalCast(BindCast):

    named('decimal')
    codomain = DecimalDomain()


class BindFloatCast(BindCast):

    named('float')
    codomain = FloatDomain()


class BindDateCast(BindCast):

    named('date')
    codomain = DateDomain()


class BindMonoFunction(BindFunction):

    signature = None
    domains = []
    codomain = None

    def correlate(self, **arguments):
        assert self.signature is not None
        assert len(self.signature.slots) == len(self.domains)
        assert self.codomain is not None
        cast_arguments = {}
        for domain, slot in zip(self.domains, self.signature.slots):
            domain = coerce(domain)
            name = slot.name
            value = arguments[name]
            if slot.is_singular:
                if value is not None:
                    value = CastBinding(value, domain, value.syntax)
            else:
                value = [CastBinding(item, domain, item.syntax)
                         for item in value]
            cast_arguments[name] = value
        return FormulaBinding(self.signature(), coerce(self.codomain),
                               self.syntax, **cast_arguments)


class BindDate(BindMonoFunction):

    named(('date', 3))
    signature = DateSig
    domains = [IntegerDomain(), IntegerDomain(), IntegerDomain()]
    codomain = DateDomain()


class BindAmongBase(BindFunction):

    signature = IsInSig
    polarity = None

    def correlate(self, lop, rops):
        domain = coerce(lop.domain, *(rop.domain for rop in rops))
        if domain is None:
            raise BindError("incompatible arguments", self.syntax.mark)
        lop = CastBinding(lop, domain, lop.syntax)
        rops = [CastBinding(rop, domain, rop.syntax) for rop in rops]
        if len(rops) == 1:
            return FormulaBinding(IsEqualSig(self.polarity),
                                  coerce(BooleanDomain()),
                                  self.syntax, lop=lop, rop=rops[0])
        else:
            return FormulaBinding(self.signature(self.polarity),
                                  coerce(BooleanDomain()),
                                  self.syntax, lop=lop, rops=rops)


class BindAmong(BindAmongBase):

    named('=')
    polarity = +1


class BindNotAmong(BindAmongBase):

    named('!=')
    polarity = -1


class BindTotallyEqualBase(BindFunction):

    signature = IsTotallyEqualSig
    polarity = None

    def correlate(self, lop, rop):
        domain = coerce(lop.domain, rop.domain)
        if domain is None:
            raise BindError("incompatible arguments", self.syntax.mark)
        lop = CastBinding(lop, domain, lop.syntax)
        rop = CastBinding(rop, domain, rop.syntax)
        return FormulaBinding(IsTotallyEqualSig(self.polarity),
                              coerce(BooleanDomain()), self.syntax,
                              lop=lop, rop=rop)


class BindTotallyEqual(BindTotallyEqualBase):

    named('==')
    polarity = +1


class BindTotallyNotEqual(BindTotallyEqualBase):

    named('!==')
    polarity = -1


class BindAnd(BindFunction):

    named('&')
    signature = BinarySig

    def correlate(self, lop, rop):
        domain = coerce(BooleanDomain())
        lop = CastBinding(lop, domain, lop.syntax)
        rop = CastBinding(rop, domain, rop.syntax)
        return FormulaBinding(AndSig(), domain, self.syntax, ops=[lop, rop])


class BindOr(BindFunction):

    named('|')
    signature = BinarySig

    def correlate(self, lop, rop):
        domain = coerce(BooleanDomain())
        lop = CastBinding(lop, domain, lop.syntax)
        rop = CastBinding(rop, domain, rop.syntax)
        return FormulaBinding(OrSig(), domain, self.syntax, ops=[lop, rop])


class BindNot(BindFunction):

    named('!_')
    signature = NotSig

    def correlate(self, op):
        domain = coerce(BooleanDomain())
        op = CastBinding(op, domain, op.syntax)
        return FormulaBinding(self.signature(), domain, self.syntax, op=op)


class BindCompare(BindFunction):

    signature = CompareSig
    relation = None

    def correlate(self, lop, rop):
        domain = coerce(lop.domain, rop.domain)
        if domain is None:
            raise BindError("incompatible arguments", self.syntax.mark)
        lop = CastBinding(lop, domain, lop.syntax)
        rop = CastBinding(rop, domain, rop.syntax)
        comparable = Comparable(domain)
        if not comparable():
            raise BindError("uncomparable arguments", self.syntax.mark)
        return FormulaBinding(self.signature(self.relation),
                              coerce(BooleanDomain()),
                              self.syntax, lop=lop, rop=rop)


class BindLessThan(BindCompare):

    named('<')
    relation = '<'


class BindLessThanOrEqual(BindCompare):

    named('<=')
    relation = '<='


class BindGreaterThan(BindCompare):

    named('>')
    relation = '>'


class BindGreaterThanOrEqual(BindCompare):

    named('>=')
    relation = '>='


class Comparable(Adapter):

    adapts(Domain)

    def __init__(self, domain):
        self.domain = domain

    def __call__(self):
        return False


class ComparableDomains(Comparable):

    adapts_many(IntegerDomain, DecimalDomain, FloatDomain,
                StringDomain, EnumDomain, DateDomain)

    def __call__(self):
        return True


class Correlate(Component):

    input_signature = Signature
    input_domains = []
    input_arity = 0

    signature = None
    domains = []
    codomain = None

    @classmethod
    def dominates(component, other):
        if issubclass(component, other):
            return True
        if issubclass(component.input_signature, other.input_signature):
            return True
        return False

    @classmethod
    def matches(component, dispatch_key):
        key_signature, key_domain_vector = dispatch_key
        if not issubclass(key_signature, component.input_signature):
            return False
        if len(key_domain_vector) < component.input_arity:
            return False
        key_domain_vector = key_domain_vector[:component.input_arity]
        for domain_vector in component.input_domains:
            if aresubclasses(key_domain_vector, domain_vector):
                return True
        return False

    @classmethod
    def dispatch(interface, binding, *args, **kwds):
        assert isinstance(binding, FormulaBinding)
        signature = type(binding.signature)
        domain_vector = []
        for slot in signature.slots:
            if not (slot.is_mandatory and slot.is_singular):
                break
            domain = type(binding.arguments[slot.name].domain)
            domain_vector.append(domain)
        return (signature, tuple(domain_vector))

    def __init__(self, binding, state):
        assert isinstance(binding, FormulaBinding)
        assert isinstance(state, BindingState)
        self.binding = binding
        self.state = state
        self.arguments = binding.arguments

    def __call__(self):
        if self.signature is None:
            raise BindError("incompatible arguments", self.binding.mark)
        signature = self.signature.inherit(self.binding.signature)
        assert self.arguments.admits(Binding, signature)
        arguments = {}
        for index, slot in enumerate(signature.slots):
            value = self.arguments[slot.name]
            if index < len(self.domains):
                domain = coerce(self.domains[index])
                if slot.is_singular:
                    if value is not None:
                        value = CastBinding(value, domain, value.syntax)
                else:
                    value = [CastBinding(item, domain, item.syntax)
                             for item in value]
            arguments[slot.name] = value
        domain = self.binding.domain
        if self.codomain is not None:
            domain = coerce(self.codomain)
        return FormulaBinding(signature, domain, self.binding.syntax,
                              **arguments)


def correlates(signature, *domain_vectors):
    assert issubclass(signature, Signature)
    domain_vectors = [domain_vector if isinstance(domain_vector, tuple)
                                  else (domain_vector,)
                      for domain_vector in domain_vectors]
    assert len(domain_vectors) > 0
    arity = len(domain_vectors[0])
    assert all(len(domain_vector) == arity
               for domain_vector in domain_vectors)
    frame = sys._getframe(1)
    frame.f_locals['input_signature'] = signature
    frame.f_locals['input_domains'] = domain_vectors
    frame.f_locals['input_arity'] = arity


class BindPolyFunction(BindFunction):

    signature = None
    codomain = UntypedDomain()

    def correlate(self, **arguments):
        binding = FormulaBinding(self.signature(), self.codomain, self.syntax,
                                 **arguments)
        correlate = Correlate(binding, self.state)
        return correlate()


class BindAdd(BindPolyFunction):

    named('+')
    signature = AddSig


class CorrelateIntegerAdd(Correlate):

    correlates(AddSig, (IntegerDomain, IntegerDomain))
    signature = AddSig
    domains = [IntegerDomain(), IntegerDomain()]
    codomain = IntegerDomain()


class CorrelateDecimalAdd(Correlate):

    correlates(AddSig, (IntegerDomain, DecimalDomain),
                       (DecimalDomain, IntegerDomain),
                       (DecimalDomain, DecimalDomain))
    signature = AddSig
    domains = [DecimalDomain(), DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatAdd(Correlate):

    correlates(AddSig, (IntegerDomain, FloatDomain),
                       (DecimalDomain, FloatDomain),
                       (FloatDomain, IntegerDomain),
                       (FloatDomain, DecimalDomain),
                       (FloatDomain, FloatDomain))
    signature = AddSig
    domains = [FloatDomain(), FloatDomain()]
    codomain = FloatDomain()


class CorrelateDateIncrement(Correlate):

    correlates(AddSig, (DateDomain, IntegerDomain))
    signature = DateIncrementSig
    domains = [DateDomain(), IntegerDomain()]
    codomain = DateDomain()


class CorrelateConcatenate(Correlate):

    correlates(AddSig, (UntypedDomain, UntypedDomain),
                       (UntypedDomain, StringDomain),
                       (StringDomain, UntypedDomain),
                       (StringDomain, StringDomain))
    signature = ConcatenateSig
    domains = [StringDomain(), StringDomain()]
    codomain = StringDomain()


class BindSubtract(BindPolyFunction):

    named('-')
    signature = SubtractSig


class CorrelateIntegerSubtract(Correlate):

    correlates(SubtractSig, (IntegerDomain, IntegerDomain))
    signature = SubtractSig
    domains = [IntegerDomain(), IntegerDomain()]
    codomain = IntegerDomain()


class CorrelateDecimalSubtract(Correlate):

    correlates(SubtractSig, (IntegerDomain, DecimalDomain),
                            (DecimalDomain, IntegerDomain),
                            (DecimalDomain, DecimalDomain))
    signature = SubtractSig
    domains = [DecimalDomain(), DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatSubtract(Correlate):

    correlates(SubtractSig, (IntegerDomain, FloatDomain),
                            (DecimalDomain, FloatDomain),
                            (FloatDomain, IntegerDomain),
                            (FloatDomain, DecimalDomain),
                            (FloatDomain, FloatDomain))
    signature = SubtractSig
    domains = [FloatDomain(), FloatDomain()]
    codomain = FloatDomain()


class CorrelateDateDecrement(Correlate):

    correlates(SubtractSig, (DateDomain, IntegerDomain))
    signature = DateDecrementSig
    domains = [DateDomain(), IntegerDomain()]
    codomain = DateDomain()


class CorrelateDateDifference(Correlate):

    correlates(SubtractSig, (DateDomain, DateDomain))
    signature = DateDifferenceSig
    domains = [DateDomain(), DateDomain()]
    codomain = IntegerDomain()


class BindMultiply(BindPolyFunction):

    named('*')
    signature = MultiplySig


class CorrelateIntegerMultiply(Correlate):

    correlates(MultiplySig, (IntegerDomain, IntegerDomain))
    signature = MultiplySig
    domains = [IntegerDomain(), IntegerDomain()]
    codomain = IntegerDomain()


class CorrelateDecimalMultiply(Correlate):

    correlates(MultiplySig, (IntegerDomain, DecimalDomain),
                            (DecimalDomain, IntegerDomain),
                            (DecimalDomain, DecimalDomain))
    signature = MultiplySig
    domains = [DecimalDomain(), DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatMultiply(Correlate):

    correlates(MultiplySig, (IntegerDomain, FloatDomain),
                            (DecimalDomain, FloatDomain),
                            (FloatDomain, IntegerDomain),
                            (FloatDomain, DecimalDomain),
                            (FloatDomain, FloatDomain))
    signature = MultiplySig
    domains = [FloatDomain(), FloatDomain()]
    codomain = FloatDomain()


class BindDivide(BindPolyFunction):

    named('/')
    signature = DivideSig


class CorrelateDecimalDivide(Correlate):

    correlates(DivideSig, (IntegerDomain, IntegerDomain),
                          (IntegerDomain, DecimalDomain),
                          (DecimalDomain, IntegerDomain),
                          (DecimalDomain, DecimalDomain))
    signature = DivideSig
    domains = [DecimalDomain(), DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatDivide(Correlate):

    correlates(DivideSig, (IntegerDomain, FloatDomain),
                          (DecimalDomain, FloatDomain),
                          (FloatDomain, IntegerDomain),
                          (FloatDomain, DecimalDomain),
                          (FloatDomain, FloatDomain))
    signature = DivideSig
    domains = [FloatDomain(), FloatDomain()]
    codomain = FloatDomain()


class BindKeepPolarity(BindPolyFunction):

    named('+_')
    signature = KeepPolaritySig


class CorrelateIntegerKeepPolarity(Correlate):

    correlates(KeepPolaritySig, IntegerDomain)
    signature = KeepPolaritySig
    domains = [IntegerDomain()]
    codomain = IntegerDomain()


class CorrelateDecimalKeepPolarity(Correlate):

    correlates(KeepPolaritySig, DecimalDomain)
    signature = KeepPolaritySig
    domains = [DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatKeepPolarity(Correlate):

    correlates(KeepPolaritySig, FloatDomain)
    signature = KeepPolaritySig
    domains = [FloatDomain()]
    codomain = FloatDomain()


class BindReversePolarity(BindPolyFunction):

    named('-_')
    signature = ReversePolaritySig


class CorrelateIntegerReversePolarity(Correlate):

    correlates(ReversePolaritySig, IntegerDomain)
    signature = ReversePolaritySig
    domains = [IntegerDomain()]
    codomain = IntegerDomain()


class CorrelateDecimalReversePolarity(Correlate):

    correlates(ReversePolaritySig, DecimalDomain)
    signature = ReversePolaritySig
    domains = [DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatReversePolarity(Correlate):

    correlates(ReversePolaritySig, FloatDomain)
    signature = ReversePolaritySig
    domains = [FloatDomain()]
    codomain = FloatDomain()


class BindRound(BindPolyFunction):

    named('round')
    signature = RoundSig


class CorrelateDecimalRound(Correlate):

    correlates(RoundSig, IntegerDomain,
                         DecimalDomain)
    signature = RoundSig
    domains = [DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatRound(Correlate):

    correlates(RoundSig, FloatDomain)
    signature = RoundSig
    domains = [FloatDomain()]
    codomain = FloatDomain()


class BindRoundTo(BindPolyFunction):

    named(('round', 2))
    signature = RoundToSig


class CorrelateDecimalRoundTo(Correlate):

    correlates(RoundToSig, (IntegerDomain, IntegerDomain),
                           (DecimalDomain, IntegerDomain))
    signature = RoundToSig
    domains = [DecimalDomain(), IntegerDomain()]
    codomain = DecimalDomain()


class BindLength(BindPolyFunction):

    named('length')
    signature = LengthSig


class CorrelateStringLength(Correlate):

    correlates(LengthSig, StringDomain,
                          UntypedDomain)
    signature = LengthSig
    domains = [StringDomain()]
    codomain = IntegerDomain()


class BindContainsBase(BindPolyFunction):

    signature = ContainsSig
    polarity = None

    def correlate(self, **arguments):
        binding = FormulaBinding(self.signature(self.polarity),
                                 self.codomain, self.syntax, **arguments)
        correlate = Correlate(binding, self.state)
        return correlate()


class BindContains(BindContainsBase):

    named('~')
    polarity = +1


class BindNotContains(BindContainsBase):

    named('!~')
    polarity = -1


class CorrelateStringContains(Correlate):

    correlates(ContainsSig, (StringDomain, StringDomain),
                            (StringDomain, UntypedDomain),
                            (UntypedDomain, StringDomain),
                            (UntypedDomain, UntypedDomain))
    signature = ContainsSig
    domains = [StringDomain(), StringDomain()]
    codomain = BooleanDomain()


class BindHomoFunction(BindFunction):

    codomain = None

    def correlate(self, **arguments):
        assert self.signature is not None
        domains = []
        for slot in self.signature.slots:
            name = slot.name
            value = arguments[name]
            if slot.is_singular:
                if value is not None:
                    domains.append(value.domain)
            else:
                domains.extend(item.domain for item in value)
        domain = coerce(*domains)
        if domain is None:
            raise BindError("incompatible arguments", self.syntax.mark)
        cast_arguments = {}
        for slot in self.signature.slots:
            name = slot.name
            value = arguments[name]
            if slot.is_singular:
                if value is not None:
                    value = CastBinding(value, domain, value.syntax)
            else:
                value = [CastBinding(item, domain, item.syntax)
                         for item in value]
            cast_arguments[name] = value
        if self.codomain is None:
            codomain = domain
        else:
            codomain = coerce(self.codomain)
        return FormulaBinding(self.signature, codomain, self.syntax,
                               **cast_arguments)


class BindIsNull(BindHomoFunction):

    named('is_null')
    signature = IsNullSig(+1)
    codomain = BooleanDomain()


class BindNullIf(BindHomoFunction):

    named('null_if')
    signature = NullIfSig()


class BindIfNull(BindHomoFunction):

    named('if_null')
    signature = IfNullSig()


class BindIf(BindFunction):

    named('if')
    signature = IfSig

    def match(self):
        operands = list(reversed(self.syntax.arguments))
        if len(operands) < 2:
            raise BindError("not enough arguments", self.syntax.mark)
        predicates = []
        consequents = []
        alternative = None
        while operands:
            if len(operands) == 1:
                alternative = operands.pop()
            else:
                predicates.append(operands.pop())
                consequents.append(operands.pop())
        return {
                'predicates': predicates,
                'consequents': consequents,
                'alternative': alternative,
        }

    def correlate(self, predicates, consequents, alternative):
        predicates = [CastBinding(predicate, coerce(BooleanDomain()),
                                  predicate.syntax)
                      for predicate in predicates]
        domains = [consequent.domain for consequent in consequents]
        if alternative is not None:
            domains.append(alternative.domain)
        domain = coerce(*domains)
        if domain is None:
            raise BindingError("incompatible arguments", self.syntax.mark)
        consequents = [CastBinding(consequent, domain, consequent.syntax)
                       for consequent in consequents]
        if alternative is not None:
            alternative = CastBinding(alternative, domain, consequent.syntax)
        return FormulaBinding(self.signature(), domain, self.syntax,
                               predicates=predicates,
                               consequents=consequents,
                               alternative=alternative)


class BindSwitch(BindFunction):

    named('switch')
    signature = SwitchSig

    def match(self):
        operands = list(reversed(self.syntax.arguments))
        if len(operands) < 3:
            raise BindError("not enough arguments", self.syntax.mark)
        variable = None
        variants = []
        consequents = []
        alternative = None
        variable = operands.pop()
        while operands:
            if len(operands) == 1:
                alternative = operands.pop()
            else:
                variants.append(operands.pop())
                consequents.append(operands.pop())
        return {
                'variable': variable,
                'variants': variants,
                'consequents': consequents,
                'alternative': alternative,
        }

    def correlate(self, variable, variants, consequents, alternative):
        domains = [variable.domain] + [variant.domain for variant in variants]
        domain = coerce(*domains)
        if domain is None:
            raise BindError("incompatible arguments", self.syntax.mark)
        variable = CastBinding(variable, domain, variable.syntax)
        variants = [CastBinding(variant, domain, variant.syntax)
                    for variant in variants]
        domains = [consequent.domain for consequent in consequents]
        if alternative is not None:
            domains.append(alternative.domain)
        domain = coerce(*domains)
        if domain is None:
            raise BindingError("incompatible arguments", self.syntax.mark)
        consequents = [CastBinding(consequent, domain, consequent.syntax)
                       for consequent in consequents]
        if alternative is not None:
            alternative = CastBinding(alternative, domain, consequent.syntax)
        return FormulaBinding(self.signature(), domain, self.syntax,
                               variable=variable,
                               variants=variants,
                               consequents=consequents,
                               alternative=alternative)


class BindExistsBase(BindFunction):

    signature = UnarySig
    bind_signature = None

    def correlate(self, op):
        op = CastBinding(op, coerce(BooleanDomain()), op.syntax)
        return FormulaBinding(self.bind_signature(), op.domain, self.syntax,
                               base=self.state.base, op=op)


class BindExists(BindExistsBase):

    named('exists')
    bind_signature = ExistsSig


class BindEvery(BindExistsBase):

    named('every')
    bind_signature = EverySig


class BindCount(BindFunction):

    named('count')
    signature = UnarySig

    def correlate(self, op):
        op = CastBinding(op, coerce(BooleanDomain()), op.syntax)
        return FormulaBinding(CountSig(), coerce(IntegerDomain()),
                               self.syntax, base=self.state.base, op=op)


class CorrelateAggregate(Adapter):

    adapts(Domain)
    signature = None
    domain = None
    codomain = None

    def __call__(self):
        return (self.signature is not None and
                self.domain is not None and
                self.codomain is not None)


class BindPolyAggregate(BindFunction):

    signature = UnarySig
    correlation = None

    def correlate(self, op):
        correlate = self.correlation(op.domain)
        if not correlate():
            raise BindError("incompatible argument", self.syntax.mark)
        op = CastBinding(op, coerce(correlate.domain), op.syntax)
        return FormulaBinding(correlate.signature(),
                              coerce(correlate.codomain), self.syntax,
                              base=self.state.base, op=op)


class CorrelateMin(CorrelateAggregate):

    signature = MinSig


class BindMin(BindPolyAggregate):

    named('min')
    correlation = CorrelateMin


class CorrelateIntegerMin(CorrelateMin):

    adapts(IntegerDomain)
    domain = IntegerDomain()
    codomain = IntegerDomain()


class CorrelateDecimalMin(CorrelateMin):

    adapts(DecimalDomain)
    domain = DecimalDomain()
    codomain = DecimalDomain()


class CorrelateFloatMin(CorrelateMin):

    adapts(FloatDomain)
    domain = FloatDomain()
    codomain = FloatDomain()


class CorrelateStringMin(CorrelateMin):

    adapts(StringDomain)
    domain = StringDomain()
    codomain = StringDomain()


class CorrelateDateMin(CorrelateMin):

    adapts(DateDomain)
    domain = DateDomain()
    codomain = DateDomain()


class CorrelateMax(CorrelateAggregate):

    signature = MaxSig


class BindMax(BindPolyAggregate):

    named('max')
    correlation = CorrelateMax


class CorrelateIntegerMax(CorrelateMax):

    adapts(IntegerDomain)
    domain = IntegerDomain()
    codomain = IntegerDomain()


class CorrelateDecimalMax(CorrelateMax):

    adapts(DecimalDomain)
    domain = DecimalDomain()
    codomain = DecimalDomain()


class CorrelateFloatMax(CorrelateMax):

    adapts(FloatDomain)
    domain = FloatDomain()
    codomain = FloatDomain()


class CorrelateStringMax(CorrelateMax):

    adapts(StringDomain)
    domain = StringDomain()
    codomain = StringDomain()


class CorrelateDateMax(CorrelateMax):

    adapts(DateDomain)
    domain = DateDomain()
    codomain = DateDomain()


class CorrelateSum(CorrelateAggregate):

    signature = SumSig


class BindSum(BindPolyAggregate):

    named('sum')
    correlation = CorrelateSum


class CorrelateIntegerSum(CorrelateSum):

    adapts(IntegerDomain)
    domain = IntegerDomain()
    codomain = IntegerDomain()


class CorrelateDecimalSum(CorrelateSum):

    adapts(DecimalDomain)
    domain = DecimalDomain()
    codomain = DecimalDomain()


class CorrelateFloatSum(CorrelateSum):

    adapts(FloatDomain)
    domain = FloatDomain()
    codomain = FloatDomain()


class CorrelateAvg(CorrelateAggregate):

    signature = AvgSig


class BindAvg(BindPolyAggregate):

    named('avg')
    correlation = CorrelateAvg


class CorrelateDecimalAvg(CorrelateAvg):

    adapts_many(IntegerDomain,
                DecimalDomain)
    domain = DecimalDomain()
    codomain = DecimalDomain()


class CorrelateFloatAvg(CorrelateAvg):

    adapts(FloatDomain)
    domain = FloatDomain()
    codomain = FloatDomain()


