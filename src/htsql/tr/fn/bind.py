#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.fn.bind`
=======================
"""


from ...adapter import Adapter, adapts, adapts_many, adapts_none, named
from ...domain import (Domain, UntypedDomain, BooleanDomain, StringDomain,
                       NumberDomain, IntegerDomain, DecimalDomain, FloatDomain,
                       DateDomain, EnumDomain)
from ..syntax import NumberSyntax, StringSyntax, IdentifierSyntax
from ..binding import (LiteralBinding, SortBinding, SieveBinding,
                       FunctionBinding, EqualityBinding, TotalEqualityBinding,
                       ConjunctionBinding, DisjunctionBinding, NegationBinding,
                       CastBinding, WrapperBinding, TitleBinding,
                       DirectionBinding)
from ..bind import BindByName
from ..error import BindError
from ..coerce import coerce
from ..lookup import lookup
from .signature import (ThisSig, RootSig, DirectSig, FiberSig, AsSig,
                        SortDirectionSig, LimitSig, SortSig, NullSig, TrueSig,
                        FalseSig, CastSig, DateSig, EqualSig, AmongSig,
                        TotallyEqualSig, AndSig, OrSig, NotSig, CompareSig,
                        AddSig, NumericAddSig, ConcatenateSig,
                        DateIncrementSig, SubtractSig, NumericSubtractSig,
                        DateDecrementSig, DateDifferenceSig, MultiplySig,
                        NumericMultiplySig, DivideSig, NumericDivideSig,
                        IsNullSig, NullIfSig, IfNullSig, IfSig, SwitchSig,
                        KeepPolaritySig, ReversePolaritySig,
                        NumericKeepPolaritySig, NumericReversePolaritySig,
                        RoundSig, RoundToSig, LengthSig, StringLengthSig,
                        ContainsSig, StringContainsSig, ExistsSig, EverySig,
                        UnarySig, CountSig, MinSig, MaxSig, SumSig, AvgSig)


class BindFunction(BindByName):

    signature = None

    def match(self):
        operands = self.syntax.arguments[:]
        arguments = {}
        slots = []
        if self.signature is not None:
            slots = self.signature.slots
        for index, slot in enumerate(slots):
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
                if index == len(slots)-1:
                    value = operands[:]
                    operands[:] = []
                else:
                    value = [operands.pop(0)]
            arguments[name] = value
        if operands:
            raise BindError("unexpected argument", operands[0].mark)
        return arguments

    def bind(self):
        arguments = self.match()
        bound_arguments = {}
        slots = []
        if self.signature is not None:
            slots = self.signature.slots
        for slot in slots:
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
    signature = RootSig()

    def expand(self):
        yield WrapperBinding(self.state.root, self.syntax)


class BindThis(BindMacro):

    named('this')
    signature = ThisSig()

    def expand(self):
        yield WrapperBinding(self.state.base, self.syntax)


class BindDirect(BindMacro):

    named('direct')
    signature = DirectSig()

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
    signature = FiberSig()

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
        condition = EqualityBinding(parent, child, self.syntax)
        yield SieveBinding(binding, condition, self.syntax)


class BindAs(BindMacro):

    named('as')
    signature = AsSig()

    def expand(self, base, title):
        if not isinstance(title, (StringSyntax, IdentifierSyntax)):
            raise BindError("expected a string literal or an identifier",
                            title.mark)
        base = self.state.bind(base)
        yield TitleBinding(base, title.value, self.syntax)


class BindDirectionBase(BindMacro):

    def expand(self, base):
        for base in self.state.bind_all(base):
            yield DirectionBinding(base, self.signature.direction, self.syntax)


class BindAscDir(BindDirectionBase):

    named('_+')
    signature = SortDirectionSig(+1)


class BindDescDir(BindDirectionBase):

    named('_-')
    signature = SortDirectionSig(-1)


class BindLimit(BindMacro):

    named('limit')
    signature = LimitSig()

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
    signature = SortSig()

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
    signature = NullSig()

    def expand(self):
        yield LiteralBinding(None, UntypedDomain(), self.syntax)


class BindTrue(BindMacro):

    named('true')
    signature = TrueSig()

    def expand(self):
        yield LiteralBinding(True, coerce(BooleanDomain()), self.syntax)


class BindFalse(BindMacro):

    named('false')
    signature = FalseSig()

    def expand(self):
        yield LiteralBinding(False, coerce(BooleanDomain()), self.syntax)


class BindCast(BindFunction):

    signature = CastSig()
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
        return FunctionBinding(self.signature, coerce(self.codomain),
                               self.syntax, **cast_arguments)


class BindDate(BindMonoFunction):

    named(('date', 3))
    signature = DateSig()
    domains = [IntegerDomain(), IntegerDomain(), IntegerDomain()]
    codomain = DateDomain()


class BindAmongBase(BindFunction):

    def correlate(self, lop, rops):
        domain = coerce(lop.domain, *(rop.domain for rop in rops))
        if domain is None:
            raise BindError("incompatible arguments", self.syntax.mark)
        lop = CastBinding(lop, domain, lop.syntax)
        rops = [CastBinding(rop, domain, rop.syntax) for rop in rops]
        if len(rops) == 1:
            binding = EqualityBinding(lop, rops[0], self.syntax)
            if self.signature.polarity == -1:
                binding = NegationBinding(binding, self.syntax)
            return binding
        else:
            return FunctionBinding(self.signature, coerce(BooleanDomain()),
                                   self.syntax, lop=lop, rops=rops)


class BindAmong(BindAmongBase):

    named('=')
    signature = AmongSig(+1)


class BindNotAmong(BindAmongBase):

    named('!=')
    signature = AmongSig(-1)


class BindTotallyEqualBase(BindFunction):

    def correlate(self, lop, rop):
        domain = coerce(lop.domain, rop.domain)
        if domain is None:
            raise BindError("incompatible arguments", self.syntax.mark)
        lop = CastBinding(lop, domain, lop.syntax)
        rop = CastBinding(rop, domain, rop.syntax)
        binding = TotalEqualityBinding(lop, rop, self.syntax)
        if self.signature.polarity == -1:
            binding = NegationBinding(binding, self.syntax)
        return binding


class BindTotallyEqual(BindTotallyEqualBase):

    named('==')
    signature = TotallyEqualSig(+1)


class BindTotallyNotEqual(BindTotallyEqualBase):

    named('!==')
    signature = TotallyEqualSig(-1)


class BindAnd(BindFunction):

    named('&')
    signature = AndSig()

    def correlate(self, lop, rop):
        lop = CastBinding(lop, coerce(BooleanDomain()), lop.syntax)
        rop = CastBinding(rop, coerce(BooleanDomain()), rop.syntax)
        return ConjunctionBinding([lop, rop], self.syntax)


class BindOr(BindFunction):

    named('|')
    signature = OrSig()

    def correlate(self, lop, rop):
        lop = CastBinding(lop, coerce(BooleanDomain()), lop.syntax)
        rop = CastBinding(rop, coerce(BooleanDomain()), rop.syntax)
        return DisjunctionBinding([lop, rop], self.syntax)


class BindNot(BindFunction):

    named('!_')
    signature = NotSig()

    def correlate(self, op):
        op = CastBinding(op, coerce(BooleanDomain()), op.syntax)
        return NegationBinding(op, self.syntax)


class BindCompare(BindFunction):

    def correlate(self, lop, rop):
        domain = coerce(lop.domain, rop.domain)
        if domain is None:
            raise BindError("incompatible arguments", self.syntax.mark)
        lop = CastBinding(lop, domain, lop.syntax)
        rop = CastBinding(rop, domain, rop.syntax)
        comparable = Comparable(domain)
        if not comparable():
            raise BindError("uncomparable arguments", self.syntax.mark)
        return FunctionBinding(self.signature, coerce(BooleanDomain()),
                               self.syntax, lop=lop, rop=rop)


class BindLessThan(BindCompare):

    named('<')
    signature = CompareSig('<')


class BindLessThanOrEqual(BindCompare):

    named('<=')
    signature = CompareSig('<=')


class BindGreaterThan(BindCompare):

    named('>')
    signature = CompareSig('>')


class BindGreaterThanOrEqual(BindCompare):

    named('>=')
    signature = CompareSig('>=')


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


class Correlate(Adapter):

    signature = None
    domains = []
    codomain = None

    def __call__(self):
        return (self.signature is not None)


class BindPolyFunction(BindFunction):

    correlation = None

    def correlate(self, **arguments):
        domains = []
        for slot in self.signature.slots:
            if not (slot.is_singular and slot.is_mandatory):
                break
            name = slot.name
            value = arguments[name]
            domains.append(value.domain)
        correlate = self.correlation(*domains)
        if not correlate():
            raise BindError("incompatible arguments", self.syntax.mark)
        correlated_arguments = arguments.copy()
        for domain, slot in zip(correlate.domains, self.signature.slots):
            name = slot.name
            value = correlated_arguments[name]
            value = CastBinding(value, coerce(domain), value.syntax)
            correlated_arguments[name] = value
        return FunctionBinding(correlate.signature, coerce(correlate.codomain),
                               self.syntax, **correlated_arguments)


class CorrelateAdd(Correlate):

    adapts(Domain, Domain)


class BindAdd(BindPolyFunction):

    named('+')
    signature = AddSig()
    correlation = CorrelateAdd


class CorrelateIntegerAdd(CorrelateAdd):

    adapts(IntegerDomain, IntegerDomain)
    signature = NumericAddSig()
    domains = [IntegerDomain(), IntegerDomain()]
    codomain = IntegerDomain()


class CorrelateDecimalAdd(CorrelateAdd):

    adapts_many((IntegerDomain, DecimalDomain),
                (DecimalDomain, IntegerDomain),
                (DecimalDomain, DecimalDomain))
    signature = NumericAddSig()
    domains = [DecimalDomain(), DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatAdd(CorrelateAdd):

    adapts_many((IntegerDomain, FloatDomain),
                (DecimalDomain, FloatDomain),
                (FloatDomain, IntegerDomain),
                (FloatDomain, DecimalDomain),
                (FloatDomain, FloatDomain))
    signature = NumericAddSig()
    domains = [FloatDomain(), FloatDomain()]
    codomain = FloatDomain()


class CorrelateDateIncrement(CorrelateAdd):

    adapts(DateDomain, IntegerDomain)
    signature = DateIncrementSig()
    domains = [DateDomain(), IntegerDomain()]
    codomain = DateDomain()


class CorrelateConcatenate(CorrelateAdd):

    adapts_many((UntypedDomain, UntypedDomain),
                (UntypedDomain, StringDomain),
                (StringDomain, UntypedDomain),
                (StringDomain, StringDomain))
    signature = ConcatenateSig()
    domains = [StringDomain(), StringDomain()]
    codomain = StringDomain()


class CorrelateSubtract(Correlate):

    adapts(Domain, Domain)


class BindSubtract(BindPolyFunction):

    named('-')
    signature = SubtractSig()
    correlation = CorrelateSubtract


class CorrelateIntegerSubtract(CorrelateSubtract):

    adapts(IntegerDomain, IntegerDomain)
    signature = NumericSubtractSig()
    domains = [IntegerDomain(), IntegerDomain()]
    codomain = IntegerDomain()


class CorrelateDecimalSubtract(CorrelateSubtract):

    adapts_many((IntegerDomain, DecimalDomain),
                (DecimalDomain, IntegerDomain),
                (DecimalDomain, DecimalDomain))
    signature = NumericSubtractSig()
    domains = [DecimalDomain(), DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatSubtract(CorrelateSubtract):

    adapts_many((IntegerDomain, FloatDomain),
                (DecimalDomain, FloatDomain),
                (FloatDomain, IntegerDomain),
                (FloatDomain, DecimalDomain),
                (FloatDomain, FloatDomain))
    signature = NumericSubtractSig()
    domains = [FloatDomain(), FloatDomain()]
    codomain = FloatDomain()


class CorrelateDateDecrement(CorrelateSubtract):

    adapts(DateDomain, IntegerDomain)
    signature = DateDecrementSig()
    domains = [DateDomain(), IntegerDomain()]
    codomain = DateDomain()


class CorrelateDateDifference(CorrelateSubtract):

    adapts(DateDomain, DateDomain)
    signature = DateDifferenceSig()
    domains = [DateDomain(), DateDomain()]
    codomain = IntegerDomain()


class CorrelateMultiply(Correlate):

    adapts(Domain, Domain)


class BindMultiply(BindPolyFunction):

    named('*')
    signature = MultiplySig()
    correlation = CorrelateMultiply


class CorrelateIntegerMultiply(CorrelateMultiply):

    adapts(IntegerDomain, IntegerDomain)
    signature = NumericMultiplySig()
    domains = [IntegerDomain(), IntegerDomain()]
    codomain = IntegerDomain()


class CorrelateDecimalMultiply(CorrelateMultiply):

    adapts_many((IntegerDomain, DecimalDomain),
                (DecimalDomain, IntegerDomain),
                (DecimalDomain, DecimalDomain))
    signature = NumericMultiplySig()
    domains = [DecimalDomain(), DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatMultiply(CorrelateMultiply):

    adapts_many((IntegerDomain, FloatDomain),
                (DecimalDomain, FloatDomain),
                (FloatDomain, IntegerDomain),
                (FloatDomain, DecimalDomain),
                (FloatDomain, FloatDomain))
    signature = NumericMultiplySig()
    domains = [FloatDomain(), FloatDomain()]
    codomain = FloatDomain()


class CorrelateDivide(Correlate):

    adapts(Domain, Domain)


class BindDivide(BindPolyFunction):

    named('/')
    signature = DivideSig()
    correlation = CorrelateDivide


class CorrelateDecimalDivide(CorrelateDivide):

    adapts_many((IntegerDomain, IntegerDomain),
                (IntegerDomain, DecimalDomain),
                (DecimalDomain, IntegerDomain),
                (DecimalDomain, DecimalDomain))
    signature = NumericDivideSig()
    domains = [DecimalDomain(), DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatDivide(CorrelateDivide):

    adapts_many((IntegerDomain, FloatDomain),
                (DecimalDomain, FloatDomain),
                (FloatDomain, IntegerDomain),
                (FloatDomain, DecimalDomain),
                (FloatDomain, FloatDomain))
    signature = NumericDivideSig()
    domains = [FloatDomain(), FloatDomain()]
    codomain = FloatDomain()


class CorrelateKeepPolarity(Correlate):

    adapts(Domain)


class BindKeepPolarity(BindPolyFunction):

    named('+_')
    signature = KeepPolaritySig()
    correlation = CorrelateKeepPolarity


class CorrelateIntegerKeepPolarity(CorrelateKeepPolarity):

    adapts(IntegerDomain)
    signature = NumericKeepPolaritySig()
    domains = [IntegerDomain()]
    codomain = IntegerDomain()


class CorrelateDecimalKeepPolarity(CorrelateKeepPolarity):

    adapts(DecimalDomain)
    signature = NumericKeepPolaritySig()
    domains = [DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatKeepPolarity(CorrelateKeepPolarity):

    adapts(FloatDomain)
    signature = NumericKeepPolaritySig()
    domains = [FloatDomain()]
    codomain = FloatDomain()


class CorrelateReversePolarity(Correlate):

    adapts(Domain)


class BindReversePolarity(BindPolyFunction):

    named('-_')
    signature = ReversePolaritySig()
    correlation = CorrelateReversePolarity


class CorrelateIntegerReversePolarity(CorrelateReversePolarity):

    adapts(IntegerDomain)
    signature = NumericReversePolaritySig()
    domains = [IntegerDomain()]
    codomain = IntegerDomain()


class CorrelateDecimalReversePolarity(CorrelateReversePolarity):

    adapts(DecimalDomain)
    signature = NumericReversePolaritySig()
    domains = [DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatReversePolarity(CorrelateReversePolarity):

    adapts(FloatDomain)
    signature = NumericReversePolaritySig()
    domains = [FloatDomain()]
    codomain = FloatDomain()


class CorrelateRound(Correlate):

    adapts(Domain)


class BindRound(BindPolyFunction):

    named('round')
    signature = RoundSig()
    correlation = CorrelateRound


class CorrelateDecimalRound(CorrelateRound):

    adapts_many(IntegerDomain,
                DecimalDomain)
    signature = RoundSig()
    domains = [DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatRound(CorrelateRound):

    adapts(FloatDomain)
    signature = RoundSig()
    domains = [FloatDomain()]
    codomain = FloatDomain()


class CorrelateRoundTo(Correlate):

    adapts(Domain, Domain)


class BindRoundTo(BindPolyFunction):

    named(('round', 2))
    signature = RoundToSig()
    correlation = CorrelateRoundTo


class CorrelateDecimalRoundTo(CorrelateRoundTo):

    adapts_many((IntegerDomain, IntegerDomain),
                (DecimalDomain, IntegerDomain))
    signature = RoundToSig()
    domains = [DecimalDomain(), IntegerDomain()]
    codomain = DecimalDomain()


class CorrelateLength(Correlate):

    adapts(Domain)


class BindLength(BindPolyFunction):

    named('length')
    signature = LengthSig()
    correlation = CorrelateLength


class CorrelateStringLength(CorrelateLength):

    adapts_many(StringDomain,
                UntypedDomain)
    signature = StringLengthSig()
    domains = [StringDomain()]
    codomain = IntegerDomain()


class CorrelateContains(Correlate):

    adapts(Domain, Domain)


class BindContains(BindPolyFunction):

    named('~')
    signature = ContainsSig(+1)
    correlation = CorrelateContains


class CorrelateStringContains(CorrelateContains):

    adapts_many((StringDomain, StringDomain),
                (StringDomain, UntypedDomain),
                (UntypedDomain, StringDomain),
                (UntypedDomain, UntypedDomain))
    signature = StringContainsSig(+1)
    domains = [StringDomain(), StringDomain()]
    codomain = BooleanDomain()


class CorrelateNotContains(Correlate):

    adapts(Domain, Domain)


class BindNotContains(BindPolyFunction):

    named('!~')
    signature = ContainsSig(-1)
    correlation = CorrelateNotContains


class CorrelateStringNotContains(CorrelateNotContains):

    adapts_many((StringDomain, StringDomain),
                (StringDomain, UntypedDomain),
                (UntypedDomain, StringDomain),
                (UntypedDomain, UntypedDomain))
    signature = StringContainsSig(-1)
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
        return FunctionBinding(self.signature, codomain, self.syntax,
                               **cast_arguments)


class BindIsNull(BindHomoFunction):

    named('is_null')
    signature = IsNullSig()
    codomain = BooleanDomain()


class BindNullIf(BindHomoFunction):

    named('null_if')
    signature = NullIfSig()


class BindIfNull(BindHomoFunction):

    named('if_null')
    signature = IfNullSig()


class BindIf(BindFunction):

    named('if')
    signature = IfSig()

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
        return FunctionBinding(self.signature, domain, self.syntax,
                               predicates=predicates,
                               consequents=consequents,
                               alternative=alternative)


class BindSwitch(BindFunction):

    named('switch')
    signature = SwitchSig()

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
        return FunctionBinding(self.signature, domain, self.syntax,
                               variable=variable,
                               variants=variants,
                               consequents=consequents,
                               alternative=alternative)


class BindExistsBase(BindFunction):

    signature = UnarySig()
    bind_signature = None

    def correlate(self, op):
        op = CastBinding(op, coerce(BooleanDomain()), op.syntax)
        return FunctionBinding(self.bind_signature, op.domain, self.syntax,
                               base=self.state.base, op=op)


class BindExists(BindExistsBase):

    named('exists')
    bind_signature = ExistsSig()


class BindEvery(BindExistsBase):

    named('every')
    bind_signature = EverySig()


class BindCount(BindFunction):

    named('count')
    signature = UnarySig()

    def correlate(self, op):
        op = CastBinding(op, coerce(BooleanDomain()), op.syntax)
        return FunctionBinding(CountSig(), coerce(IntegerDomain()),
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

    signature = UnarySig()
    correlation = None

    def correlate(self, op):
        correlate = self.correlation(op.domain)
        if not correlate():
            raise BindError("incompatible argument", self.syntax.mark)
        op = CastBinding(op, coerce(correlate.domain), op.syntax)
        return FunctionBinding(correlate.signature, coerce(correlate.codomain),
                               self.syntax, base=self.state.base, op=op)


class CorrelateMin(CorrelateAggregate):

    signature = MinSig()


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

    signature = MaxSig()


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

    signature = SumSig()


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

    signature = AvgSig()


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


