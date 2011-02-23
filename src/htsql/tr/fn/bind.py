#
# Copyright (c) 2006-2011, Prometheus Research, LLC
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
                       IntegerDomain, DecimalDomain, FloatDomain,
                       DateDomain, EnumDomain)
from ..syntax import NumberSyntax, StringSyntax, IdentifierSyntax
from ..binding import (LiteralBinding, SortBinding, SieveBinding,
                       FormulaBinding, CastBinding, WrapperBinding,
                       TitleBinding, DirectionBinding, QuotientBinding,
                       AssignmentBinding, DefinitionBinding, Binding)
from ..bind import BindByName, BindingState
from ..error import BindError
from ..coerce import coerce
from ..lookup import lookup, get_complement, get_kernel
from ..signature import (Signature, NullarySig, UnarySig, BinarySig,
                         CompareSig, IsEqualSig, IsTotallyEqualSig, IsInSig,
                         IsNullSig, IfNullSig, NullIfSig, AndSig, OrSig,
                         NotSig)
from .signature import (FiberSig, AsSig, SortDirectionSig, LimitSig,
                        SortSig, CastSig, MakeDateSig, ExtractYearSig,
                        ExtractMonthSig, ExtractDaySig,
                        AddSig, ConcatenateSig,
                        HeadSig, TailSig, SliceSig, AtSig, ReplaceSig,
                        UpperSig, LowerSig, TrimSig,
                        DateIncrementSig, SubtractSig, DateDecrementSig,
                        DateDifferenceSig, TodaySig,
                        MultiplySig, DivideSig, IfSig, SwitchSig,
                        KeepPolaritySig, ReversePolaritySig,
                        RoundSig, RoundToSig, LengthSig,
                        ContainsSig, ExistsSig, CountSig, MinMaxSig,
                        SumSig, AvgSig, AggregateSig, QuantifySig,
                        QuotientSig, KernelSig, ComplementSig,
                        AssignmentSig, DefineSig)
import sys


class BindFunction(BindByName):

    signature = None
    hint = None
    help = None

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


class Correlate(Component):

    input_signature = Signature
    input_domains = []
    input_arity = 0

    @classmethod
    def dominates(component, other):
        if component.input_signature is None:
            return False
        if other.input_signature is None:
            return False
        if issubclass(component, other):
            return True
        if (issubclass(component.input_signature, other.input_signature)
            and component.input_signature is not other.input_signature):
            return True
        return False

    @classmethod
    def matches(component, dispatch_key):
        if component.input_signature is None:
            return False
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
        raise BindError("incompatible arguments", self.binding.mark)


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


def correlates_none():
    frame = sys._getframe(1)
    frame.f_locals['input_signature'] = None
    frame.f_locals['input_domains'] = []
    frame.f_locals['input_arity'] = 0


class CorrelateFunction(Correlate):

    correlates_none()
    signature = None
    domains = []
    codomain = None

    hint = None
    help = None

    def __call__(self):
        signature = self.binding.signature
        if self.signature is not None:
            signature = signature.clone_to(self.signature)
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


class BindPolyFunction(BindFunction):

    signature = None
    codomain = UntypedDomain()

    def correlate(self, **arguments):
        binding = FormulaBinding(self.signature(), self.codomain, self.syntax,
                                 **arguments)
        correlate = Correlate(binding, self.state)
        return correlate()


class BindNull(BindMacro):

    named('null')
    signature = NullarySig
    hint = """null() -> NULL"""

    def expand(self):
        yield LiteralBinding(None, UntypedDomain(), self.syntax)


class BindTrue(BindMacro):

    named('true')
    signature = NullarySig
    hint = """true() -> TRUE"""

    def expand(self):
        yield LiteralBinding(True, coerce(BooleanDomain()), self.syntax)


class BindFalse(BindMacro):

    named('false')
    signature = NullarySig
    hint = """false() -> FALSE"""

    def expand(self):
        yield LiteralBinding(False, coerce(BooleanDomain()), self.syntax)


class BindRoot(BindMacro):

    named('root')
    signature = NullarySig
    hint = """base.root() -> the root space"""

    def expand(self):
        yield WrapperBinding(self.state.root, self.syntax)


class BindThis(BindMacro):

    named('this')
    signature = NullarySig
    hint = """base.this() -> the current base space"""

    def expand(self):
        yield WrapperBinding(self.state.base, self.syntax)


class BindFiber(BindMacro):

    named('fiber')
    signature = FiberSig
    hint = """base.fiber(T[, img][, cimg]) -> fiber product of base and T"""

    def expand(self, table, image=None, counterimage=None):
        if not isinstance(table, IdentifierSyntax):
            raise BindError("an identifier expected", table.mark)
        binding = lookup(self.state.root, table)
        if binding is None:
            raise BindError("unknown identifier", table.mark)
        binding = binding.clone(base=self.state.base)
        if image is None and counterimage is None:
            yield WrapperBinding(binding, self.syntax)
            return
        if image is None:
            image = counterimage
        if counterimage is None:
            counterimage = image
        parent = self.state.bind(image)
        child = self.state.bind(counterimage, base=binding)
        domain = coerce(parent.domain, child.domain)
        if domain is None:
            raise BindError("incompatible images", self.syntax.mark)
        parent = CastBinding(parent, domain, parent.syntax)
        child = CastBinding(child, domain, child.syntax)
        condition = FormulaBinding(IsEqualSig(+1), coerce(BooleanDomain()),
                                   self.syntax, lop=parent, rop=child)
        yield SieveBinding(binding, condition, self.syntax)


class BindQuotient(BindMacro):

    named('quotient')
    signature = QuotientSig

    def expand(self, seed, kernel):
        seed_binding = self.state.bind(seed)
        kernel_bindings = []
        for expression in kernel:
            kernel_bindings.extend(self.state.bind_all(expression,
                                                       base=seed_binding))
        yield QuotientBinding(self.state.base, seed_binding, kernel_bindings,
                              self.syntax)


class BindQuotientOperator(BindMacro):

    named('^')
    signature = QuotientSig

    def expand(self, seed, kernel):
        seed_binding = self.state.bind(seed)
        kernel_bindings = []
        for expression in kernel:
            kernel_bindings.extend(self.state.bind_all(expression,
                                                       base=seed_binding))
        yield QuotientBinding(self.state.base, seed_binding, kernel_bindings,
                              self.syntax)


class BindKernel(BindMacro):

    named('kernel')
    signature = KernelSig

    def expand(self, index=None):
        if index is not None:
            if not isinstance(index, NumberSyntax):
                raise BindError("expected a numeric literal", index.mark)
            try:
                index = int(index.value)
            except ValueError:
                raise BindError("expected an integer value", index.mark)
        bindings = get_kernel(self.state.base, self.syntax)
        if bindings is None:
            raise BindError("expected a quotient context", self.syntax.mark)
        if index is not None:
            if not (0 <= index < len(bindings)):
                raise BindError("index is out of range", self.syntax.mark)
            yield bindings[index]
        else:
            for binding in bindings:
                yield binding


class BindComplement(BindMacro):

    named('complement')
    signature = ComplementSig

    def expand(self):
        binding = get_complement(self.state.base, self.syntax)
        if binding is None:
            raise BindError("expected a quotient context", self.syntax.mark)
        yield binding


class BindAs(BindMacro):

    named('as')
    signature = AsSig
    hint = """as(expr, title) -> expression with a title"""
    help = """
    Decorates an expression with a title.

    `expr`: an arbitrary expression.
    `title`: an identifier or a string literal.
    """

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
    hint = """(expr +) -> sort in ascending order"""
    help = """
    Decorates an expression with a sort order indicator.
    """


class BindDescDir(BindDirectionBase):

    named('_-')
    direction = -1
    hint = """(expr -) -> sort in descending order"""


class BindLimit(BindMacro):

    named('limit')
    signature = LimitSig
    hint = """base.limit(N[, skip]) -> slice of the base space"""

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
    hint = """base.sort(expr[, ...]) -> sorted space"""

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


class BindAssignment(BindMacro):

    named(':=')
    signature = AssignmentSig

    def expand(self, lop, rop):
        if not isinstance(lop, IdentifierSyntax):
            raise BindError("an identifier expected", lop.mark)
        name = lop.value
        yield AssignmentBinding(name, rop, self.syntax)


class BindDefine(BindMacro):

    named('define')
    signature = DefineSig

    def expand(self, ops):
        binding = self.state.base
        for op in ops:
            assignment = self.state.bind(op)
            if not isinstance(assignment, AssignmentBinding):
                raise BindError("an assignment expected", op.mark)
            body = self.state.bind(assignment.body, base=binding)
            binding = DefinitionBinding(binding, assignment.name,
                                        body, self.syntax)
        yield binding


class BindCast(BindFunction):

    signature = CastSig
    codomain = None

    def correlate(self, base):
        domain = coerce(self.codomain)
        return CastBinding(base, domain, self.syntax)


class BindBooleanCast(BindCast):

    named('boolean', 'bool')
    codomain = BooleanDomain()
    hint = """boolean(expr) -> expression converted to Boolean"""


class BindStringCast(BindCast):

    named('string', 'str')
    codomain = StringDomain()
    hint = """string(expr) -> expression converted to a string"""


class BindIntegerCast(BindCast):

    named('integer', 'int')
    codomain = IntegerDomain()
    hint = """integer(expr) -> expression converted to integer"""


class BindDecimalCast(BindCast):

    named('decimal', 'dec')
    codomain = DecimalDomain()
    hint = """decimal(expr) -> expression converted to decimal"""


class BindFloatCast(BindCast):

    named('float')
    codomain = FloatDomain()
    hint = """float(expr) -> expression converted to float"""


class BindDateCast(BindCast):

    named('date')
    codomain = DateDomain()
    hint = """date(expr) -> expression converted to date"""


class BindMakeDate(BindMonoFunction):

    named(('date', 3))
    signature = MakeDateSig
    domains = [IntegerDomain(), IntegerDomain(), IntegerDomain()]
    codomain = DateDomain()
    hint = """date(year, month, day) -> date value"""


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
    hint = """(x = y) -> TRUE if x is equal to y"""


class BindNotAmong(BindAmongBase):

    named('!=')
    polarity = -1
    hint = """(x != y) -> TRUE if x is not equal to y"""


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
    hint = """(x == y) -> TRUE if x is equal to y"""


class BindTotallyNotEqual(BindTotallyEqualBase):

    named('!==')
    polarity = -1
    hint = """(x !== y) -> TRUE if x is not equal to y"""


class BindAnd(BindFunction):

    named('&')
    signature = BinarySig
    hint = """(p & q) -> TRUE if both p and q are TRUE"""

    def correlate(self, lop, rop):
        domain = coerce(BooleanDomain())
        lop = CastBinding(lop, domain, lop.syntax)
        rop = CastBinding(rop, domain, rop.syntax)
        return FormulaBinding(AndSig(), domain, self.syntax, ops=[lop, rop])


class BindOr(BindFunction):

    named('|')
    signature = BinarySig
    hint = """(p | q) -> TRUE if either p or q is TRUE"""

    def correlate(self, lop, rop):
        domain = coerce(BooleanDomain())
        lop = CastBinding(lop, domain, lop.syntax)
        rop = CastBinding(rop, domain, rop.syntax)
        return FormulaBinding(OrSig(), domain, self.syntax, ops=[lop, rop])


class BindNot(BindFunction):

    named('!_')
    signature = NotSig
    hint = """(! p) -> TRUE if p is FALSE"""

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
    hint = """(x < y) -> TRUE if x is less than y"""


class BindLessThanOrEqual(BindCompare):

    named('<=')
    relation = '<='
    hint = """(x < y) -> TRUE if x is less than or equal to y"""


class BindGreaterThan(BindCompare):

    named('>')
    relation = '>'
    hint = """(x > y) -> TRUE if x is greater than y"""


class BindGreaterThanOrEqual(BindCompare):

    named('>=')
    relation = '>='
    hint = """(x >= y) -> TRUE if x is greater than or equal to y"""


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


class BindAdd(BindPolyFunction):

    named('+')
    signature = AddSig
    hint = """(x + y) -> sum of x and y"""


class CorrelateIntegerAdd(CorrelateFunction):

    correlates(AddSig, (IntegerDomain, IntegerDomain))
    signature = AddSig
    domains = [IntegerDomain(), IntegerDomain()]
    codomain = IntegerDomain()


class CorrelateDecimalAdd(CorrelateFunction):

    correlates(AddSig, (IntegerDomain, DecimalDomain),
                       (DecimalDomain, IntegerDomain),
                       (DecimalDomain, DecimalDomain))
    signature = AddSig
    domains = [DecimalDomain(), DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatAdd(CorrelateFunction):

    correlates(AddSig, (IntegerDomain, FloatDomain),
                       (DecimalDomain, FloatDomain),
                       (FloatDomain, IntegerDomain),
                       (FloatDomain, DecimalDomain),
                       (FloatDomain, FloatDomain))
    signature = AddSig
    domains = [FloatDomain(), FloatDomain()]
    codomain = FloatDomain()


class CorrelateDateIncrement(CorrelateFunction):

    correlates(AddSig, (DateDomain, IntegerDomain))
    signature = DateIncrementSig
    domains = [DateDomain(), IntegerDomain()]
    codomain = DateDomain()


class CorrelateConcatenate(CorrelateFunction):

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
    hint = """(x - y) -> difference between x and y"""


class CorrelateIntegerSubtract(CorrelateFunction):

    correlates(SubtractSig, (IntegerDomain, IntegerDomain))
    signature = SubtractSig
    domains = [IntegerDomain(), IntegerDomain()]
    codomain = IntegerDomain()


class CorrelateDecimalSubtract(CorrelateFunction):

    correlates(SubtractSig, (IntegerDomain, DecimalDomain),
                            (DecimalDomain, IntegerDomain),
                            (DecimalDomain, DecimalDomain))
    signature = SubtractSig
    domains = [DecimalDomain(), DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatSubtract(CorrelateFunction):

    correlates(SubtractSig, (IntegerDomain, FloatDomain),
                            (DecimalDomain, FloatDomain),
                            (FloatDomain, IntegerDomain),
                            (FloatDomain, DecimalDomain),
                            (FloatDomain, FloatDomain))
    signature = SubtractSig
    domains = [FloatDomain(), FloatDomain()]
    codomain = FloatDomain()


class CorrelateDateDecrement(CorrelateFunction):

    correlates(SubtractSig, (DateDomain, IntegerDomain))
    signature = DateDecrementSig
    domains = [DateDomain(), IntegerDomain()]
    codomain = DateDomain()


class CorrelateDateDifference(CorrelateFunction):

    correlates(SubtractSig, (DateDomain, DateDomain))
    signature = DateDifferenceSig
    domains = [DateDomain(), DateDomain()]
    codomain = IntegerDomain()


class BindMultiply(BindPolyFunction):

    named('*')
    signature = MultiplySig
    hint = """(x * y) -> product of x and y"""


class CorrelateIntegerMultiply(CorrelateFunction):

    correlates(MultiplySig, (IntegerDomain, IntegerDomain))
    signature = MultiplySig
    domains = [IntegerDomain(), IntegerDomain()]
    codomain = IntegerDomain()


class CorrelateDecimalMultiply(CorrelateFunction):

    correlates(MultiplySig, (IntegerDomain, DecimalDomain),
                            (DecimalDomain, IntegerDomain),
                            (DecimalDomain, DecimalDomain))
    signature = MultiplySig
    domains = [DecimalDomain(), DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatMultiply(CorrelateFunction):

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
    hint = """(x / y) -> quotient of x divided by y"""


class CorrelateDecimalDivide(CorrelateFunction):

    correlates(DivideSig, (IntegerDomain, IntegerDomain),
                          (IntegerDomain, DecimalDomain),
                          (DecimalDomain, IntegerDomain),
                          (DecimalDomain, DecimalDomain))
    signature = DivideSig
    domains = [DecimalDomain(), DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatDivide(CorrelateFunction):

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
    hint = """(+ x) -> x"""


class CorrelateIntegerKeepPolarity(CorrelateFunction):

    correlates(KeepPolaritySig, IntegerDomain)
    signature = KeepPolaritySig
    domains = [IntegerDomain()]
    codomain = IntegerDomain()


class CorrelateDecimalKeepPolarity(CorrelateFunction):

    correlates(KeepPolaritySig, DecimalDomain)
    signature = KeepPolaritySig
    domains = [DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatKeepPolarity(CorrelateFunction):

    correlates(KeepPolaritySig, FloatDomain)
    signature = KeepPolaritySig
    domains = [FloatDomain()]
    codomain = FloatDomain()


class BindReversePolarity(BindPolyFunction):

    named('-_')
    signature = ReversePolaritySig
    hint = """(- x) -> negation of x"""


class CorrelateIntegerReversePolarity(CorrelateFunction):

    correlates(ReversePolaritySig, IntegerDomain)
    signature = ReversePolaritySig
    domains = [IntegerDomain()]
    codomain = IntegerDomain()


class CorrelateDecimalReversePolarity(CorrelateFunction):

    correlates(ReversePolaritySig, DecimalDomain)
    signature = ReversePolaritySig
    domains = [DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatReversePolarity(CorrelateFunction):

    correlates(ReversePolaritySig, FloatDomain)
    signature = ReversePolaritySig
    domains = [FloatDomain()]
    codomain = FloatDomain()


class BindRound(BindPolyFunction):

    named('round')
    signature = RoundSig
    hint = """round(x) -> x rounded to zero"""


class CorrelateDecimalRound(CorrelateFunction):

    correlates(RoundSig, IntegerDomain,
                         DecimalDomain)
    signature = RoundSig
    domains = [DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatRound(CorrelateFunction):

    correlates(RoundSig, FloatDomain)
    signature = RoundSig
    domains = [FloatDomain()]
    codomain = FloatDomain()


class BindRoundTo(BindPolyFunction):

    named(('round', 2))
    signature = RoundToSig
    hint = """round(x, n) -> x rounded to a given precision"""


class CorrelateDecimalRoundTo(CorrelateFunction):

    correlates(RoundToSig, (IntegerDomain, IntegerDomain),
                           (DecimalDomain, IntegerDomain))
    signature = RoundToSig
    domains = [DecimalDomain(), IntegerDomain()]
    codomain = DecimalDomain()


class BindLength(BindPolyFunction):

    named('length')
    signature = LengthSig
    hint = """length(s) -> length of s"""


class CorrelateStringLength(CorrelateFunction):

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
    hint = """(s ~ sub) -> TRUE if s contains sub"""


class BindNotContains(BindContainsBase):

    named('!~')
    polarity = -1
    hint = """(s !~ sub) -> TRUE if s does not contain sub"""


class CorrelateStringContains(CorrelateFunction):

    correlates(ContainsSig, (StringDomain, StringDomain),
                            (StringDomain, UntypedDomain),
                            (UntypedDomain, StringDomain),
                            (UntypedDomain, UntypedDomain))
    signature = ContainsSig
    domains = [StringDomain(), StringDomain()]
    codomain = BooleanDomain()


class BindHeadTailBase(BindPolyFunction):

    signature = None

    def correlate(self, op, length):
        if length is not None:
            length = CastBinding(length, coerce(IntegerDomain()), length.syntax)
        binding = FormulaBinding(self.signature(), UntypedDomain(),
                                 self.syntax, op=op, length=length)
        correlate = Correlate(binding, self.state)
        return correlate()


class BindHead(BindPolyFunction):

    named('head')
    signature = HeadSig
    hint = """head(s[, N=1]) -> the first N elements of s"""


class BindTail(BindPolyFunction):

    named('tail')
    signature = TailSig
    hint = """tail(s[, N=1]) -> the last N elements of s"""


class CorrelateHead(CorrelateFunction):

    correlates(HeadSig, UntypedDomain,
                        StringDomain)
    domains = [StringDomain()]
    codomain = StringDomain()


class CorrelateTail(CorrelateFunction):

    correlates(TailSig, UntypedDomain,
                        StringDomain)
    domains = [StringDomain()]
    codomain = StringDomain()


class BindSlice(BindPolyFunction):

    named('slice')
    signature = SliceSig
    hint = """slice(s, i, j) -> slice of s from i-th to j-th elements"""

    def correlate(self, op, left, right):
        if left is not None:
            left = CastBinding(left, coerce(IntegerDomain()), left.syntax)
        if right is not None:
            right = CastBinding(right, coerce(IntegerDomain()), right.syntax)
        binding = FormulaBinding(self.signature(), UntypedDomain(),
                                 self.syntax, op=op, left=left, right=right)
        correlate = Correlate(binding, self.state)
        return correlate()


class CorrelateSlice(CorrelateFunction):

    correlates(SliceSig, UntypedDomain,
                         StringDomain)
    domains = [StringDomain()]
    codomain = StringDomain()


class BindAt(BindPolyFunction):

    named('at')
    signature = AtSig
    hint = """at(s, i[, len=1]) -> i-th to (i+len)-th elements of s"""

    def correlate(self, op, index, length):
        index = CastBinding(index, coerce(IntegerDomain()), index.syntax)
        if length is not None:
            length = CastBinding(length, coerce(IntegerDomain()),
                                 length.syntax)
        binding = FormulaBinding(self.signature(), UntypedDomain(),
                                 self.syntax, op=op, index=index, length=length)
        correlate = Correlate(binding, self.state)
        return correlate()


class CorrelateAt(CorrelateFunction):

    correlates(AtSig, UntypedDomain,
                      StringDomain)
    domains = [StringDomain()]
    codomain = StringDomain()


class BindReplace(BindPolyFunction):

    named('replace')
    signature = ReplaceSig
    hint = """replace(s, o, n) -> s with occurences of o replaced by n"""


class CorrelateReplace(CorrelateFunction):

    correlates(ReplaceSig, UntypedDomain,
                           StringDomain)
    domains = [StringDomain(), StringDomain(), StringDomain()]
    codomain = StringDomain()


class BindUpper(BindPolyFunction):

    named('upper')
    signature = UpperSig
    hint = """upper(s) -> s converted to uppercase"""


class CorrelateUpper(CorrelateFunction):

    correlates(UpperSig, UntypedDomain,
                         StringDomain)
    domains = [StringDomain()]
    codomain = StringDomain()


class BindLower(BindPolyFunction):

    named('lower')
    signature = LowerSig
    hint = """lower(s) -> s converted to lowercase"""


class CorrelateLower(CorrelateFunction):

    correlates(LowerSig, UntypedDomain,
                         StringDomain)
    domains = [StringDomain()]
    codomain = StringDomain()


class BindTrimBase(BindPolyFunction):

    signature = TrimSig
    is_left = False
    is_right = False

    def correlate(self, op):
        signature = self.signature(is_left=self.is_left,
                                   is_right=self.is_right)
        binding = FormulaBinding(signature, UntypedDomain(), self.syntax, op=op)
        correlate = Correlate(binding, self.state)
        return correlate()


class BindTrim(BindTrimBase):

    named('trim')
    is_left = True
    is_right = True
    hint = """trim(s) -> s with leading and trailing whitespaces removed"""


class BindLTrim(BindTrimBase):

    named('ltrim')
    is_left = True
    hint = """ltrim(s) -> s with leading whitespaces removed"""


class BindRTrim(BindTrimBase):

    named('rtrim')
    is_right = True
    hint = """rtrim(s) -> s with trailing whitespaces removed"""


class BindToday(BindMonoFunction):

    named('today')
    signature = TodaySig
    codomain = DateDomain()
    hint = """today() -> the current date"""


class BindExtractYear(BindPolyFunction):

    named('year')
    signature = ExtractYearSig
    hint = """year(date) -> the year of a given date"""


class BindExtractMonth(BindPolyFunction):

    named('month')
    signature = ExtractMonthSig
    hint = """month(date) -> the month of a given date"""


class BindExtractDay(BindPolyFunction):

    named('day')
    signature = ExtractDaySig
    hint = """day(date) -> the day of a given date"""


class CorrelateExtractYear(CorrelateFunction):

    correlates(ExtractYearSig, DateDomain)
    domains = [DateDomain()]
    codomain = IntegerDomain()


class CorrelateExtractMonth(CorrelateFunction):

    correlates(ExtractMonthSig, DateDomain)
    domains = [DateDomain()]
    codomain = IntegerDomain()


class CorrelateExtractDay(CorrelateFunction):

    correlates(ExtractDaySig, DateDomain)
    domains = [DateDomain()]
    codomain = IntegerDomain()


class CorrelateTrim(CorrelateFunction):

    correlates(TrimSig, UntypedDomain,
                        StringDomain)
    domains = [StringDomain()]
    codomain = StringDomain()


class BindIsNull(BindHomoFunction):

    named('is_null')
    signature = IsNullSig(+1)
    codomain = BooleanDomain()
    hint = """is_null(x) -> TRUE if x is NULL"""


class BindNullIf(BindHomoFunction):

    named('null_if')
    signature = NullIfSig()
    hint = """null_if(x, y) -> NULL if x is equal to y; x otherwise"""


class BindIfNull(BindHomoFunction):

    named('if_null')
    signature = IfNullSig()
    hint = """if_null(x, y) -> y if x is NULL; x otherwise"""


class BindIf(BindFunction):

    named('if')
    signature = IfSig
    hint = """if(p, c[, ...][, a=NULL]) -> c if p; a otherwise"""

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
            raise BindError("incompatible arguments", self.syntax.mark)
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
    hint = """switch(x, v, c, [...][, a=NULL]) -> c if x = v; a otherwise"""

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
            raise BindError("incompatible arguments", self.syntax.mark)
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

    signature = ExistsSig
    polarity = None

    def correlate(self, op):
        op = CastBinding(op, coerce(BooleanDomain()), op.syntax)
        return FormulaBinding(QuantifySig(self.polarity), op.domain,
                              self.syntax, base=self.state.base, op=op)


class BindExists(BindExistsBase):

    named('exists')
    polarity = +1
    hint = """base.exists(p) -> TRUE if there exists p such that p = TRUE"""


class BindEvery(BindExistsBase):

    named('every')
    polarity = -1
    hint = """base.every(p) -> TRUE if p = TRUE for every p"""


class BindCount(BindFunction):

    named('count')
    signature = CountSig
    hint = """base.count(p) -> the number of p such that p = TRUE"""

    def correlate(self, op):
        op = CastBinding(op, coerce(BooleanDomain()), op.syntax)
        op = FormulaBinding(CountSig(), coerce(IntegerDomain()),
                            self.syntax, op=op)
        return FormulaBinding(AggregateSig(), op.domain, self.syntax,
                              base=self.state.base, op=op)


class BindPolyAggregate(BindPolyFunction):

    signature = UnarySig
    codomain = UntypedDomain()

    def correlate(self, op):
        binding = FormulaBinding(self.signature(), self.codomain, self.syntax,
                                 op=op)
        correlate = Correlate(binding, self.state)
        return correlate()


class CorrelateAggregate(CorrelateFunction):

    correlates_none()
    signature = None
    domains = []
    codomain = None

    def __call__(self):
        op = super(CorrelateAggregate, self).__call__()
        return FormulaBinding(AggregateSig(), op.domain, op.syntax,
                              base=self.state.base, op=op)


class BindMinMaxBase(BindPolyAggregate):

    signature = MinMaxSig
    polarity = None

    def correlate(self, op):
        binding = FormulaBinding(self.signature(self.polarity), self.codomain,
                                 self.syntax, op=op)
        correlate = Correlate(binding, self.state)
        return correlate()


class BindMinMaxBase(BindMinMaxBase):

    named('min')
    signature = MinMaxSig
    polarity = +1
    hint = """base.min(x) -> the minimal value in the set of x"""


class BindMinMaxBase(BindMinMaxBase):

    named('max')
    signature = MinMaxSig
    polarity = -1
    hint = """base.avg(x) -> the maximal value in the set of x"""


class CorrelateIntegerMinMax(CorrelateAggregate):

    correlates(MinMaxSig, IntegerDomain)
    signature = MinMaxSig
    domains = [IntegerDomain()]
    codomain = IntegerDomain()


class CorrelateDecimalMinMax(CorrelateAggregate):

    correlates(MinMaxSig, DecimalDomain)
    signature = MinMaxSig
    domains = [DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatMinMax(CorrelateAggregate):

    correlates(MinMaxSig, FloatDomain)
    signature = MinMaxSig
    domains = [FloatDomain()]
    codomain = FloatDomain()


class CorrelateStringMinMax(CorrelateAggregate):

    correlates(MinMaxSig, StringDomain)
    signature = MinMaxSig
    domains = [StringDomain()]
    codomain = StringDomain()


class CorrelateDateMinMax(CorrelateAggregate):

    correlates(MinMaxSig, DateDomain)
    signature = MinMaxSig
    domains = [DateDomain()]
    codomain = DateDomain()


class BindSum(BindPolyAggregate):

    named('sum')
    signature = SumSig
    hint = """base.sum(x) -> the sum of x"""


class CorrelateIntegerSum(CorrelateAggregate):

    correlates(SumSig, IntegerDomain)
    signature = SumSig
    domains = [IntegerDomain()]
    codomain = IntegerDomain()


class CorrelateDecimalSum(CorrelateAggregate):

    correlates(SumSig, DecimalDomain)
    signature = SumSig
    domains = [DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatSum(CorrelateAggregate):

    correlates(SumSig, FloatDomain)
    signature = SumSig
    domains = [FloatDomain()]
    codomain = FloatDomain()


class BindAvg(BindPolyAggregate):

    named('avg')
    signature = AvgSig
    hint = """base.avg(x) -> the average value of x"""


class CorrelateDecimalAvg(CorrelateAggregate):

    correlates(AvgSig, IntegerDomain,
                       DecimalDomain)
    signature = AvgSig
    domains = [DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatAvg(CorrelateAggregate):

    correlates(AvgSig, FloatDomain)
    signature = AvgSig
    domains = [FloatDomain()]
    codomain = FloatDomain()


