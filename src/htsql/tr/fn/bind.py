#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql.tr.fn.bind`
=======================
"""


from ...util import aresubclasses
from ...adapter import Adapter, Component, adapts, adapts_many, named
from ...domain import (Domain, UntypedDomain, BooleanDomain, StringDomain,
                       IntegerDomain, DecimalDomain, FloatDomain,
                       DateDomain, TimeDomain, DateTimeDomain, EnumDomain)
from ..syntax import (NumberSyntax, StringSyntax, IdentifierSyntax,
                      ReferenceSyntax, SpecifierSyntax, ApplicationSyntax,
                      FunctionSyntax, GroupSyntax)
from ..binding import (LiteralBinding, SortBinding, SieveBinding,
                       FormulaBinding, CastBinding, WrappingBinding,
                       TitleBinding, DirectionBinding, QuotientBinding,
                       AssignmentBinding, DefinitionBinding, AliasBinding,
                       SelectionBinding, HomeBinding, RescopingBinding,
                       CoverBinding, ForkBinding, LinkBinding,
                       CommandBinding, SegmentBinding, QueryBinding, Binding,
                       BindingRecipe, ComplementRecipe, KernelRecipe,
                       SubstitutionRecipe, ClosedRecipe)
from ..bind import BindByName, BindByRecipe, BindingState
from ..error import BindError
from ..coerce import coerce
from ..lookup import (lookup_attribute, lookup_complement, direct, expand,
                      guess_name, lookup_command)
from ..signature import (Signature, NullarySig, UnarySig, BinarySig,
                         CompareSig, IsEqualSig, IsTotallyEqualSig, IsInSig,
                         IsNullSig, IfNullSig, NullIfSig, AndSig, OrSig,
                         NotSig)
from .signature import (FiberSig, AsSig, SortDirectionSig, LimitSig,
                        SortSig, CastSig, MakeDateSig, MakeDateTimeSig,
                        CombineDateTimeSig,
                        ExtractYearSig, ExtractMonthSig, ExtractDaySig,
                        ExtractHourSig, ExtractMinuteSig, ExtractSecondSig,
                        AddSig, ConcatenateSig,
                        HeadSig, TailSig, SliceSig, AtSig, ReplaceSig,
                        UpperSig, LowerSig, TrimSig,
                        DateIncrementSig, DateTimeIncrementSig,
                        SubtractSig, DateDecrementSig, DateTimeDecrementSig,
                        DateDifferenceSig, TodaySig, NowSig,
                        MultiplySig, DivideSig, IfSig, SwitchSig,
                        KeepPolaritySig, ReversePolaritySig,
                        RoundSig, RoundToSig, TruncSig, TruncToSig, LengthSig,
                        ContainsSig, ExistsSig, CountSig, MinMaxSig,
                        SumSig, AvgSig, AggregateSig, QuantifySig,
                        QuotientSig, AssignmentSig, DefineSig,
                        WhereSig, SelectSig, LinkSig)
from ...cmd.command import (DefaultCmd, RetrieveCmd, TextCmd, HTMLCmd, JSONCmd,
                            CSVCmd, TSVCmd, SQLCmd)
import sys


class BindFunction(BindByName):

    signature = None
    hint = None
    help = None

    def match(self):
        assert self.signature is not None
        arguments = {}
        if isinstance(self.syntax, ApplicationSyntax):
            operands = self.syntax.arguments[:]
        else:
            operands = []
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
                    bound_value = self.state.bind(value)
                    #if expand(bound_value) is not None:
                    #    raise BindError("unexpected list argument",
                    #                    value.mark)
            else:
                if len(value) > 1:
                    bound_value = [self.state.bind(item) for item in value]
                elif len(value) == 1:
                    [value] = value
                    bound_value = self.state.bind(value)
                    recipies = expand(bound_value, is_hard=False)
                    if slot.is_mandatory and (recipies is not None and
                                              not recipies):
                        raise BindError("missing argument %s" % name,
                                        value.mark)
                    if recipies is None:
                        bound_value = [bound_value]
                    else:
                        bound_value = []
                        for syntax, recipe in recipies:
                            bind = BindByRecipe(recipe, syntax, self.state)
                            bound_value.append(bind())
                else:
                    bound_value = []
            bound_arguments[name] = bound_value
        return bound_arguments

    def correlate(self, **arguments):
        raise NotImplementedError()

    def __call__(self):
        arguments = self.bind()
        return self.correlate(**arguments)


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
        return FormulaBinding(self.state.scope,
                              self.signature(), coerce(self.codomain),
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
        return FormulaBinding(self.state.scope,
                              self.signature, codomain, self.syntax,
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
        return FormulaBinding(self.state.scope,
                              signature, domain, self.binding.syntax,
                              **arguments)


class BindPolyFunction(BindFunction):

    signature = None
    codomain = UntypedDomain()

    def correlate(self, **arguments):
        binding = FormulaBinding(self.state.scope,
                                 self.signature(), self.codomain, self.syntax,
                                 **arguments)
        correlate = Correlate(binding, self.state)
        return correlate()


class BindCommand(BindMacro):

    signature = UnarySig


class BindRetrieve(BindCommand):

    named('retrieve')

    def expand(self, op):
        op = self.state.bind(op)
        if not isinstance(op, SegmentBinding):
            raise BindError("a segment is required", op.mark)
        binding = QueryBinding(self.state.root, op, op.syntax)
        command = RetrieveCmd(binding)
        return CommandBinding(self.state.scope, command, self.syntax)


class BindFormat(BindCommand):

    command = None

    def expand(self, op):
        op = self.state.bind(op)
        producer = lookup_command(op)
        if producer is None:
            if not isinstance(op, SegmentBinding):
                raise BindError("a segment is required", op.mark)
            binding = QueryBinding(self.state.root, op, op.syntax)
            producer = DefaultCmd(binding)
        command = self.command(producer)
        return CommandBinding(self.state.scope, command, self.syntax)


class BindText(BindFormat):

    named('txt')
    command = TextCmd


class BindHTML(BindFormat):

    named('html')
    command = HTMLCmd


class BindJSON(BindFormat):

    named('json')
    command = JSONCmd


class BindCSV(BindFormat):

    named('csv')
    command = CSVCmd


class BindTSV(BindFormat):

    named('tsv')
    command = TSVCmd


class BindSQL(BindFormat):

    named('sql')
    command = SQLCmd


class BindNull(BindMacro):

    named('null', ('null', None))
    signature = NullarySig
    hint = """null() -> NULL"""

    def expand(self):
        return LiteralBinding(self.state.scope,
                              None, UntypedDomain(), self.syntax)


class BindTrue(BindMacro):

    named('true', ('true', None))
    signature = NullarySig
    hint = """true() -> TRUE"""

    def expand(self):
        return LiteralBinding(self.state.scope,
                              True, coerce(BooleanDomain()), self.syntax)


class BindFalse(BindMacro):

    named('false', ('false', None))
    signature = NullarySig
    hint = """false() -> FALSE"""

    def expand(self):
        return LiteralBinding(self.state.scope,
                              False, coerce(BooleanDomain()), self.syntax)


class BindRoot(BindMacro):

    named('root')
    signature = NullarySig
    hint = """base.root() -> the root space"""

    def expand(self):
        return WrappingBinding(self.state.root, self.syntax)


class BindThis(BindMacro):

    named('this')
    signature = NullarySig
    hint = """base.this() -> the current base space"""

    def expand(self):
        return WrappingBinding(self.state.scope, self.syntax)


class BindHome(BindMacro):

    named('home')
    signature = NullarySig

    def expand(self):
        return HomeBinding(self.state.scope, self.syntax)


class BindDistinct(BindMacro):

    named('distinct')
    signature = UnarySig

    def expand(self, op):
        seed = self.state.bind(op)
        recipes = expand(seed, is_hard=False)
        if recipes is None:
            raise BindError("a selector is required", op.mark)
        kernels = []
        for syntax, recipe in recipes:
            element = self.state.use(recipe, syntax, scope=seed)
            element = RescopingBinding(element, seed, element.syntax)
            domain = coerce(element.domain)
            if domain is None:
                raise BindError("invalid element type", element.mark)
            element = CastBinding(element, domain, element.syntax)
            kernels.append(element)
        quotient = QuotientBinding(self.state.scope, seed, kernels,
                                   self.syntax)
        binding = quotient
        name = guess_name(seed)
        if name is not None:
            recipe = ComplementRecipe(quotient)
            recipe = ClosedRecipe(recipe)
            binding = DefinitionBinding(binding, name, False, None, recipe,
                                        self.syntax)
        for index, kernel in enumerate(kernels):
            name = guess_name(kernel)
            if name is not None:
                recipe = KernelRecipe(quotient, index)
                recipe = ClosedRecipe(recipe)
                binding = DefinitionBinding(binding, name, False, None, recipe,
                                            self.syntax)
        return binding


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
        return TitleBinding(base, title.value, self.syntax)


class BindSieve(BindMacro):

    named('?')
    signature = BinarySig

    def expand(self, lop, rop):
        base = self.state.bind(lop)
        filter = self.state.bind(rop, base)
        filter = CastBinding(filter, coerce(BooleanDomain()), filter.syntax)
        return SieveBinding(base, filter, self.syntax)


class BindFilter(BindMacro):

    named('filter')
    signature = UnarySig

    def expand(self, op):
        filter = self.state.bind(op)
        filter = CastBinding(filter, coerce(BooleanDomain()), filter.syntax)
        return SieveBinding(self.state.scope, filter, self.syntax)


class BindSelect(BindMacro):

    named('select')
    signature = SelectSig

    def expand(self, ops):
        elements = []
        for op in ops:
            element = self.state.bind(op)
            recipies = expand(element, is_hard=False)
            if recipies is not None:
                for syntax, recipe in recipies:
                    if not isinstance(syntax, (IdentifierSyntax, GroupSyntax)):
                        syntax = GroupSyntax(syntax, syntax.mark)
                    syntax = SpecifierSyntax(element.syntax, syntax,
                                             syntax.mark)
                    bind = BindByRecipe(recipe, syntax, self.state)
                    elements.append(bind())
            else:
                elements.append(element)
        order = []
        for element in elements:
            direction = direct(element)
            if direction is not None:
                order.append(element)
        base = self.state.scope
        if order:
            base = SortBinding(base, order, None, None, base.syntax)
        return SelectionBinding(base, elements, base.syntax)


class BindMoniker(BindMacro):

    named('moniker')
    signature = LinkSig

    def expand(self, seed):
        seed = self.state.bind(seed)
        return CoverBinding(self.state.scope, seed, self.syntax)


class BindFork(BindMacro):

    named('fork')
    signature = SelectSig

    def expand(self, ops):
        elements = []
        for op in ops:
            element = self.state.bind(op)
            recipies = expand(element, is_hard=False)
            if recipies is not None:
                for syntax, recipe in recipies:
                    if not isinstance(syntax, (IdentifierSyntax, GroupSyntax)):
                        syntax = GroupSyntax(syntax, syntax.mark)
                    syntax = SpecifierSyntax(element.syntax, syntax,
                                             syntax.mark)
                    bind = BindByRecipe(recipe, syntax, self.state)
                    elements.append(bind())
            else:
                elements.append(element)
        return ForkBinding(self.state.scope, elements, self.syntax)


class BindDirectionBase(BindMacro):

    signature = SortDirectionSig
    direction = None

    def expand(self, base):
        base = self.state.bind(base)
        return DirectionBinding(base, self.direction, self.syntax)


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
        return SortBinding(self.state.scope, [], limit, offset, self.syntax)


class BindSort(BindMacro):

    named('sort')
    signature = SortSig
    hint = """base.sort(expr[, ...]) -> sorted space"""

    def expand(self, order):
        bindings = []
        for item in order:
            binding = self.state.bind(item)
            recipies = expand(binding, is_hard=False)
            if recipies is None:
                domain = coerce(binding.domain)
                if domain is None:
                    raise BindError("incompatible expression type",
                                    binding.mark)
                binding = CastBinding(binding, domain, binding.syntax)
                bindings.append(binding)
            else:
                for syntax, recipe in recipies:
                    bind = BindByRecipe(recipe, syntax, self.state)
                    binding = bind()
                    bindings.append(binding)
        return SortBinding(self.state.scope, bindings, None, None, self.syntax)


class BindDefine(BindMacro):

    named('define')
    signature = DefineSig

    def expand(self, ops):
        binding = self.state.scope
        for op in ops:
            assignment = self.state.bind(op, scope=binding)
            if not isinstance(assignment, AssignmentBinding):
                raise BindError("an assignment expected", op.mark)
            name, is_reference = assignment.terms[0]
            arity = None
            if is_reference:
                body = self.state.bind(assignment.body, scope=binding)
                recipe = BindingRecipe(body)
            else:
                if (len(assignment.terms) == 1 and
                        assignment.parameters is not None):
                    arity = len(assignment.parameters)
                recipe = SubstitutionRecipe(binding, assignment.terms[1:],
                                            assignment.parameters,
                                            assignment.body)
            recipe = ClosedRecipe(recipe)
            binding = DefinitionBinding(binding, name, is_reference, arity,
                                        recipe, self.syntax)
        return binding


class BindWhere(BindMacro):

    named('where')
    signature = WhereSig

    def expand(self, lop, rops):
        binding = self.state.scope
        for op in rops:
            assignment = self.state.bind(op, scope=binding)
            if not isinstance(assignment, AssignmentBinding):
                raise BindError("an assignment expected", op.mark)
            name, is_reference = assignment.terms[0]
            arity = None
            if is_reference:
                body = self.state.bind(assignment.body, scope=binding)
                recipe = BindingRecipe(body)
            else:
                if (len(assignment.terms) == 1 and
                        assignment.parameters is not None):
                    arity = len(assignment.parameters)
                recipe = SubstitutionRecipe(binding, assignment.terms[1:],
                                            assignment.parameters,
                                            assignment.body)
            recipe = ClosedRecipe(recipe)
            binding = DefinitionBinding(binding, name, is_reference, arity,
                                        recipe, self.syntax)
        return self.state.bind(lop, scope=binding)


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


class BindTimeCast(BindCast):

    named('time')
    codomain = TimeDomain()


class BindDateTimeCast(BindCast):

    named('datetime')
    codomain = DateTimeDomain()


class BindMakeDate(BindMonoFunction):

    named(('date', 3))
    signature = MakeDateSig
    domains = [IntegerDomain(), IntegerDomain(), IntegerDomain()]
    codomain = DateDomain()
    hint = """date(year, month, day) -> date value"""


class BindMakeDateTime(BindMonoFunction):

    named(('datetime', 3),
          ('datetime', 4),
          ('datetime', 5),
          ('datetime', 6))
    signature = MakeDateTimeSig
    domains = [IntegerDomain(), IntegerDomain(), IntegerDomain(),
               IntegerDomain(), IntegerDomain(), FloatDomain()]
    codomain = DateTimeDomain()


class BindCombineDateTime(BindMonoFunction):

    named(('datetime', 2))
    signature = CombineDateTimeSig
    domains = [DateDomain(), TimeDomain()]
    codomain = DateTimeDomain()


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
            return FormulaBinding(self.state.scope,
                                  IsEqualSig(self.polarity),
                                  coerce(BooleanDomain()),
                                  self.syntax, lop=lop, rop=rops[0])
        else:
            return FormulaBinding(self.state.scope,
                                  self.signature(self.polarity),
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
        return FormulaBinding(self.state.scope,
                              IsTotallyEqualSig(self.polarity),
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
        return FormulaBinding(self.state.scope,
                              AndSig(), domain, self.syntax, ops=[lop, rop])


class BindOr(BindFunction):

    named('|')
    signature = BinarySig
    hint = """(p | q) -> TRUE if either p or q is TRUE"""

    def correlate(self, lop, rop):
        domain = coerce(BooleanDomain())
        lop = CastBinding(lop, domain, lop.syntax)
        rop = CastBinding(rop, domain, rop.syntax)
        return FormulaBinding(self.state.scope,
                              OrSig(), domain, self.syntax, ops=[lop, rop])


class BindNot(BindFunction):

    named('!_')
    signature = NotSig
    hint = """(! p) -> TRUE if p is FALSE"""

    def correlate(self, op):
        domain = coerce(BooleanDomain())
        op = CastBinding(op, domain, op.syntax)
        return FormulaBinding(self.state.scope,
                              self.signature(), domain, self.syntax, op=op)


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
        return FormulaBinding(self.state.scope,
                              self.signature(self.relation),
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
                StringDomain, EnumDomain, DateDomain, TimeDomain,
                DateTimeDomain)

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


class CorrelateDateTimeIncrement(CorrelateFunction):

    correlates(AddSig, (DateTimeDomain, IntegerDomain),
                       (DateTimeDomain, DecimalDomain),
                       (DateTimeDomain, FloatDomain))
    signature = DateTimeIncrementSig
    domains = [DateTimeDomain(), FloatDomain()]
    codomain = DateTimeDomain()


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


class CorrelateDateTimeDecrement(CorrelateFunction):

    correlates(SubtractSig, (DateTimeDomain, IntegerDomain),
                            (DateTimeDomain, DecimalDomain),
                            (DateTimeDomain, FloatDomain))
    signature = DateTimeDecrementSig
    domains = [DateTimeDomain(), FloatDomain()]
    codomain = DateTimeDomain()


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


class BindTrunc(BindPolyFunction):

    named('trunc')
    signature = TruncSig


class CorrelateDecimalTrunc(CorrelateFunction):

    correlates(TruncSig, IntegerDomain,
                         DecimalDomain)
    signature = TruncSig
    domains = [DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatTrunc(CorrelateFunction):

    correlates(TruncSig, FloatDomain)
    signature = TruncSig
    domains = [FloatDomain()]
    codomain = FloatDomain()


class BindTruncTo(BindPolyFunction):

    named(('trunc', 2))
    signature = TruncToSig


class CorrelateDecimalTruncTo(CorrelateFunction):

    correlates(TruncToSig, (IntegerDomain, IntegerDomain),
                           (DecimalDomain, IntegerDomain))
    signature = TruncToSig
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
        binding = FormulaBinding(self.state.scope,
                                 self.signature(self.polarity),
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
        binding = FormulaBinding(self.state.scope,
                                 self.signature(), UntypedDomain(),
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
        binding = FormulaBinding(self.state.scope,
                                 self.signature(), UntypedDomain(),
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
        binding = FormulaBinding(self.state.scope,
                                 self.signature(), UntypedDomain(),
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
        binding = FormulaBinding(self.state.scope,
                                 signature, UntypedDomain(), self.syntax, op=op)
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


class BindNow(BindMonoFunction):

    named('now')
    signature = NowSig
    codomain = DateTimeDomain()


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


class BindExtractHour(BindPolyFunction):

    named('hour')
    signature = ExtractHourSig


class BindExtractMinute(BindPolyFunction):

    named('minute')
    signature = ExtractMinuteSig


class BindExtractSecond(BindPolyFunction):

    named('second')
    signature = ExtractSecondSig


class CorrelateExtractYearFromDate(CorrelateFunction):

    correlates(ExtractYearSig, DateDomain)
    domains = [DateDomain()]
    codomain = IntegerDomain()


class CorrelateExtractYearFromDateTime(CorrelateFunction):

    correlates(ExtractYearSig, DateTimeDomain)
    domains = [DateTimeDomain()]
    codomain = IntegerDomain()


class CorrelateExtractMonthFromDate(CorrelateFunction):

    correlates(ExtractMonthSig, DateDomain)
    domains = [DateDomain()]
    codomain = IntegerDomain()


class CorrelateExtractMonthFromDateTime(CorrelateFunction):

    correlates(ExtractMonthSig, DateTimeDomain)
    domains = [DateTimeDomain()]
    codomain = IntegerDomain()


class CorrelateExtractDayFromDate(CorrelateFunction):

    correlates(ExtractDaySig, DateDomain)
    domains = [DateDomain()]
    codomain = IntegerDomain()


class CorrelateExtractDayFromDateTime(CorrelateFunction):

    correlates(ExtractDaySig, DateTimeDomain)
    domains = [DateTimeDomain()]
    codomain = IntegerDomain()


class CorrelateExtractHourFromTime(CorrelateFunction):

    correlates(ExtractHourSig, TimeDomain)
    domains = [TimeDomain()]
    codomain = IntegerDomain()


class CorrelateExtractHourFromDateTime(CorrelateFunction):

    correlates(ExtractHourSig, DateTimeDomain)
    domains = [DateTimeDomain()]
    codomain = IntegerDomain()


class CorrelateExtractMinuteFromTime(CorrelateFunction):

    correlates(ExtractMinuteSig, TimeDomain)
    domains = [TimeDomain()]
    codomain = IntegerDomain()


class CorrelateExtractMinuteFromDateTime(CorrelateFunction):

    correlates(ExtractMinuteSig, DateTimeDomain)
    domains = [DateTimeDomain()]
    codomain = IntegerDomain()


class CorrelateExtractSecondFromTime(CorrelateFunction):

    correlates(ExtractSecondSig, TimeDomain)
    domains = [TimeDomain()]
    codomain = FloatDomain()


class CorrelateExtractSecondFromDateTime(CorrelateFunction):

    correlates(ExtractSecondSig, DateTimeDomain)
    domains = [DateTimeDomain()]
    codomain = FloatDomain()


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
        return FormulaBinding(self.state.scope,
                              self.signature(), domain, self.syntax,
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
        return FormulaBinding(self.state.scope,
                              self.signature(), domain, self.syntax,
                              variable=variable,
                              variants=variants,
                              consequents=consequents,
                              alternative=alternative)


class BindExistsBase(BindFunction):

    signature = ExistsSig
    polarity = None

    def correlate(self, op):
        recipies = expand(op, is_hard=False)
        plural_base = None
        if recipies is not None:
            if len(recipies) != 1:
                raise BindError("one operand is expected", op.mark)
            plural_base = op
            syntax, recipe = recipies[0]
            bind = BindByRecipe(recipe, syntax, self.state)
            op = bind()
        op = CastBinding(op, coerce(BooleanDomain()), op.syntax)
        return FormulaBinding(self.state.scope,
                              QuantifySig(self.polarity), op.domain,
                              self.syntax, plural_base=plural_base, op=op)


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
        recipies = expand(op, is_hard=False)
        plural_base = None
        if recipies is not None:
            if len(recipies) != 1:
                raise BindError("one operand is expected", op.mark)
            plural_base = op
            syntax, recipe = recipies[0]
            bind = BindByRecipe(recipe, syntax, self.state)
            op = bind()
        op = CastBinding(op, coerce(BooleanDomain()), op.syntax)
        op = FormulaBinding(self.state.scope,
                            CountSig(), coerce(IntegerDomain()),
                            self.syntax, op=op)
        return FormulaBinding(self.state.scope,
                              AggregateSig(), op.domain, self.syntax,
                              plural_base=plural_base, op=op)


class BindPolyAggregate(BindPolyFunction):

    signature = UnarySig
    codomain = UntypedDomain()

    def correlate(self, op):
        recipies = expand(op, is_hard=False)
        plural_base = None
        if recipies is not None:
            if len(recipies) != 1:
                raise BindError("one operand is expected", op.mark)
            plural_base = op
            syntax, recipe = recipies[0]
            bind = BindByRecipe(recipe, syntax, self.state)
            op = bind()
        binding = FormulaBinding(self.state.scope,
                                 self.signature(), self.codomain, self.syntax,
                                 op=op)
        correlate = Correlate(binding, self.state)
        binding = correlate()
        return FormulaBinding(self.state.scope,
                              AggregateSig(), binding.domain, binding.syntax,
                              plural_base=plural_base, op=binding)


class BindMinMaxBase(BindPolyAggregate):

    signature = MinMaxSig
    polarity = None

    def correlate(self, op):
        recipies = expand(op, is_hard=False)
        plural_base = None
        if recipies is not None:
            if len(recipies) != 1:
                raise BindError("one operand is expected", op.mark)
            plural_base = op
            syntax, recipe = recipies[0]
            bind = BindByRecipe(recipe, syntax, self.state)
            op = bind()
        binding = FormulaBinding(self.state.scope,
                                 self.signature(self.polarity), self.codomain,
                                 self.syntax, op=op)
        correlate = Correlate(binding, self.state)
        binding = correlate()
        return FormulaBinding(self.state.scope,
                              AggregateSig(), binding.domain, binding.syntax,
                              plural_base=plural_base, op=binding)


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


class CorrelateIntegerMinMax(CorrelateFunction):

    correlates(MinMaxSig, IntegerDomain)
    signature = MinMaxSig
    domains = [IntegerDomain()]
    codomain = IntegerDomain()


class CorrelateDecimalMinMax(CorrelateFunction):

    correlates(MinMaxSig, DecimalDomain)
    signature = MinMaxSig
    domains = [DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatMinMax(CorrelateFunction):

    correlates(MinMaxSig, FloatDomain)
    signature = MinMaxSig
    domains = [FloatDomain()]
    codomain = FloatDomain()


class CorrelateStringMinMax(CorrelateFunction):

    correlates(MinMaxSig, StringDomain)
    signature = MinMaxSig
    domains = [StringDomain()]
    codomain = StringDomain()


class CorrelateDateMinMax(CorrelateFunction):

    correlates(MinMaxSig, DateDomain)
    signature = MinMaxSig
    domains = [DateDomain()]
    codomain = DateDomain()


class CorrelateTimeMinMax(CorrelateFunction):

    correlates(MinMaxSig, TimeDomain)
    signature = MinMaxSig
    domains = [TimeDomain()]
    codomain = TimeDomain()


class CorrelateDateTimeMinMax(CorrelateFunction):

    correlates(MinMaxSig, DateTimeDomain)
    signature = MinMaxSig
    domains = [DateTimeDomain()]
    codomain = DateTimeDomain()


class BindSum(BindPolyAggregate):

    named('sum')
    signature = SumSig
    hint = """base.sum(x) -> the sum of x"""


class CorrelateIntegerSum(CorrelateFunction):

    correlates(SumSig, IntegerDomain)
    signature = SumSig
    domains = [IntegerDomain()]
    codomain = IntegerDomain()


class CorrelateDecimalSum(CorrelateFunction):

    correlates(SumSig, DecimalDomain)
    signature = SumSig
    domains = [DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatSum(CorrelateFunction):

    correlates(SumSig, FloatDomain)
    signature = SumSig
    domains = [FloatDomain()]
    codomain = FloatDomain()


class BindAvg(BindPolyAggregate):

    named('avg')
    signature = AvgSig
    hint = """base.avg(x) -> the average value of x"""


class CorrelateDecimalAvg(CorrelateFunction):

    correlates(AvgSig, IntegerDomain,
                       DecimalDomain)
    signature = AvgSig
    domains = [DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatAvg(CorrelateFunction):

    correlates(AvgSig, FloatDomain)
    signature = AvgSig
    domains = [FloatDomain()]
    codomain = FloatDomain()


