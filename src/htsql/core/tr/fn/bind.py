#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql.core.tr.fn.bind`
============================
"""


from ...util import aresubclasses
from ...adapter import Adapter, Component, adapt, adapt_many, call
from ...domain import (Domain, UntypedDomain, BooleanDomain, StringDomain,
                       IntegerDomain, DecimalDomain, FloatDomain,
                       DateDomain, TimeDomain, DateTimeDomain, EnumDomain,
                       ListDomain, RecordDomain)
from ..syntax import (NumberSyntax, StringSyntax, IdentifierSyntax,
                      SpecifierSyntax, ApplicationSyntax, OperatorSyntax,
                      GroupSyntax)
from ..binding import (LiteralBinding, SortBinding, SieveBinding,
                       FormulaBinding, CastBinding, ImplicitCastBinding,
                       WrappingBinding, TitleBinding, DirectionBinding,
                       QuotientBinding, AssignmentBinding, DefinitionBinding,
                       SelectionBinding, HomeBinding, RescopingBinding,
                       CoverBinding, ForkBinding, ClipBinding, CommandBinding,
                       SegmentBinding, QueryBinding, Binding,
                       BindingRecipe, ComplementRecipe, KernelRecipe,
                       SubstitutionRecipe, ClosedRecipe)
from ..bind import BindByName, BindingState
from ..error import BindError
from ..coerce import coerce
from ..decorate import decorate
from ..lookup import direct, expand, identify, guess_tag, lookup_command
from ..signature import (Signature, NullarySig, UnarySig, BinarySig,
                         CompareSig, IsEqualSig, IsTotallyEqualSig, IsInSig,
                         IsNullSig, IfNullSig, NullIfSig, AndSig, OrSig,
                         NotSig, SortDirectionSig)
from .signature import (AsSig, LimitSig, SortSig, CastSig,
                        MakeDateSig, MakeDateTimeSig, CombineDateTimeSig,
                        ExtractYearSig, ExtractMonthSig, ExtractDaySig,
                        ExtractHourSig, ExtractMinuteSig, ExtractSecondSig,
                        AddSig, ConcatenateSig, HeadSig, TailSig, SliceSig,
                        AtSig, ReplaceSig, UpperSig, LowerSig, TrimSig,
                        DateIncrementSig, DateTimeIncrementSig,
                        SubtractSig, DateDecrementSig, DateTimeDecrementSig,
                        DateDifferenceSig, TodaySig, NowSig,
                        MultiplySig, DivideSig, IfSig, SwitchSig,
                        KeepPolaritySig, ReversePolaritySig,
                        RoundSig, RoundToSig, TruncSig, TruncToSig, LengthSig,
                        ContainsSig, ExistsSig, CountSig, MinMaxSig,
                        SumSig, AvgSig, AggregateSig, QuantifySig,
                        DefineSig, WhereSig, SelectSig, LinkSig, TopSig)
from ...cmd.command import RendererCmd, DefaultCmd, RetrieveCmd, SQLCmd
from ...fmt.format import (RawFormat, JSONFormat, CSVFormat, TSVFormat,
                           HTMLFormat, TextFormat, XMLFormat)
import sys


class BindFunction(BindByName):

    signature = None
    hint = None
    help = None

    def match(self):
        assert self.signature is not None
        arguments = {}
        if self.arguments is None:
            operands = []
        else:
            operands = self.arguments[:]
        min_args = len([slot for slot in self.signature.slots
                             if slot.is_mandatory])
        max_args = len(self.signature.slots)
        if self.signature.slots and not self.signature.slots[-1].is_singular:
            max_args = None
        if len(operands) < min_args or (max_args is not None and
                                        len(operands) > max_args):
            if min_args == max_args == 1:
                message = "1 argument"
            elif min_args == max_args:
                message = "%s arguments" % min_args
            elif max_args == 1:
                message = "%s to %s argument" % (min_args, max_args)
            elif max_args is not None:
                message = "%s to %s arguments" % (min_args, max_args)
            else:
                message = "%s or more arguments" % min_args
            raise BindError("function '%s' expects %s; got %s"
                            % (self.name.encode('utf-8'),
                               message, len(operands)),
                            self.syntax.mark)

        for index, slot in enumerate(self.signature.slots):
            name = slot.name
            value = None
            if not operands:
                assert not slot.is_mandatory
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
        assert not operands
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
                    recipes = expand(bound_value, with_syntax=True)
                    if slot.is_mandatory and (recipes is not None and
                                              not recipes):
                        raise BindError("at least one element is expected",
                                        value.mark)
                    if recipes is None:
                        bound_value = [bound_value]
                    else:
                        bound_value = []
                        for syntax, recipe in recipes:
                            bound_value.append(self.state.use(recipe, syntax))
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
                    value = ImplicitCastBinding(value, domain, value.syntax)
            else:
                value = [ImplicitCastBinding(item, domain, item.syntax)
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
            if len(domains) > 1:
                raise BindError("cannot coerce values of types (%s)"
                                " to a common type"
                                % (", ".join("'%s'" % domain.family
                                             for domain in domains)),
                                self.syntax.mark)
            else:
                raise BindError("a scalar value is expected",
                                self.syntax.mark)
        cast_arguments = {}
        for slot in self.signature.slots:
            name = slot.name
            value = arguments[name]
            if slot.is_singular:
                if value is not None:
                    value = ImplicitCastBinding(value, domain, value.syntax)
            else:
                value = [ImplicitCastBinding(item, domain, item.syntax)
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

    __signature__ = None
    __domains__ = []
    __arity__ = 0

    @classmethod
    def __dominates__(component, other):
        if component.__signature__ is None:
            return False
        if other.__signature__ is None:
            return False
        if issubclass(component, other):
            return True
        if (issubclass(component.__signature__, other.__signature__)
            and component.__signature__ is not other.__signature__):
            return True
        return False

    @classmethod
    def __matches__(component, dispatch_key):
        if component.__signature__ is None:
            return False
        key_signature, key_domain_vector = dispatch_key
        if not issubclass(key_signature, component.__signature__):
            return False
        if len(key_domain_vector) < component.__arity__:
            return False
        key_domain_vector = key_domain_vector[:component.__arity__]
        for domain_vector in component.__domains__:
            if aresubclasses(key_domain_vector, domain_vector):
                return True
        return False

    @classmethod
    def __dispatch__(interface, binding, *args, **kwds):
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
        if isinstance(self.binding.syntax, OperatorSyntax):
            name = "operator '%s'" % self.binding.syntax.symbol.encode('utf-8')
        elif isinstance(self.binding.syntax, ApplicationSyntax):
            name = "function '%s'" % self.binding.syntax.name.encode('utf-8')
        else:
            name = "'%s'" % self.binding.syntax
        key_signature, domain_vector = self.__dispatch_key__
        if len(domain_vector) > 1:
            types = "types"
            values = "values"
        else:
            types = "type"
            values = "a value"
        families = ", ".join("'%s'" % domain_class.family
                             for domain_class in domain_vector)
        if len(domain_vector) > 1:
            families = "(%s)" % families
        valid_types = []
        for component in self.__interface__.__implementations__():
            if component.__signature__ is None:
                continue
            if not issubclass(key_signature, component.__signature__):
                continue
            for domain_vector in component.__domains__:
                if any(issubclass(domain_class, UntypedDomain)
                       for domain_class in domain_vector):
                    continue
                valid_families = ", ".join("'%s'" % domain_class.family
                                           for domain_class in domain_vector)
                if len(domain_vector) > 1:
                    valid_families = "(%s)" % valid_families
                if valid_families not in valid_types:
                    valid_types.append(valid_families)
        hint = None
        if valid_types:
            hint = "valid %s: %s" % (types, ", ".join(valid_types))
        raise BindError("%s cannot be applied to %s of %s %s"
                        % (name, values, types, families),
                        self.binding.mark, hint=hint)


def match(signature, *domain_vectors):
    assert issubclass(signature, Signature)
    domain_vectors = [domain_vector if isinstance(domain_vector, tuple)
                                  else (domain_vector,)
                      for domain_vector in domain_vectors]
    assert len(domain_vectors) > 0
    arity = len(domain_vectors[0])
    assert all(len(domain_vector) == arity
               for domain_vector in domain_vectors)
    frame = sys._getframe(1)
    frame.f_locals['__signature__'] = signature
    frame.f_locals['__domains__'] = domain_vectors
    frame.f_locals['__arity__'] = arity


class CorrelateFunction(Correlate):

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
                        value = ImplicitCastBinding(value, domain,
                                                    value.syntax)
                else:
                    value = [ImplicitCastBinding(item, domain, item.syntax)
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
        return Correlate.__invoke__(binding, self.state)


class BindCommand(BindMacro):

    signature = UnarySig


class BindRetrieve(BindCommand):

    call('retrieve')

    def expand(self, op):
        op = self.state.bind(op)
        if not isinstance(op, SegmentBinding):
            raise BindError("function '%s' expects a segment argument"
                            % self.name.encode('utf-8'), op.mark)
        profile = decorate(op)
        binding = QueryBinding(self.state.root, op, profile, op.syntax)
        command = RetrieveCmd(binding)
        return CommandBinding(self.state.scope, command, self.syntax)


class BindFormat(BindCommand):

    format = None

    def expand(self, op):
        op = self.state.bind(op)
        producer = lookup_command(op)
        if producer is None:
            if not isinstance(op, SegmentBinding):
                raise BindError("function '%s' expects a segment argument"
                                % self.name.encode('utf-8'), op.mark)
            profile = decorate(op)
            binding = QueryBinding(self.state.root, op, profile, op.syntax)
            producer = DefaultCmd(binding)
        format = self.format()
        command = RendererCmd(format, producer)
        return CommandBinding(self.state.scope, command, self.syntax)


class BindText(BindFormat):

    call('txt')
    format = TextFormat


class BindHTML(BindFormat):

    call('html')
    format = HTMLFormat


class BindRaw(BindFormat):

    call('raw')
    format = RawFormat


class BindJSON(BindFormat):

    call('json')
    format = JSONFormat


class BindCSV(BindFormat):

    call('csv')
    format = CSVFormat


class BindTSV(BindFormat):

    call('tsv')
    format = TSVFormat


class BindXML(BindFormat):

    call('xml')
    format = XMLFormat


class BindSQL(BindCommand):

    call('sql')

    def expand(self, op):
        op = self.state.bind(op)
        producer = lookup_command(op)
        if producer is None:
            if not isinstance(op, SegmentBinding):
                raise BindError("function '%s' expects a segment argument"
                                % self.name.encode('utf-8'), op.mark)
            profile = decorate(op)
            binding = QueryBinding(self.state.root, op, profile, op.syntax)
            producer = DefaultCmd(binding)
        command = SQLCmd(producer)
        return CommandBinding(self.state.scope, command, self.syntax)


class BindNull(BindMacro):

    call('null', ('null', None))
    signature = NullarySig
    hint = """null() -> NULL"""

    def expand(self):
        return LiteralBinding(self.state.scope,
                              None, UntypedDomain(), self.syntax)


class BindTrue(BindMacro):

    call('true', ('true', None))
    signature = NullarySig
    hint = """true() -> TRUE"""

    def expand(self):
        return LiteralBinding(self.state.scope,
                              True, coerce(BooleanDomain()), self.syntax)


class BindFalse(BindMacro):

    call('false', ('false', None))
    signature = NullarySig
    hint = """false() -> FALSE"""

    def expand(self):
        return LiteralBinding(self.state.scope,
                              False, coerce(BooleanDomain()), self.syntax)


class BindRoot(BindMacro):

    call('root')
    signature = NullarySig
    hint = """base.root() -> the root space"""

    def expand(self):
        return WrappingBinding(self.state.root, self.syntax)


class BindThis(BindMacro):

    call('this')
    signature = NullarySig
    hint = """base.this() -> the current base space"""

    def expand(self):
        return WrappingBinding(self.state.scope, self.syntax)


class BindHome(BindMacro):

    call('home')
    signature = NullarySig

    def expand(self):
        return HomeBinding(self.state.scope, self.syntax)


class BindDistinct(BindMacro):

    call('distinct')
    signature = UnarySig

    def expand(self, op):
        seed = self.state.bind(op)
        recipes = expand(seed, with_syntax=True)
        if recipes is None:
            raise BindError("function '%s' expects an argument with a selector"
                            % self.name.encode('utf-8'), op.mark)
        kernels = []
        for syntax, recipe in recipes:
            element = self.state.use(recipe, syntax, scope=seed)
            element = RescopingBinding(element, seed, element.syntax)
            domain = coerce(element.domain)
            if domain is None:
                raise BindError("quotient column must be scalar", element.mark)
            element = ImplicitCastBinding(element, domain, element.syntax)
            kernels.append(element)
        quotient = QuotientBinding(self.state.scope, seed, kernels,
                                   self.syntax)
        binding = quotient
        name = guess_tag(seed)
        if name is not None:
            recipe = ComplementRecipe(quotient)
            recipe = ClosedRecipe(recipe)
            binding = DefinitionBinding(binding, name, False, None, recipe,
                                        self.syntax)
        for index, kernel in enumerate(kernels):
            name = guess_tag(kernel)
            if name is not None:
                recipe = KernelRecipe(quotient, index)
                recipe = ClosedRecipe(recipe)
                binding = DefinitionBinding(binding, name, False, None, recipe,
                                            self.syntax)
        return binding


class BindAs(BindMacro):

    call('as')
    signature = AsSig
    hint = """as(expr, title) -> expression with a title"""
    help = """
    Decorates an expression with a title.

    `expr`: an arbitrary expression.
    `title`: an identifier or a string literal.
    """

    def expand(self, base, title):
        if not isinstance(title, (StringSyntax, IdentifierSyntax)):
            raise BindError("function '%s' expects a string literal"
                            " or an identifier" % self.name.encode('utf-8'),
                            title.mark)
        base = self.state.bind(base)
        return TitleBinding(base, title.value, self.syntax)


#class BindSieve(BindMacro):
#
#    call('?')
#    signature = BinarySig
#
#    def expand(self, lop, rop):
#        base = self.state.bind(lop)
#        filter = self.state.bind(rop, base)
#        filter = ImplicitCastBinding(filter, coerce(BooleanDomain()),
#                                     filter.syntax)
#        return SieveBinding(base, filter, self.syntax)


class BindFilter(BindMacro):

    call('filter')
    signature = UnarySig

    def expand(self, op):
        filter = self.state.bind(op)
        filter = ImplicitCastBinding(filter, coerce(BooleanDomain()),
                                     filter.syntax)
        return SieveBinding(self.state.scope, filter, self.syntax)


class BindSelect(BindMacro):

    call('select')
    signature = SelectSig

    def expand(self, ops):
        elements = []
        for op in ops:
            element = self.state.bind(op)
            recipes = expand(element, with_syntax=True)
            if recipes is not None:
                for syntax, recipe in recipes:
                    if not isinstance(syntax, (IdentifierSyntax, GroupSyntax)):
                        syntax = GroupSyntax(syntax, syntax.mark)
                    syntax = SpecifierSyntax(element.syntax, syntax,
                                             syntax.mark)
                    elements.append(self.state.use(recipe, syntax))
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
        fields = [decorate(element) for element in elements]
        domain = RecordDomain(fields)
        return SelectionBinding(base, elements, domain, base.syntax)


class BindMoniker(BindMacro):

    call('moniker')
    signature = LinkSig

    def expand(self, seed):
        seed = self.state.bind(seed)
        return CoverBinding(self.state.scope, seed, self.syntax)


class BindFork(BindMacro):

    call('fork')
    signature = SelectSig

    def expand(self, ops):
        elements = []
        for op in ops:
            element = self.state.bind(op)
            recipes = expand(element, with_syntax=True)
            if recipes is not None:
                for syntax, recipe in recipes:
                    if not isinstance(syntax, (IdentifierSyntax, GroupSyntax)):
                        syntax = GroupSyntax(syntax, syntax.mark)
                    syntax = SpecifierSyntax(element.syntax, syntax,
                                             syntax.mark)
                    elements.append(self.state.use(recipe, syntax))
            else:
                elements.append(element)
        return ForkBinding(self.state.scope, elements, self.syntax)


class BindTop(BindMacro):

    call('top')
    signature = TopSig

    def parse(self, argument):
        try:
            if not isinstance(argument, NumberSyntax):
                raise ValueError
            value = int(argument.value)
            if not (value >= 0):
                raise ValueError
            if not isinstance(value, int):
                raise ValueError
        except ValueError:
            raise BindError("function '%s' expects a non-negative integer"
                            % self.name.encode('utf-8'), argument.mark)
        return value

    def expand(self, seed, limit=None, offset=None):
        seed = self.state.bind(seed)
        if limit is not None:
            limit = self.parse(limit)
        if offset is not None:
            offset = self.parse(offset)
        return ClipBinding(self.state.scope, seed, limit, offset, self.syntax)


class BindDirectionBase(BindMacro):

    signature = SortDirectionSig
    direction = None

    def expand(self, base):
        base = self.state.bind(base)
        return DirectionBinding(base, self.direction, self.syntax)


class BindAscDir(BindDirectionBase):

    call('_+')
    direction = +1
    hint = """(expr +) -> sort in ascending order"""
    help = """
    Decorates an expression with a sort order indicator.
    """


class BindDescDir(BindDirectionBase):

    call('_-')
    direction = -1
    hint = """(expr -) -> sort in descending order"""


class BindLimit(BindMacro):

    call('limit')
    signature = LimitSig
    hint = """base.limit(N[, skip]) -> slice of the base space"""

    def parse(self, argument):
        try:
            if not isinstance(argument, NumberSyntax):
                raise ValueError
            value = int(argument.value)
            if not (value >= 0):
                raise ValueError
            if not isinstance(value, int):
                raise ValueError
        except ValueError:
            raise BindError("function '%s' expects a non-negative integer"
                            % self.name.encode('utf-8'), argument.mark)
        return value

    def expand(self, limit, offset=None):
        limit = self.parse(limit)
        if offset is not None:
            offset = self.parse(offset)
        return SortBinding(self.state.scope, [], limit, offset, self.syntax)


class BindSort(BindMacro):

    call('sort')
    signature = SortSig
    hint = """base.sort(expr[, ...]) -> sorted space"""

    def expand(self, order):
        bindings = []
        for item in order:
            binding = self.state.bind(item)
            recipes = expand(binding, with_syntax=True)
            if recipes is None:
                domain = coerce(binding.domain)
                if domain is None:
                    raise BindError("function '%s' expects a scalar"
                                    " expression" % self.name.encode('utf-8'),
                                    binding.mark)
                binding = ImplicitCastBinding(binding, domain, binding.syntax)
                bindings.append(binding)
            else:
                for syntax, recipe in recipes:
                    # FIXME: coerce?
                    binding = self.state.use(recipe, syntax)
                    bindings.append(binding)
        return SortBinding(self.state.scope, bindings, None, None, self.syntax)


class BindDefine(BindMacro):

    call('define')
    signature = DefineSig

    def expand(self, ops):
        binding = self.state.scope
        for op in ops:
            assignment = self.state.bind(op, scope=binding)
            if not isinstance(assignment, AssignmentBinding):
                raise BindError("function '%s' expects an assignment"
                                " expression" % self.name.encode('utf-8'),
                                op.mark)
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

    call('where')
    signature = WhereSig

    def expand(self, lop, rops):
        binding = self.state.scope
        for op in rops:
            assignment = self.state.bind(op, scope=binding)
            if not isinstance(assignment, AssignmentBinding):
                raise BindError("function '%s' expects an assignment"
                                " expression" % self.name.encode('utf-8'),
                                op.mark)
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


class BindId(BindMacro):

    call('id')
    signature = NullarySig

    def expand(self):
        recipe = identify(self.state.scope)
        if recipe is None:
            raise BindError("cannot determine identity", self.syntax.mark)
        return self.state.use(recipe, self.syntax)


class BindCast(BindFunction):

    signature = CastSig
    codomain = None

    def correlate(self, base):
        domain = coerce(self.codomain)
        return CastBinding(base, domain, self.syntax)


class BindBooleanCast(BindCast):

    call('boolean', 'bool')
    codomain = BooleanDomain()
    hint = """boolean(expr) -> expression converted to Boolean"""


class BindStringCast(BindCast):

    call('string', 'str')
    codomain = StringDomain()
    hint = """string(expr) -> expression converted to a string"""


class BindIntegerCast(BindCast):

    call('integer', 'int')
    codomain = IntegerDomain()
    hint = """integer(expr) -> expression converted to integer"""


class BindDecimalCast(BindCast):

    call('decimal', 'dec')
    codomain = DecimalDomain()
    hint = """decimal(expr) -> expression converted to decimal"""


class BindFloatCast(BindCast):

    call('float')
    codomain = FloatDomain()
    hint = """float(expr) -> expression converted to float"""


class BindDateCast(BindCast):

    call('date')
    codomain = DateDomain()
    hint = """date(expr) -> expression converted to date"""


class BindTimeCast(BindCast):

    call('time')
    codomain = TimeDomain()


class BindDateTimeCast(BindCast):

    call('datetime')
    codomain = DateTimeDomain()


class BindMakeDate(BindMonoFunction):

    call(('date', 3))
    signature = MakeDateSig
    domains = [IntegerDomain(), IntegerDomain(), IntegerDomain()]
    codomain = DateDomain()
    hint = """date(year, month, day) -> date value"""


class BindMakeDateTime(BindMonoFunction):

    call(('datetime', 3),
          ('datetime', 4),
          ('datetime', 5),
          ('datetime', 6))
    signature = MakeDateTimeSig
    domains = [IntegerDomain(), IntegerDomain(), IntegerDomain(),
               IntegerDomain(), IntegerDomain(), FloatDomain()]
    codomain = DateTimeDomain()


class BindCombineDateTime(BindMonoFunction):

    call(('datetime', 2))
    signature = CombineDateTimeSig
    domains = [DateDomain(), TimeDomain()]
    codomain = DateTimeDomain()


class BindAmongBase(BindFunction):

    signature = IsInSig
    polarity = None

    def correlate(self, lop, rops):
        domains = [lop.domain] + [rop.domain for rop in rops]
        domain = coerce(*domains)
        if domain is None:
            raise BindError("cannot coerce values of types (%s)"
                            " to a common type"
                            % (", ".join("'%s'" % domain.family
                                         for domain in domains)),
                            self.syntax.mark)
        lop = ImplicitCastBinding(lop, domain, lop.syntax)
        rops = [ImplicitCastBinding(rop, domain, rop.syntax) for rop in rops]
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

    call('=')
    polarity = +1
    hint = """(x = y) -> TRUE if x is equal to y"""


class BindNotAmong(BindAmongBase):

    call('!=')
    polarity = -1
    hint = """(x != y) -> TRUE if x is not equal to y"""


class BindTotallyEqualBase(BindFunction):

    signature = IsTotallyEqualSig
    polarity = None

    def correlate(self, lop, rop):
        domains = [lop.domain, rop.domain]
        domain = coerce(*domains)
        if domain is None:
            raise BindError("cannot coerce values of types (%s)"
                            " to a common type"
                            % (", ".join("'%s'" % domain.family
                                         for domain in domains)),
                            self.syntax.mark)
        lop = ImplicitCastBinding(lop, domain, lop.syntax)
        rop = ImplicitCastBinding(rop, domain, rop.syntax)
        return FormulaBinding(self.state.scope,
                              IsTotallyEqualSig(self.polarity),
                              coerce(BooleanDomain()), self.syntax,
                              lop=lop, rop=rop)


class BindTotallyEqual(BindTotallyEqualBase):

    call('==')
    polarity = +1
    hint = """(x == y) -> TRUE if x is equal to y"""


class BindTotallyNotEqual(BindTotallyEqualBase):

    call('!==')
    polarity = -1
    hint = """(x !== y) -> TRUE if x is not equal to y"""


class BindAnd(BindFunction):

    call('&')
    signature = BinarySig
    hint = """(p & q) -> TRUE if both p and q are TRUE"""

    def correlate(self, lop, rop):
        domain = coerce(BooleanDomain())
        lop = ImplicitCastBinding(lop, domain, lop.syntax)
        rop = ImplicitCastBinding(rop, domain, rop.syntax)
        return FormulaBinding(self.state.scope,
                              AndSig(), domain, self.syntax, ops=[lop, rop])


class BindOr(BindFunction):

    call('|')
    signature = BinarySig
    hint = """(p | q) -> TRUE if either p or q is TRUE"""

    def correlate(self, lop, rop):
        domain = coerce(BooleanDomain())
        lop = ImplicitCastBinding(lop, domain, lop.syntax)
        rop = ImplicitCastBinding(rop, domain, rop.syntax)
        return FormulaBinding(self.state.scope,
                              OrSig(), domain, self.syntax, ops=[lop, rop])


class BindNot(BindFunction):

    call('!_')
    signature = NotSig
    hint = """(! p) -> TRUE if p is FALSE"""

    def correlate(self, op):
        domain = coerce(BooleanDomain())
        op = ImplicitCastBinding(op, domain, op.syntax)
        return FormulaBinding(self.state.scope,
                              self.signature(), domain, self.syntax, op=op)


class BindCompare(BindFunction):

    signature = CompareSig
    relation = None

    def correlate(self, lop, rop):
        domains = [lop.domain, rop.domain]
        domain = coerce(*domains)
        if domain is None:
            raise BindError("cannot coerce values of types (%s)"
                            " to a common type"
                            % (", ".join("'%s'" % domain.family
                                         for domain in domains)),
                            self.syntax.mark)
        lop = ImplicitCastBinding(lop, domain, lop.syntax)
        rop = ImplicitCastBinding(rop, domain, rop.syntax)
        is_comparable = Comparable.__invoke__(domain)
        if not is_comparable:
            raise BindError("values of type '%s' are not comparable"
                            % domain.family, self.syntax.mark)
        return FormulaBinding(self.state.scope,
                              self.signature(self.relation),
                              coerce(BooleanDomain()),
                              self.syntax, lop=lop, rop=rop)


class BindLessThan(BindCompare):

    call('<')
    relation = '<'
    hint = """(x < y) -> TRUE if x is less than y"""


class BindLessThanOrEqual(BindCompare):

    call('<=')
    relation = '<='
    hint = """(x < y) -> TRUE if x is less than or equal to y"""


class BindGreaterThan(BindCompare):

    call('>')
    relation = '>'
    hint = """(x > y) -> TRUE if x is greater than y"""


class BindGreaterThanOrEqual(BindCompare):

    call('>=')
    relation = '>='
    hint = """(x >= y) -> TRUE if x is greater than or equal to y"""


class Comparable(Adapter):

    adapt(Domain)

    def __init__(self, domain):
        self.domain = domain

    def __call__(self):
        return False


class ComparableDomains(Comparable):

    adapt_many(IntegerDomain, DecimalDomain, FloatDomain,
               StringDomain, EnumDomain, DateDomain, TimeDomain,
               DateTimeDomain)

    def __call__(self):
        return True


class BindAdd(BindPolyFunction):

    call('+')
    signature = AddSig
    hint = """(x + y) -> sum of x and y"""


class CorrelateIntegerAdd(CorrelateFunction):

    match(AddSig, (IntegerDomain, IntegerDomain))
    signature = AddSig
    domains = [IntegerDomain(), IntegerDomain()]
    codomain = IntegerDomain()


class CorrelateDecimalAdd(CorrelateFunction):

    match(AddSig, (IntegerDomain, DecimalDomain),
                  (DecimalDomain, IntegerDomain),
                  (DecimalDomain, DecimalDomain))
    signature = AddSig
    domains = [DecimalDomain(), DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatAdd(CorrelateFunction):

    match(AddSig, (IntegerDomain, FloatDomain),
                  (DecimalDomain, FloatDomain),
                  (FloatDomain, IntegerDomain),
                  (FloatDomain, DecimalDomain),
                  (FloatDomain, FloatDomain))
    signature = AddSig
    domains = [FloatDomain(), FloatDomain()]
    codomain = FloatDomain()


class CorrelateDateIncrement(CorrelateFunction):

    match(AddSig, (DateDomain, IntegerDomain))
    signature = DateIncrementSig
    domains = [DateDomain(), IntegerDomain()]
    codomain = DateDomain()


class CorrelateDateTimeIncrement(CorrelateFunction):

    match(AddSig, (DateTimeDomain, IntegerDomain),
                  (DateTimeDomain, DecimalDomain),
                  (DateTimeDomain, FloatDomain))
    signature = DateTimeIncrementSig
    domains = [DateTimeDomain(), FloatDomain()]
    codomain = DateTimeDomain()


class CorrelateConcatenate(CorrelateFunction):

    match(AddSig, (UntypedDomain, UntypedDomain),
                  (UntypedDomain, StringDomain),
                  (StringDomain, UntypedDomain),
                  (StringDomain, StringDomain))
    signature = ConcatenateSig
    domains = [StringDomain(), StringDomain()]
    codomain = StringDomain()


class BindSubtract(BindPolyFunction):

    call('-')
    signature = SubtractSig
    hint = """(x - y) -> difference between x and y"""


class CorrelateIntegerSubtract(CorrelateFunction):

    match(SubtractSig, (IntegerDomain, IntegerDomain))
    signature = SubtractSig
    domains = [IntegerDomain(), IntegerDomain()]
    codomain = IntegerDomain()


class CorrelateDecimalSubtract(CorrelateFunction):

    match(SubtractSig, (IntegerDomain, DecimalDomain),
                       (DecimalDomain, IntegerDomain),
                       (DecimalDomain, DecimalDomain))
    signature = SubtractSig
    domains = [DecimalDomain(), DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatSubtract(CorrelateFunction):

    match(SubtractSig, (IntegerDomain, FloatDomain),
                       (DecimalDomain, FloatDomain),
                       (FloatDomain, IntegerDomain),
                       (FloatDomain, DecimalDomain),
                       (FloatDomain, FloatDomain))
    signature = SubtractSig
    domains = [FloatDomain(), FloatDomain()]
    codomain = FloatDomain()


class CorrelateDateDecrement(CorrelateFunction):

    match(SubtractSig, (DateDomain, IntegerDomain))
    signature = DateDecrementSig
    domains = [DateDomain(), IntegerDomain()]
    codomain = DateDomain()


class CorrelateDateTimeDecrement(CorrelateFunction):

    match(SubtractSig, (DateTimeDomain, IntegerDomain),
                       (DateTimeDomain, DecimalDomain),
                       (DateTimeDomain, FloatDomain))
    signature = DateTimeDecrementSig
    domains = [DateTimeDomain(), FloatDomain()]
    codomain = DateTimeDomain()


class CorrelateDateDifference(CorrelateFunction):

    match(SubtractSig, (DateDomain, DateDomain))
    signature = DateDifferenceSig
    domains = [DateDomain(), DateDomain()]
    codomain = IntegerDomain()


class BindMultiply(BindPolyFunction):

    call('*')
    signature = MultiplySig
    hint = """(x * y) -> product of x and y"""


class CorrelateIntegerMultiply(CorrelateFunction):

    match(MultiplySig, (IntegerDomain, IntegerDomain))
    signature = MultiplySig
    domains = [IntegerDomain(), IntegerDomain()]
    codomain = IntegerDomain()


class CorrelateDecimalMultiply(CorrelateFunction):

    match(MultiplySig, (IntegerDomain, DecimalDomain),
                       (DecimalDomain, IntegerDomain),
                       (DecimalDomain, DecimalDomain))
    signature = MultiplySig
    domains = [DecimalDomain(), DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatMultiply(CorrelateFunction):

    match(MultiplySig, (IntegerDomain, FloatDomain),
                       (DecimalDomain, FloatDomain),
                       (FloatDomain, IntegerDomain),
                       (FloatDomain, DecimalDomain),
                       (FloatDomain, FloatDomain))
    signature = MultiplySig
    domains = [FloatDomain(), FloatDomain()]
    codomain = FloatDomain()


class BindDivide(BindPolyFunction):

    call('/')
    signature = DivideSig
    hint = """(x / y) -> quotient of x divided by y"""


class CorrelateDecimalDivide(CorrelateFunction):

    match(DivideSig, (IntegerDomain, IntegerDomain),
                     (IntegerDomain, DecimalDomain),
                     (DecimalDomain, IntegerDomain),
                     (DecimalDomain, DecimalDomain))
    signature = DivideSig
    domains = [DecimalDomain(), DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatDivide(CorrelateFunction):

    match(DivideSig, (IntegerDomain, FloatDomain),
                     (DecimalDomain, FloatDomain),
                     (FloatDomain, IntegerDomain),
                     (FloatDomain, DecimalDomain),
                     (FloatDomain, FloatDomain))
    signature = DivideSig
    domains = [FloatDomain(), FloatDomain()]
    codomain = FloatDomain()


class BindKeepPolarity(BindPolyFunction):

    call('+_')
    signature = KeepPolaritySig
    hint = """(+ x) -> x"""


class CorrelateIntegerKeepPolarity(CorrelateFunction):

    match(KeepPolaritySig, IntegerDomain)
    signature = KeepPolaritySig
    domains = [IntegerDomain()]
    codomain = IntegerDomain()


class CorrelateDecimalKeepPolarity(CorrelateFunction):

    match(KeepPolaritySig, DecimalDomain)
    signature = KeepPolaritySig
    domains = [DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatKeepPolarity(CorrelateFunction):

    match(KeepPolaritySig, FloatDomain)
    signature = KeepPolaritySig
    domains = [FloatDomain()]
    codomain = FloatDomain()


class BindReversePolarity(BindPolyFunction):

    call('-_')
    signature = ReversePolaritySig
    hint = """(- x) -> negation of x"""


class CorrelateIntegerReversePolarity(CorrelateFunction):

    match(ReversePolaritySig, IntegerDomain)
    signature = ReversePolaritySig
    domains = [IntegerDomain()]
    codomain = IntegerDomain()


class CorrelateDecimalReversePolarity(CorrelateFunction):

    match(ReversePolaritySig, DecimalDomain)
    signature = ReversePolaritySig
    domains = [DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatReversePolarity(CorrelateFunction):

    match(ReversePolaritySig, FloatDomain)
    signature = ReversePolaritySig
    domains = [FloatDomain()]
    codomain = FloatDomain()


class BindRound(BindPolyFunction):

    call('round')
    signature = RoundSig
    hint = """round(x) -> x rounded to zero"""


class CorrelateDecimalRound(CorrelateFunction):

    match(RoundSig, IntegerDomain,
                    DecimalDomain)
    signature = RoundSig
    domains = [DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatRound(CorrelateFunction):

    match(RoundSig, FloatDomain)
    signature = RoundSig
    domains = [FloatDomain()]
    codomain = FloatDomain()


class BindRoundTo(BindPolyFunction):

    call(('round', 2))
    signature = RoundToSig
    hint = """round(x, n) -> x rounded to a given precision"""


class CorrelateDecimalRoundTo(CorrelateFunction):

    match(RoundToSig, (IntegerDomain, IntegerDomain),
                      (DecimalDomain, IntegerDomain))
    signature = RoundToSig
    domains = [DecimalDomain(), IntegerDomain()]
    codomain = DecimalDomain()


class BindTrunc(BindPolyFunction):

    call('trunc')
    signature = TruncSig


class CorrelateDecimalTrunc(CorrelateFunction):

    match(TruncSig, IntegerDomain,
                    DecimalDomain)
    signature = TruncSig
    domains = [DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatTrunc(CorrelateFunction):

    match(TruncSig, FloatDomain)
    signature = TruncSig
    domains = [FloatDomain()]
    codomain = FloatDomain()


class BindTruncTo(BindPolyFunction):

    call(('trunc', 2))
    signature = TruncToSig


class CorrelateDecimalTruncTo(CorrelateFunction):

    match(TruncToSig, (IntegerDomain, IntegerDomain),
                      (DecimalDomain, IntegerDomain))
    signature = TruncToSig
    domains = [DecimalDomain(), IntegerDomain()]
    codomain = DecimalDomain()


class BindLength(BindPolyFunction):

    call('length')
    signature = LengthSig
    hint = """length(s) -> length of s"""


class CorrelateStringLength(CorrelateFunction):

    match(LengthSig, StringDomain,
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
        return Correlate.__invoke__(binding, self.state)


class BindContains(BindContainsBase):

    call('~')
    polarity = +1
    hint = """(s ~ sub) -> TRUE if s contains sub"""


class BindNotContains(BindContainsBase):

    call('!~')
    polarity = -1
    hint = """(s !~ sub) -> TRUE if s does not contain sub"""


class CorrelateStringContains(CorrelateFunction):

    match(ContainsSig, (StringDomain, StringDomain),
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
            length = ImplicitCastBinding(length, coerce(IntegerDomain()),
                                         length.syntax)
        binding = FormulaBinding(self.state.scope,
                                 self.signature(), UntypedDomain(),
                                 self.syntax, op=op, length=length)
        return Correlate.__invoke__(binding, self.state)


class BindHead(BindPolyFunction):

    call('head')
    signature = HeadSig
    hint = """head(s[, N=1]) -> the first N elements of s"""


class BindTail(BindPolyFunction):

    call('tail')
    signature = TailSig
    hint = """tail(s[, N=1]) -> the last N elements of s"""


class CorrelateHead(CorrelateFunction):

    match(HeadSig, UntypedDomain,
                   StringDomain)
    domains = [StringDomain()]
    codomain = StringDomain()


class CorrelateTail(CorrelateFunction):

    match(TailSig, UntypedDomain,
                   StringDomain)
    domains = [StringDomain()]
    codomain = StringDomain()


class BindSlice(BindPolyFunction):

    call('slice')
    signature = SliceSig
    hint = """slice(s, i, j) -> slice of s from i-th to j-th elements"""

    def correlate(self, op, left, right):
        if left is not None:
            left = ImplicitCastBinding(left, coerce(IntegerDomain()),
                                       left.syntax)
        if right is not None:
            right = ImplicitCastBinding(right, coerce(IntegerDomain()),
                                        right.syntax)
        binding = FormulaBinding(self.state.scope,
                                 self.signature(), UntypedDomain(),
                                 self.syntax, op=op, left=left, right=right)
        return Correlate.__invoke__(binding, self.state)


class CorrelateSlice(CorrelateFunction):

    match(SliceSig, UntypedDomain,
                    StringDomain)
    domains = [StringDomain()]
    codomain = StringDomain()


class BindAt(BindPolyFunction):

    call('at')
    signature = AtSig
    hint = """at(s, i[, len=1]) -> i-th to (i+len)-th elements of s"""

    def correlate(self, op, index, length):
        index = ImplicitCastBinding(index, coerce(IntegerDomain()),
                                    index.syntax)
        if length is not None:
            length = ImplicitCastBinding(length, coerce(IntegerDomain()),
                                         length.syntax)
        binding = FormulaBinding(self.state.scope,
                                 self.signature(), UntypedDomain(),
                                 self.syntax, op=op, index=index, length=length)
        return Correlate.__invoke__(binding, self.state)


class CorrelateAt(CorrelateFunction):

    match(AtSig, UntypedDomain,
                 StringDomain)
    domains = [StringDomain()]
    codomain = StringDomain()


class BindReplace(BindPolyFunction):

    call('replace')
    signature = ReplaceSig
    hint = """replace(s, o, n) -> s with occurences of o replaced by n"""


class CorrelateReplace(CorrelateFunction):

    match(ReplaceSig, UntypedDomain,
                      StringDomain)
    domains = [StringDomain(), StringDomain(), StringDomain()]
    codomain = StringDomain()


class BindUpper(BindPolyFunction):

    call('upper')
    signature = UpperSig
    hint = """upper(s) -> s converted to uppercase"""


class CorrelateUpper(CorrelateFunction):

    match(UpperSig, UntypedDomain,
                    StringDomain)
    domains = [StringDomain()]
    codomain = StringDomain()


class BindLower(BindPolyFunction):

    call('lower')
    signature = LowerSig
    hint = """lower(s) -> s converted to lowercase"""


class CorrelateLower(CorrelateFunction):

    match(LowerSig, UntypedDomain,
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
        return Correlate.__invoke__(binding, self.state)


class BindTrim(BindTrimBase):

    call('trim')
    is_left = True
    is_right = True
    hint = """trim(s) -> s with leading and trailing whitespaces removed"""


class BindLTrim(BindTrimBase):

    call('ltrim')
    is_left = True
    hint = """ltrim(s) -> s with leading whitespaces removed"""


class BindRTrim(BindTrimBase):

    call('rtrim')
    is_right = True
    hint = """rtrim(s) -> s with trailing whitespaces removed"""


class BindToday(BindMonoFunction):

    call('today')
    signature = TodaySig
    codomain = DateDomain()
    hint = """today() -> the current date"""


class BindNow(BindMonoFunction):

    call('now')
    signature = NowSig
    codomain = DateTimeDomain()


class BindExtractYear(BindPolyFunction):

    call('year')
    signature = ExtractYearSig
    hint = """year(date) -> the year of a given date"""


class BindExtractMonth(BindPolyFunction):

    call('month')
    signature = ExtractMonthSig
    hint = """month(date) -> the month of a given date"""


class BindExtractDay(BindPolyFunction):

    call('day')
    signature = ExtractDaySig
    hint = """day(date) -> the day of a given date"""


class BindExtractHour(BindPolyFunction):

    call('hour')
    signature = ExtractHourSig


class BindExtractMinute(BindPolyFunction):

    call('minute')
    signature = ExtractMinuteSig


class BindExtractSecond(BindPolyFunction):

    call('second')
    signature = ExtractSecondSig


class CorrelateExtractYearFromDate(CorrelateFunction):

    match(ExtractYearSig, DateDomain)
    domains = [DateDomain()]
    codomain = IntegerDomain()


class CorrelateExtractYearFromDateTime(CorrelateFunction):

    match(ExtractYearSig, DateTimeDomain)
    domains = [DateTimeDomain()]
    codomain = IntegerDomain()


class CorrelateExtractMonthFromDate(CorrelateFunction):

    match(ExtractMonthSig, DateDomain)
    domains = [DateDomain()]
    codomain = IntegerDomain()


class CorrelateExtractMonthFromDateTime(CorrelateFunction):

    match(ExtractMonthSig, DateTimeDomain)
    domains = [DateTimeDomain()]
    codomain = IntegerDomain()


class CorrelateExtractDayFromDate(CorrelateFunction):

    match(ExtractDaySig, DateDomain)
    domains = [DateDomain()]
    codomain = IntegerDomain()


class CorrelateExtractDayFromDateTime(CorrelateFunction):

    match(ExtractDaySig, DateTimeDomain)
    domains = [DateTimeDomain()]
    codomain = IntegerDomain()


class CorrelateExtractHourFromTime(CorrelateFunction):

    match(ExtractHourSig, TimeDomain)
    domains = [TimeDomain()]
    codomain = IntegerDomain()


class CorrelateExtractHourFromDateTime(CorrelateFunction):

    match(ExtractHourSig, DateTimeDomain)
    domains = [DateTimeDomain()]
    codomain = IntegerDomain()


class CorrelateExtractMinuteFromTime(CorrelateFunction):

    match(ExtractMinuteSig, TimeDomain)
    domains = [TimeDomain()]
    codomain = IntegerDomain()


class CorrelateExtractMinuteFromDateTime(CorrelateFunction):

    match(ExtractMinuteSig, DateTimeDomain)
    domains = [DateTimeDomain()]
    codomain = IntegerDomain()


class CorrelateExtractSecondFromTime(CorrelateFunction):

    match(ExtractSecondSig, TimeDomain)
    domains = [TimeDomain()]
    codomain = FloatDomain()


class CorrelateExtractSecondFromDateTime(CorrelateFunction):

    match(ExtractSecondSig, DateTimeDomain)
    domains = [DateTimeDomain()]
    codomain = FloatDomain()


class CorrelateTrim(CorrelateFunction):

    match(TrimSig, UntypedDomain,
                   StringDomain)
    domains = [StringDomain()]
    codomain = StringDomain()


class BindIsNull(BindHomoFunction):

    call('is_null')
    signature = IsNullSig(+1)
    codomain = BooleanDomain()
    hint = """is_null(x) -> TRUE if x is NULL"""


class BindNullIf(BindHomoFunction):

    call('null_if')
    signature = NullIfSig()
    hint = """null_if(x, y) -> NULL if x is equal to y; x otherwise"""


class BindIfNull(BindHomoFunction):

    call('if_null')
    signature = IfNullSig()
    hint = """if_null(x, y) -> y if x is NULL; x otherwise"""


class BindIf(BindFunction):

    call('if')
    signature = IfSig
    hint = """if(p, c[, ...][, a=NULL]) -> c if p; a otherwise"""

    def match(self):
        operands = list(reversed(self.syntax.arguments))
        if len(operands) < 2:
            raise BindError("function '%s' expects 2 or more arguments;"
                            " got %s" % (self.name.encode('utf-8'),
                                         len(operands)), self.syntax.mark)
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
        predicates = [ImplicitCastBinding(predicate, coerce(BooleanDomain()),
                                          predicate.syntax)
                      for predicate in predicates]
        domains = [consequent.domain for consequent in consequents]
        if alternative is not None:
            domains.append(alternative.domain)
        domain = coerce(*domains)
        if domain is None:
            if len(domains) > 1:
                raise BindError("cannot coerce values of types (%s)"
                                " to a common type"
                                % (", ".join("'%s'" % domain.family
                                             for domain in domains)),
                                self.syntax.mark)
            else:
                raise BindError("a scalar value is expected",
                                consequents[0].mark
                                if consequents else alternative.mark)
        consequents = [ImplicitCastBinding(consequent, domain,
                                           consequent.syntax)
                       for consequent in consequents]
        if alternative is not None:
            alternative = ImplicitCastBinding(alternative, domain,
                                              alternative.syntax)
        return FormulaBinding(self.state.scope,
                              self.signature(), domain, self.syntax,
                              predicates=predicates,
                              consequents=consequents,
                              alternative=alternative)


class BindSwitch(BindFunction):

    call('switch')
    signature = SwitchSig
    hint = """switch(x, v, c, [...][, a=NULL]) -> c if x = v; a otherwise"""

    def match(self):
        operands = list(reversed(self.syntax.arguments))
        if len(operands) < 3:
            raise BindError("function '%s' expects 3 or more arguments;"
                            " got %s" % (self.name.encode('utf-8'),
                                         len(operands)), self.syntax.mark)
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
            raise BindError("cannot coerce values of types (%s)"
                            " to a common type"
                            % (", ".join("'%s'" % domain.family
                                         for domain in domains)),
                            self.syntax.mark)
        variable = ImplicitCastBinding(variable, domain, variable.syntax)
        variants = [ImplicitCastBinding(variant, domain, variant.syntax)
                    for variant in variants]
        domains = [consequent.domain for consequent in consequents]
        if alternative is not None:
            domains.append(alternative.domain)
        domain = coerce(*domains)
        if domain is None:
            if len(domains) > 1:
                raise BindError("cannot coerce values of types (%s)"
                                " to a common type"
                                % (", ".join("'%s'" % domain.family
                                             for domain in domains)),
                                self.syntax.mark)
            else:
                raise BindError("a scalar value is expected",
                                consequents[0].mark
                                if consequents else alternative.mark)
        consequents = [ImplicitCastBinding(consequent, domain,
                                           consequent.syntax)
                       for consequent in consequents]
        if alternative is not None:
            alternative = ImplicitCastBinding(alternative, domain,
                                              alternative.syntax)
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
        recipes = expand(op, with_syntax=True)
        plural_base = None
        if recipes is not None:
            if len(recipes) != 1:
                raise BindError("function '%s' expects 1 argument; got %s"
                                % (self.name.encode('utf-8'), len(recipes)),
                                op.mark)
            plural_base = op
            syntax, recipe = recipes[0]
            op = self.state.use(recipe, syntax)
        op = ImplicitCastBinding(op, coerce(BooleanDomain()), op.syntax)
        return FormulaBinding(self.state.scope,
                              QuantifySig(self.polarity), op.domain,
                              self.syntax, plural_base=plural_base, op=op)


class BindExists(BindExistsBase):

    call('exists')
    polarity = +1
    hint = """base.exists(p) -> TRUE if there exists p such that p = TRUE"""


class BindEvery(BindExistsBase):

    call('every')
    polarity = -1
    hint = """base.every(p) -> TRUE if p = TRUE for every p"""


class BindCount(BindFunction):

    call('count')
    signature = CountSig
    hint = """base.count(p) -> the number of p such that p = TRUE"""

    def correlate(self, op):
        recipes = expand(op, with_syntax=True)
        plural_base = None
        if recipes is not None:
            if len(recipes) != 1:
                raise BindError("function '%s' expects 1 argument; got %s"
                                % (self.name.encode('utf-8'), len(recipes)),
                                op.mark)
            plural_base = op
            syntax, recipe = recipes[0]
            op = self.state.use(recipe, syntax)
        op = ImplicitCastBinding(op, coerce(BooleanDomain()), op.syntax)
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
        recipes = expand(op, with_syntax=True)
        plural_base = None
        if recipes is not None:
            if len(recipes) != 1:
                raise BindError("function '%s' expects 1 argument; got %s"
                                % (self.name.encode('utf-8'), len(recipes)),
                                op.mark)
            plural_base = op
            syntax, recipe = recipes[0]
            op = self.state.use(recipe, syntax)
        binding = FormulaBinding(self.state.scope,
                                 self.signature(), self.codomain, self.syntax,
                                 op=op)
        binding = Correlate.__invoke__(binding, self.state)
        return FormulaBinding(self.state.scope,
                              AggregateSig(), binding.domain, binding.syntax,
                              plural_base=plural_base, op=binding)


class BindMinMaxBase(BindPolyAggregate):

    signature = MinMaxSig
    polarity = None

    def correlate(self, op):
        recipes = expand(op, with_syntax=True)
        plural_base = None
        if recipes is not None:
            if len(recipes) != 1:
                raise BindError("function '%s' expects 1 argument; got %s"
                                % (self.name.encode('utf-8'), len(recipes)),
                                op.mark)
            plural_base = op
            syntax, recipe = recipes[0]
            op = self.state.use(recipe, syntax)
        binding = FormulaBinding(self.state.scope,
                                 self.signature(self.polarity), self.codomain,
                                 self.syntax, op=op)
        binding = Correlate.__invoke__(binding, self.state)
        return FormulaBinding(self.state.scope,
                              AggregateSig(), binding.domain, binding.syntax,
                              plural_base=plural_base, op=binding)


class BindMinMaxBase(BindMinMaxBase):

    call('min')
    signature = MinMaxSig
    polarity = +1
    hint = """base.min(x) -> the minimal value in the set of x"""


class BindMinMaxBase(BindMinMaxBase):

    call('max')
    signature = MinMaxSig
    polarity = -1
    hint = """base.avg(x) -> the maximal value in the set of x"""


class CorrelateIntegerMinMax(CorrelateFunction):

    match(MinMaxSig, IntegerDomain)
    signature = MinMaxSig
    domains = [IntegerDomain()]
    codomain = IntegerDomain()


class CorrelateDecimalMinMax(CorrelateFunction):

    match(MinMaxSig, DecimalDomain)
    signature = MinMaxSig
    domains = [DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatMinMax(CorrelateFunction):

    match(MinMaxSig, FloatDomain)
    signature = MinMaxSig
    domains = [FloatDomain()]
    codomain = FloatDomain()


class CorrelateStringMinMax(CorrelateFunction):

    match(MinMaxSig, StringDomain)
    signature = MinMaxSig
    domains = [StringDomain()]
    codomain = StringDomain()


class CorrelateDateMinMax(CorrelateFunction):

    match(MinMaxSig, DateDomain)
    signature = MinMaxSig
    domains = [DateDomain()]
    codomain = DateDomain()


class CorrelateTimeMinMax(CorrelateFunction):

    match(MinMaxSig, TimeDomain)
    signature = MinMaxSig
    domains = [TimeDomain()]
    codomain = TimeDomain()


class CorrelateDateTimeMinMax(CorrelateFunction):

    match(MinMaxSig, DateTimeDomain)
    signature = MinMaxSig
    domains = [DateTimeDomain()]
    codomain = DateTimeDomain()


class BindSum(BindPolyAggregate):

    call('sum')
    signature = SumSig
    hint = """base.sum(x) -> the sum of x"""


class CorrelateIntegerSum(CorrelateFunction):

    match(SumSig, IntegerDomain)
    signature = SumSig
    domains = [IntegerDomain()]
    codomain = IntegerDomain()


class CorrelateDecimalSum(CorrelateFunction):

    match(SumSig, DecimalDomain)
    signature = SumSig
    domains = [DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatSum(CorrelateFunction):

    match(SumSig, FloatDomain)
    signature = SumSig
    domains = [FloatDomain()]
    codomain = FloatDomain()


class BindAvg(BindPolyAggregate):

    call('avg')
    signature = AvgSig
    hint = """base.avg(x) -> the average value of x"""


class CorrelateDecimalAvg(CorrelateFunction):

    match(AvgSig, IntegerDomain,
                  DecimalDomain)
    signature = AvgSig
    domains = [DecimalDomain()]
    codomain = DecimalDomain()


class CorrelateFloatAvg(CorrelateFunction):

    match(AvgSig, FloatDomain)
    signature = AvgSig
    domains = [FloatDomain()]
    codomain = FloatDomain()


