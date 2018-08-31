#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ..adapter import Adapter, adapt, adapt_many
from ..domain import (Domain, UntypedDomain, EntityDomain, RecordDomain,
        IdentityDomain, BooleanDomain, NumberDomain, IntegerDomain,
        DecimalDomain, FloatDomain, TextDomain, EnumDomain, DateDomain,
        TimeDomain, DateTimeDomain, OpaqueDomain)
from ..error import Error, translate_guard
from .coerce import coerce
from .flow import (Flow, CollectFlow, SelectionFlow, HomeFlow,
        RootFlow, TableFlow, ChainFlow, ColumnFlow, QuotientFlow, KernelFlow,
        ComplementFlow, IdentityFlow, LocateFlow, CoverFlow, ForkFlow,
        AttachFlow, ClipFlow, SieveFlow, SortFlow, CastFlow, RescopingFlow,
        LiteralFlow, FormulaFlow)
from .lookup import direct
from .space import (RootSpace, ScalarSpace, DirectTableSpace, FiberTableSpace,
        QuotientSpace, ComplementSpace, MonikerSpace, LocatorSpace,
        ForkedSpace, AttachSpace, ClippedSpace, FilteredSpace, OrderedSpace,
        SegmentExpr, LiteralCode, FormulaCode, CastCode, ColumnUnit,
        ScalarUnit, KernelUnit, CoveringUnit)
from .signature import Signature, IsNullSig, NullIfSig, IsEqualSig, AndSig
import decimal


class EncodingState:

    def __init__(self):
        self.flow_to_code = {}
        self.flow_to_space = {}
        self.flow_to_bundle = {}

    def encode(self, flow):
        # When caching is enabled, we check if `flow` was
        # already encoded.  If not, we encode it and save the
        # result.
        with translate_guard(flow):
            if flow not in self.flow_to_code:
                code = Encode.__prepare__(flow, self)()
                self.flow_to_code[flow] = code
            return self.flow_to_code[flow]

    def relate(self, flow):
        with translate_guard(flow):
            if flow not in self.flow_to_space:
                space = Relate.__prepare__(flow, self)()
                self.flow_to_space[flow] = space
            return self.flow_to_space[flow]

    def unpack(self, flow):
        with translate_guard(flow):
            if flow not in self.flow_to_bundle:
                bundle = Unpack.__prepare__(flow, self)()
                self.flow_to_bundle[flow] = bundle
            return self.flow_to_bundle[flow]


class Bundle:

    def __init__(self, codes, segments):
        self.codes = codes
        self.segments = segments


class EncodeBase(Adapter):
    """
    Applies an encoding adapter to a flow node.

    This is a base class for the two encoding adapters: :class:`Encode`
    and :class:`Relate`; it encapsulates methods and attributes shared
    between these adapters.

    The encoding process translates flow nodes to data spaces or
    expressions over data spaces.

    `flow` (:class:`htsql.core.tr.flow.Flow`)
        The flow node to encode.

    `state` (:class:`EncodingState`)
        The current state of the encoding process.
    """

    adapt(Flow)

    def __init__(self, flow, state):
        assert isinstance(flow, Flow)
        assert isinstance(state, EncodingState)
        self.flow = flow
        self.state = state


class Encode(EncodeBase):
    """
    Translates a flow node to a code expression node.

    This is an interface adapter; see subclasses for implementations.

    The :class:`Encode` adapter has the following signature::

        Encode: (Flow, EncodingState) -> Expression

    The adapter is polymorphic on the `Flow` argument.

    This adapter provides non-trivial implementation for flow
    nodes representing HTSQL functions and operators.
    """

    def __call__(self):
        # The default implementation generates an error.
        # FIXME: a better error message?
        raise Error("Expected a code expression")


class Relate(EncodeBase):
    """
    Translates a flow node to a data space node.

    This is an interface adapter; see subclasses for implementations.

    The :class:`Relate` adapter has the following signature::

        Relate: (Flow, EncodingState) -> Space

    The adapter is polymorphic on the `Flow` argument.

    The adapter provides non-trivial implementations for scoping
    and chaining flows.
    """

    def __call__(self):
        # The default implementation generates an error.
        # FIXME: a better error message?
        #raise Error("Expected a flow expression")
        return self.state.relate(self.flow.base)


class Unpack(EncodeBase):

    def __call__(self):
        code = self.state.encode(self.flow)
        return Bundle([code], [])


class UnpackCollect(Unpack):

    adapt(CollectFlow)

    def __call__(self):
        root = self.state.relate(self.flow.base)
        if coerce(self.flow.seed.domain) is not None:
            bundle = None
            code = self.state.encode(self.flow.seed)
            units = code.units
            space = None
        elif isinstance(self.flow.seed.domain,
                (RecordDomain, IdentityDomain)):
            bundle = self.state.unpack(self.flow.seed)
            space = self.state.relate(self.flow.seed)
            units = None
        else:
            bundle = self.state.unpack(self.flow.seed)
            units = [unit for code in bundle.codes
                          for unit in code.units]
            space = None
        if space is None:
            if not units:
                space = RootSpace(None, self.flow)
            else:
                spaces = []
                for unit in units:
                    if any(space.dominates(unit.space) for space in spaces):
                        continue
                    spaces = [space for space in spaces
                                  if not unit.space.dominates(space)]
                    spaces.append(unit.space)
                if len(spaces) > 1:
                    raise Error("Cannot deduce an unambiguous segment flow")
                else:
                    [space] = spaces
        if not space.spans(root):
            raise Error("Expected a descendant segment flow")
        if bundle is None:
            if (isinstance(code, LiteralCode) and
                    isinstance(code.domain, UntypedDomain)):
                if code.value is None:
                    filter = LiteralCode(False, coerce(BooleanDomain()),
                                         code.flow)
                    space = FilteredSpace(space, filter, space.flow)
            else:
                filter = FormulaCode(IsNullSig(-1), coerce(BooleanDomain()),
                                     code.flow, op=code)
                space = FilteredSpace(space, filter, space.flow)
        if bundle is not None:
            codes = bundle.codes
            dependents = bundle.segments
        else:
            codes = [code]
            dependents = []
        segment = SegmentExpr(root, space, codes, dependents, self.flow)
        return Bundle([], [segment])


class RelateRoot(Relate):

    adapt(RootFlow)

    def __call__(self):
        # The root flow gives rise to a root space.
        return RootSpace(None, self.flow)


class RelateHome(Relate):

    adapt(HomeFlow)

    def __call__(self):
        # Generate the parent space.
        base = self.state.relate(self.flow.base)
        # A home flow gives rise to a scalar space.
        return ScalarSpace(base, self.flow)


class RelateTable(Relate):

    adapt(TableFlow)

    def __call__(self):
        # Generate the parent space.
        base = self.state.relate(self.flow.base)
        # Produce a link from a scalar to a table class.
        return DirectTableSpace(base, self.flow.table, self.flow)


class RelateChain(Relate):

    adapt(ChainFlow)

    def __call__(self):
        # Generate the parent space.
        space = self.state.relate(self.flow.base)
        # Produce a link between table classes.
        for join in self.flow.joins:
            space = FiberTableSpace(space, join, self.flow)
        return space


class RelateSieve(Relate):

    adapt(SieveFlow)

    def __call__(self):
        # Generate the parent space.
        space = self.state.relate(self.flow.base)
        # Encode the predicate expression.
        filter = self.state.encode(self.flow.filter)
        # Produce a filtering space operation.
        return FilteredSpace(space, filter, self.flow)


class RelateSort(Relate):

    adapt(SortFlow)

    def __call__(self):
        # Generate the parent space.
        space = self.state.relate(self.flow.base)
        # List of pairs `(code, direction)` containing the expressions
        # to sort by and respective direction indicators.
        order = []
        # Iterate over ordering flow nodes.
        for flow, direction in self.flow.order:
            # Encode the flow node.
            code = self.state.encode(flow)
            order.append((code, direction))
        # The slice indicators.
        limit = self.flow.limit
        offset = self.flow.offset
        # Produce an ordering space operation.
        return OrderedSpace(space, order, limit, offset, self.flow)


class RelateQuotient(Relate):

    adapt(QuotientFlow)

    def __call__(self):
        # Generate the parent space.
        base = self.state.relate(self.flow.base)
        # Generate the seed space of the quotient.
        seed = self.state.relate(self.flow.seed)
        # Verify that the seed is a plural descendant of the parent space.
        with translate_guard(seed):
            if base.spans(seed):
                raise Error("Expected a plural expression")
            if not seed.spans(base):
                raise Error("Expected a descendant expression")
        # Encode the kernel expressions.
        kernels = [self.state.encode(flow)
                   for flow in self.flow.kernels]
        # Note: we need to check that the kernel is not scalar, but we can't
        # do it here because some units may be removed by the unmasking
        # process; so the check is delegated to unmasking.
        # Produce a quotient space.
        return QuotientSpace(base, seed, kernels, self.flow)


class RelateComplement(Relate):

    adapt(ComplementFlow)

    def __call__(self):
        # Generate the parent space.
        base = self.state.relate(self.flow.base)
        # Produce a complement space.
        return ComplementSpace(base, self.flow)


class RelateMoniker(Relate):

    adapt(CoverFlow)

    def __call__(self):
        # Generate the parent space.
        base = self.state.relate(self.flow.base)
        # Generate the seed space.
        seed = self.state.relate(self.flow.seed)
        # Produce a masking space operation.
        return MonikerSpace(base, seed, self.flow)


class RelateFork(Relate):

    adapt(ForkFlow)

    def __call__(self):
        # Generate the parent space.
        base = self.state.relate(self.flow.base)
        # The seed coincides with the parent space -- but could be changed
        # after the rewrite step.
        seed = base
        # Generate the fork kernel.
        kernels = [self.state.encode(flow)
                   for flow in self.flow.kernels]
        # Verify that the kernel is singular against the parent space.
        # FIXME: handled by the compiler.
        #for code in kernels:
        #    if not all(seed.spans(unit.space) for unit in code.units):
        #        raise Error("a singular expression is expected",
        #                    code.mark)
        return ForkedSpace(base, seed, kernels, self.flow)


class RelateAttach(Relate):

    adapt(AttachFlow)

    def __call__(self):
        # Generate the parent and the seed spaces.
        base = self.state.relate(self.flow.base)
        seed = self.state.relate(self.flow.seed)
        # Encode linking expressions.
        images = [(self.state.encode(lflow), self.state.encode(rflow))
                  for lflow, rflow in self.flow.images]
        # Verify that linking pairs are singular against the parent and
        # the seed spaces.
        # FIXME: don't check as the constraint may be violated after
        # rewriting; handled by the compiler.
        #for lcode, rcode in images:
        #    if not all(base.spans(unit.space) for unit in lcode.units):
        #        raise Error("a singular expression is expected",
        #                    lcode.mark)
        #    if not all(seed.spans(unit.space) for unit in rcode.units):
        #        raise Error("a singular expression is expected",
        #                    rcode.mark)
        filter = None
        if self.flow.condition is not None:
            filter = self.state.encode(self.flow.condition)
        return AttachSpace(base, seed, images, filter, self.flow)


class EncodeClip(Encode):

    adapt(ClipFlow)

    def __call__(self):
        root = self.state.relate(self.flow.base)
        code = self.state.encode(self.flow.seed)
        units = [unit for unit in code.units]
        if not units:
            space = RootSpace(None, self.flow)
        else:
            spaces = []
            for unit in units:
                if any(space.dominates(unit.space) for space in spaces):
                    continue
                spaces = [space for space in spaces
                              if not unit.space.dominates(space)]
                spaces.append(unit.space)
            if len(spaces) > 1:
                raise Error("Cannot deduce an unambiguous clip flow")
            else:
                [space] = spaces
        if not space.spans(root):
            raise Error("Expected a descendant clip flow")
        filter = FormulaCode(IsNullSig(-1), coerce(BooleanDomain()),
                             code.flow, op=code)
        space = FilteredSpace(space, filter, space.flow)
        space = ClippedSpace(root, space, self.flow.limit,
                             self.flow.offset, self.flow)
        return CoveringUnit(code, space, self.flow)


class RelateClip(Relate):

    adapt(ClipFlow)

    def __call__(self):
        base = self.state.relate(self.flow.base)
        seed = self.state.relate(self.flow.seed)
        if not (seed.spans(base) and not base.spans(seed)):
            with translate_guard(self.flow.seed):
                raise Error("Expected a plural expression")
        return ClippedSpace(base, seed, self.flow.limit,
                           self.flow.offset, self.flow)


class RelateLocator(Relate):

    adapt(LocateFlow)

    def __call__(self):
        base = self.state.relate(self.flow.base)
        seed = self.state.relate(self.flow.seed)
        images = [(self.state.encode(lop), self.state.encode(rop))
                  for lop, rop in self.flow.images]
        filter = None
        if self.flow.condition is not None:
            filter = self.state.encode(self.flow.condition)
        return LocatorSpace(base, seed, images, filter, self.flow)


class EncodeColumn(Encode):

    adapt(ColumnFlow)

    def __call__(self):
        # Find the space of the column.
        space = self.state.relate(self.flow.base)
        # Generate a column unit node on the space.
        return ColumnUnit(self.flow.column, space, self.flow)


class RelateColumn(Relate):

    adapt(ColumnFlow)

    def __call__(self):
        # If the column flow has an associated table flow node,
        # delegate the adapter to it.
        if self.flow.link is not None:
            return self.state.relate(self.flow.link)
        # Otherwise, let the parent produce an error message.
        return super(RelateColumn, self).__call__()


class EncodeKernel(Encode):

    adapt(KernelFlow)

    def __call__(self):
        # Get the quotient space of the kernel.
        space = self.state.relate(self.flow.base)
        # Extract the respective kernel expression from the space.
        code = space.family.kernels[self.flow.index]
        # Generate a unit expression.
        return KernelUnit(code, space, self.flow)


class EncodeLiteral(Encode):

    adapt(LiteralFlow)

    def __call__(self):
        # Switch the class from `Flow` to `Code` keeping all attributes.
        return LiteralCode(self.flow.value, self.flow.domain,
                           self.flow)


class EncodeCast(Encode):

    adapt(CastFlow)

    def __call__(self):
        # Delegate it to the `Convert` adapter.
        return Convert.__prepare__(self.flow, self.state)()


class Convert(Adapter):
    """
    Encodes a cast flow to a code node.

    This is an auxiliary adapter used to encode
    :class:`htsql.core.tr.flow.CastFlow` nodes.  The adapter is polymorphic
    by the origin and the target domains.

    The purpose of the adapter is multifold.  The :class:`Convert` adapter:

    - verifies that the conversion from the source to the target
      domain is admissible;
    - eliminates redundant conversions;
    - handles conversion from the special types:
      :class:`htsql.core.domain.UntypedDomain` and
      :class:`htsql.core.domain.RecordDomain`;
    - when possible, expresses the cast in terms of other operations; otherwise,
      generates a new :class:`htsql.core.tr.space.CastCode` node.

    `flow` (:class:`htsql.core.tr.flow.CastFlow`)
        The flow node to encode.

        Note that the adapter is dispatched on the pair
        `(flow.base.domain, flow.domain)`.

    `state` (:class:`EncodingState`)
        The current state of the encoding process.

    Aliases:

    `base` (:class:`htsql.core.tr.flow.Flow`)
        An alias for `flow.base`; the operand of the cast expression.

    `domain` (:class:`htsql.core.domain.Domain`)
        An alias for `flow.domain`; the target domain.
    """

    adapt(Domain, Domain)

    @classmethod
    def __dispatch__(interface, flow, *args, **kwds):
        # We override the standard extract of the dispatch key, which
        # returns the type of the first argument(s).  For `Convert`,
        # the dispatch key is the pair of the origin and the target domains.
        assert isinstance(flow, CastFlow)
        return (type(flow.base.domain), type(flow.domain))

    def __init__(self, flow, state):
        assert isinstance(flow, CastFlow)
        assert isinstance(state, EncodingState)
        self.flow = flow
        self.base = flow.base
        self.domain = flow.domain
        self.state = state

    def __call__(self):
        # A final check to eliminate conversion when the origin and
        # the target domains are the same.  It is likely no-op since
        # this case should be already handled.
        if self.base.domain == self.domain:
            return self.state.encode(self.base)
        # The default implementation complains that the conversion is
        # not admissible.
        raise Error("Cannot convert a value of type %s to %s"
                    % (self.base.domain, self.domain))


class ConvertUntyped(Convert):
    # Validate and convert untyped literals.

    adapt(UntypedDomain, Domain)

    def __call__(self):
        # The base flow is of untyped domain, however it does not have
        # to be an instance of `LiteralFlow` since the actual literal node
        # could be wrapped by decorators.  However after we encode the node,
        # the decorators are gone and the result must be a `LiteralCode`
        # The domain should remain the same too.
        # FIXME: the literal could possibly be wrapped into `ScalarUnit`
        # if the literal flow was rescoped.
        base = self.state.encode(self.base)
        assert isinstance(base, (LiteralCode, ScalarUnit))
        assert isinstance(base.domain, UntypedDomain)
        # Unwrap scalar units from the literal code.
        wrappers = []
        while isinstance(base, ScalarUnit):
            wrappers.append(base)
            base = base.code
        assert isinstance(base, LiteralCode)
        # If the operand is a scalar unit,
        # Convert the serialized literal value to a Python object; raises
        # a `ValueError` if the literal is not in a valid format.
        try:
            value = self.domain.parse(base.value)
        except ValueError as exc:
            # FIXME: `domain.parse()` should raise `Error`?
            raise Error(str(exc))
        # Generate a new literal node with the converted value and
        # the target domain.
        code = LiteralCode(value, self.domain, self.flow)
        # If necessary, wrap the literal back into scalar units.
        while wrappers:
            wrapper = wrappers.pop()
            code = wrapper.clone(code=code)
        return code


class ConvertToItself(Convert):
    # Eliminate redundant conversions.

    adapt_many((BooleanDomain, BooleanDomain),
               (IntegerDomain, IntegerDomain),
               (FloatDomain, FloatDomain),
               (DecimalDomain, DecimalDomain),
               (TextDomain, TextDomain),
               (DateDomain, DateDomain),
               (TimeDomain, TimeDomain),
               (DateTimeDomain, DateTimeDomain))
    # FIXME: do we need `EnumDomain` here?

    def __call__(self):
        # Encode and return the operand of the cast; drop the cast node itself.
        return self.state.encode(self.flow.base)


class ConvertEntityToBoolean(Convert):
    # Converts a record expression to a conditional expression.

    adapt_many((EntityDomain, BooleanDomain),
               (RecordDomain, BooleanDomain))

    def __call__(self):
        # When the flow domain is tuple, we assume that the flow
        # represents some space.  In this case, Boolean cast produces
        # an expression which is `FALSE` when the space is empty and
        # `TRUE` otherwise.  The actual expression is:
        #   `!is_null(unit)`,
        # where `unit` is some non-nullable function on the space.

        # Translate the operand to a space node.
        space = self.state.relate(self.base)
        # A `TRUE` literal.
        true_literal = LiteralCode(True, coerce(BooleanDomain()), self.flow)
        # A `TRUE` constant as a function on the space.
        unit = ScalarUnit(true_literal, space, self.flow)
        # Return `!is_null(unit)`.
        return FormulaCode(IsNullSig(-1), coerce(BooleanDomain()),
                           self.flow, op=unit)


class ConvertTextToBoolean(Convert):
    # Convert a string expression to a conditional expression.

    adapt(TextDomain, BooleanDomain)

    def __call__(self):
        # A `NULL` value and an empty string are converted to `FALSE`,
        # any other string value is converted to `TRUE`.

        # Encode the operand of the cast.
        code = self.state.encode(self.base)
        # An empty string.
        empty_literal = LiteralCode('', self.base.domain, self.flow)
        # Construct: `null_if(base,'')`.
        code = FormulaCode(NullIfSig(), self.base.domain, self.flow,
                           lop=code, rop=empty_literal)
        # Construct: `!is_null(null_if(base,''))`.
        code = FormulaCode(IsNullSig(-1), self.domain, self.flow,
                           op=code)
        # Return `!is_null(null_if(base,''))`.
        return code


class ConvertToBoolean(Convert):
    # Convert an expression of any type to a conditional expression.

    adapt_many((NumberDomain, BooleanDomain),
               (EnumDomain, BooleanDomain),
               (DateDomain, BooleanDomain),
               (TimeDomain, BooleanDomain),
               (DateTimeDomain, BooleanDomain),
               (OpaqueDomain, BooleanDomain))
    # Note: we include the opaque domain here to ensure that any
    # data type could be converted to Boolean.  However this may
    # lead to unintuitive results.

    def __call__(self):
        # A `NULL` value is converted to `FALSE`; any other value is
        # converted to `TRUE`.

        # Construct and return `!is_null(base)`.
        return FormulaCode(IsNullSig(-1), self.domain, self.flow,
                           op=self.state.encode(self.base))


class ConvertToText(Convert):
    # Convert an expression to a string.

    adapt_many((BooleanDomain, TextDomain),
               (NumberDomain, TextDomain),
               (EnumDomain, TextDomain),
               (DateDomain, TextDomain),
               (TimeDomain, TextDomain),
               (DateTimeDomain, TextDomain),
               (OpaqueDomain, TextDomain))
    # Note: we assume we could convert any opaque data type to string;
    # it is risky but convenient.

    def __call__(self):
        # We generate a cast code node leaving it to the serializer
        # to specialize on the origin data type.
        return CastCode(self.state.encode(self.base), self.domain,
                        self.flow)


class ConvertToInteger(Convert):
    # Convert an expression to an integer value.

    adapt_many((DecimalDomain, IntegerDomain),
               (FloatDomain, IntegerDomain),
               (TextDomain, IntegerDomain))

    def __call__(self):
        # We leave conversion from literal values to the database
        # engine even though we could handle it here because the
        # conversion may be engine-specific.
        return CastCode(self.state.encode(self.base), self.domain,
                        self.flow)


class ConvertToDecimal(Convert):
    # Convert an expression to a decimal value.

    adapt_many((IntegerDomain, DecimalDomain),
               (FloatDomain, DecimalDomain),
               (TextDomain, DecimalDomain))

    def __call__(self):
        # Encode the operand of the cast.
        code = self.state.encode(self.base)
        # Handle conversion from an integer literal manually.
        # We do not handle conversion from other literal types
        # because it may be engine-specific.
        if isinstance(code, LiteralCode):
            if isinstance(code.domain, IntegerDomain):
                if code.value is None:
                    return code.clone(domain=self.domain)
                else:
                    value = decimal.Decimal(code.value)
                    return code.clone(value=value, domain=self.domain)
        # For the regular case, generate an appropriate cast node.
        return CastCode(code, self.domain, self.flow)


class ConvertToFloat(Convert):
    # Convert an expression to a float value.

    adapt_many((IntegerDomain, FloatDomain),
               (DecimalDomain, FloatDomain),
               (TextDomain, FloatDomain))

    def __call__(self):
        # Encode the operand of the cast.
        code = self.state.encode(self.base)
        # Handle conversion from an integer and decimal literals manually.
        # We do not handle conversion from other literal types because it
        # may be engine-specific.
        if isinstance(code, LiteralCode):
            if isinstance(code.domain, (IntegerDomain, DecimalDomain)):
                if code.value is None:
                    return code.clone(domain=self.domain)
                else:
                    value = float(code.value)
                    return code.clone(value=value, domain=self.domain)
        # For the regular case, generate an appropriate cast node.
        return CastCode(code, self.domain, self.flow)


class ConvertToDate(Convert):
    # Convert an expression to a date value.

    adapt_many((TextDomain, DateDomain),
               (DateTimeDomain, DateDomain))

    def __call__(self):
        # We leave conversion from literal values to the database
        # engine even though we could handle it here because the
        # conversion may be engine-specific.
        return CastCode(self.state.encode(self.base), self.domain,
                        self.flow)


class ConvertToTime(Convert):
    # Convert an expression to a time value.

    adapt_many((TextDomain, TimeDomain),
               (DateTimeDomain, TimeDomain))

    def __call__(self):
        # Leave conversion to the database engine.
        return CastCode(self.state.encode(self.base), self.domain,
                        self.flow)


class ConvertToDateTime(Convert):
    # Convert an expression to a datetime value.

    adapt_many((TextDomain, DateTimeDomain),
               (DateDomain, DateTimeDomain))

    def __call__(self):
        # Leave conversion to the database engine.
        return CastCode(self.state.encode(self.base), self.domain,
                        self.flow)


class EncodeRescoping(Encode):

    adapt(RescopingFlow)

    def __call__(self):
        # Wrap the base expression into a scalar unit.
        code = self.state.encode(self.flow.base)
        space = self.state.relate(self.flow.scope)
        return ScalarUnit(code, space, self.flow)


class EncodeFormula(Encode):

    adapt(FormulaFlow)

    def __call__(self):
        # Delegate the translation to the `EncodeBySignature` adapter.
        return EncodeBySignature.__prepare__(self.flow, self.state)()


class EncodeBySignatureBase(Adapter):
    """
    Translates a formula node.

    This is a base class for the two encoding adapters:
    :class:`EncodeBySignature` and :class:`RelateBySignature`;
    it encapsulates methods and attributes shared between these adapters.

    The adapter accepts a flow formula node and is polymorphic
    on the formula signature.

    `flow` (:class:`htsql.core.tr.flow.FormulaFlow`)
        The formula node to encode.

    `state` (:class:`EncodingState`)
        The current state of the encoding process.

    Aliases:

    `signature` (:class:`htsql.core.tr.signature.Signature`)
        The signature of the formula.

    `domain` (:class:`htsql.core.tr.domain.Domain`)
        The co-domain of the formula.

    `arguments` (:class:`htsql.core.tr.signature.Bag`)
        The arguments of the formula.
    """

    adapt(Signature)

    @classmethod
    def __dispatch__(interface, flow, *args, **kwds):
        # We need to override `dispatch` since the adapter is polymorphic
        # not on the type of the node itself, but on the type of the
        # node signature.
        assert isinstance(flow, FormulaFlow)
        return (type(flow.signature),)

    def __init__(self, flow, state):
        assert isinstance(flow, FormulaFlow)
        assert isinstance(state, EncodingState)
        self.flow = flow
        self.state = state
        # Extract commonly used attributes of the node.
        self.signature = flow.signature
        self.domain = flow.domain
        self.arguments = flow.arguments


class EncodeBySignature(EncodeBySignatureBase):
    """
    Translates a formula flow to a code node.

    This is an auxiliary adapter used to encode
    class:`htsql.core.tr.flow.FormulaFlow` nodes.  The adapter is
    polymorphic on the formula signature.

    Unless overridden, the adapter encodes the arguments of the formula
    and generates a new formula code with the same signature.
    """

    def __call__(self):
        # Encode the arguments of the formula.
        arguments = self.arguments.map(self.state.encode)
        # Produce a formula code with the same signature.
        return FormulaCode(self.signature,
                           self.domain,
                           self.flow,
                           **arguments)


class UnpackSelection(Unpack):

    adapt(SelectionFlow)

    def __call__(self):
        codes = []
        segments = []
        space = self.state.relate(self.flow)
        indicator = LiteralCode(True, coerce(BooleanDomain()), self.flow)
        indicator = ScalarUnit(indicator, space, self.flow)
        codes.append(indicator)
        for element in self.flow.elements:
            bundle = self.state.unpack(element)
            codes.extend(bundle.codes)
            segments.extend(bundle.segments)
        return Bundle(codes, segments)


class RelateSelection(Relate):

    adapt_many(SelectionFlow,
               IdentityFlow)

    def __call__(self):
        return self.state.relate(self.flow.base)


class UnpackIdentity(Unpack):

    adapt(IdentityFlow)

    def __call__(self):
        codes = []
        segments = []
        indicators =  []
        space = self.state.relate(self.flow)
        true_indicator = LiteralCode(True, coerce(BooleanDomain()), self.flow)
        true_indicator = ScalarUnit(true_indicator, space, self.flow)
        indicators.append(true_indicator)
        for element in self.flow.elements:
            bundle = self.state.unpack(element)
            for code in bundle.codes:
                if (isinstance(code, ScalarUnit) and
                    code.space.dominates(space) and
                    isinstance(code.code, LiteralCode) and
                    code.code.value is True):
                    code = true_indicator
                elif (not space.is_inflated and
                      isinstance(code, ColumnUnit) and
                      code.space.dominates(space) and
                      code.space != space):
                    code = code.clone(space=code.space.inflate())
                codes.append(code)
            segments.extend(bundle.segments)
            if bundle.codes:
                indicator = bundle.codes[0]
                if (isinstance(indicator, ScalarUnit) and
                    indicator.space.conforms(space) and
                    isinstance(indicator.code, LiteralCode) and
                    indicator.code.value is True):
                    continue
                if (isinstance(indicator, ColumnUnit) and
                    indicator.space.conforms(space) and
                    not indicator.column.is_nullable):
                    continue
                if not (isinstance(indicator, FormulaCode) and
                        indicator.signature == IsNullSig(-1)):
                    indicator = FormulaCode(IsNullSig(-1),
                                            coerce(BooleanDomain()),
                                            indicator.flow, op=indicator)
                indicators.append(indicator)
        if len(indicators) == 1:
            [indicator] = indicators
        else:
            indicator = FormulaCode(AndSig(), coerce(BooleanDomain()),
                                    self.flow, ops=indicators)
        codes.insert(0, indicator)
        return Bundle(codes, segments)


def encode(flow, state=None):
    """
    Encodes the given flow to an expression node.

    Returns a :class:`htsql.core.tr.space.Expression` instance (in most cases,
    a :class:`htsql.core.tr.space.Code` instance).

    `flow` (:class:`htsql.core.tr.flow.Flow`)
        The flow node to encode.

    `state` (:class:`EncodingState` or ``None``)
        The encoding state to use.  If not set, a new encoding state
        is instantiated.
    """
    # Create a new encoding state if necessary.
    if state is None:
        state = EncodingState()
    # Realize and apply the `Encode` adapter.
    return Encode.__invoke__(flow, state)


def relate(flow, state=None):
    """
    Encodes the given flow to a data space node.

    Returns a :class:`htsql.core.tr.space.Space` instance.

    `flow` (:class:`htsql.core.tr.flow.Flow`)
        The flow node to encode.

    `state` (:class:`EncodingState` or ``None``)
        The encoding state to use.  If not set, a new encoding state
        is instantiated.
    """
    # Create a new encoding state if necessary.
    if state is None:
        state = EncodingState()
    # Realize and apply the `Relate` adapter.
    return Relate.__invoke__(flow, state)


def encode(flow):
    state = EncodingState()
    bundle = state.unpack(flow)
    if len(bundle.codes) == 0 and len(bundle.segments) == 1:
        [segment] = bundle.segments
    else:
        root = RootSpace(None, flow)
        space = state.relate(flow)
        if not root.spans(space) or \
            any(not space.spans(unit.space) for code in bundle.codes
                                            for unit in code.units):
            with translate_guard(flow):
                raise Error("Expected a singular expression")
        segment = SegmentExpr(root, space, bundle.codes, bundle.segments, flow)
    return segment


