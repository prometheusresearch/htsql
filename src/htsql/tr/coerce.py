#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.coerce`
======================

This module implements the unary and binary coerce adapters.
"""


from ..util import listof
from ..adapter import Adapter, adapts, adapts_many
from ..domain import (Domain, VoidDomain, TupleDomain, UntypedDomain,
                      BooleanDomain, IntegerDomain, DecimalDomain, FloatDomain,
                      StringDomain, EnumDomain, DateDomain, TimeDomain,
                      DateTimeDomain, OpaqueDomain)


class UnaryCoerce(Adapter):
    """
    Validates and specializes a domain.

    The :class:`UnaryCoerce` adapter has the following signature::

        UnaryCoerce: Domain -> maybe(Domain)

    The adapter checks if the given domain is valid.  If so, the domain
    or its specialized version is returned; otherwise, ``None`` is returned.

    The primary use cases are:

    - disabling special domains in regular expressions;

    - specializing untyped values;

    - providing engine-specific versions of generic domains.

    The adapter is rarely used directly, use :func:`coerce` instead.
    """

    adapts(Domain)

    def __init__(self, domain):
        self.domain = domain

    def __call__(self):
        # By default, we assume that the domain is valid and specializes
        # into itself.
        return self.domain


class BinaryCoerce(Adapter):
    """
    Determines a common domain of two domains.

    The :class:`BinaryCoerce` adapter has the following signature::

        BinaryCoerce: (Domain, Domain) -> maybe(Domain)

    :class:`BinaryCoerce` is polymorphic on both arguments.  

    The adapter checks if two domains could be reduced to a single common
    domains.  If the common domain cannot be determined, the adapter returns
    ``None``.
    
    The primary use cases are:

    - checking if two domains are compatible (that is, if values of these
      domains are comparable without explicit cast).

    - deducing the actual type of untyped values.

    The adapter is rarely used directly, use :func:`coerce` instead.
    """

    adapts(Domain, Domain)

    def __init__(self, ldomain, rdomain):
        self.ldomain = ldomain
        self.rdomain = rdomain

    def __call__(self):
        # By default, we assume that the domain is compatible with itself.
        if self.ldomain == self.rdomain:
            return self.ldomain
        return None


class UnaryCoerceSpecial(UnaryCoerce):
    """
    Disables special domains.
    """

    adapts_many(VoidDomain,
                TupleDomain)

    def __call__(self):
        # `VoidDomain` and `TupleDomain` values cannot be compared.
        return None


class UnaryCoerceUntyped(UnaryCoerce):
    """
    Specializes untyped values.
    """

    adapts(UntypedDomain)

    def __call__(self):
        # Specializes untyped top-level expressions to the string type.
        return StringDomain()


class BinaryCoerceBoolean(BinaryCoerce):
    """
    Coerces untyped values to :class:`BooleanDomain`.
    """

    adapts_many((BooleanDomain, BooleanDomain),
                (BooleanDomain, UntypedDomain),
                (UntypedDomain, BooleanDomain))

    def __call__(self):
        return BooleanDomain()


class BinaryCoerceInteger(BinaryCoerce):
    """
    Coerces untyped values to :class:`IntegerDomain`.
    """

    adapts_many((IntegerDomain, IntegerDomain),
                (IntegerDomain, UntypedDomain),
                (UntypedDomain, IntegerDomain))

    def __call__(self):
        # Note that we use the generic version of the domain.  Engine addons
        # may override this implementation to provide an engine-specific
        # version of the domain.
        return IntegerDomain()


class BinaryCoerceDecimal(BinaryCoerce):
    """
    Coerces untyped and integer values to :class:`DecimalDomain`.
    """

    adapts_many((DecimalDomain, DecimalDomain),
                (DecimalDomain, IntegerDomain),
                (DecimalDomain, UntypedDomain),
                (IntegerDomain, DecimalDomain),
                (UntypedDomain, DecimalDomain))

    def __call__(self):
        # Note that we use the generic version of the domain.  Engine addons
        # may override this implementation to provide an engine-specific
        # version of the domain.
        return DecimalDomain()


class BinaryCoerceFloat(BinaryCoerce):
    """
    Coerces untyped, integer and decimal values to :class:`FloatDomain`.
    """

    adapts_many((FloatDomain, FloatDomain),
                (FloatDomain, DecimalDomain),
                (FloatDomain, IntegerDomain),
                (FloatDomain, UntypedDomain),
                (DecimalDomain, FloatDomain),
                (IntegerDomain, FloatDomain),
                (UntypedDomain, FloatDomain))

    def __call__(self):
        return FloatDomain()


class BinaryCoerceString(BinaryCoerce):
    """
    Coerces untyped values to :class:`StringDomain`.
    """

    adapts_many((StringDomain, StringDomain),
                (StringDomain, UntypedDomain),
                (UntypedDomain, StringDomain))

    def __call__(self):
        # Note that we use the generic version of the domain.  Engine addons
        # may override this implementation to provide an engine-specific
        # version of the domain.
        return StringDomain()


class BinaryCoerceEnum(BinaryCoerce):
    """
    Validates and coerces to :class:`EnumDomain`.
    """

    adapts_many((EnumDomain, EnumDomain),
                (EnumDomain, UntypedDomain),
                (UntypedDomain, EnumDomain))

    def __call__(self):
        # Note that `EnumDomain` represents not a single domain,
        # but a family of (incompatible) domains.  We assume that
        # only equal enum domains are compatible; the engines
        # should either satisfy this assumption or override this
        # implementation.
        if isinstance(self.ldomain, UntypedDomain):
            return self.rdomain
        elif isinstance(self.rdomain, UntypedDomain):
            return self.ldomain
        elif self.ldomain == self.rdomain:
            return self.ldomain
        return None


class BinaryCoerceDate(BinaryCoerce):
    """
    Coerce to :class:`DateDomain`.
    """

    adapts_many((DateDomain, DateDomain),
                (DateDomain, UntypedDomain),
                (UntypedDomain, DateDomain))

    def __call__(self):
        return DateDomain()


class BinaryCoerceTime(BinaryCoerce):
    """
    Coerce to :class:`TimeDomain`.
    """

    adapts_many((TimeDomain, TimeDomain),
                (TimeDomain, UntypedDomain),
                (UntypedDomain, TimeDomain))

    def __call__(self):
        return TimeDomain()


class BinaryCoerceDateTime(BinaryCoerce):
    """
    Coerce to :class:`DateTimeDomain`.
    """

    adapts_many((DateTimeDomain, DateTimeDomain),
                (DateTimeDomain, UntypedDomain),
                (UntypedDomain, DateTimeDomain))

    def __call__(self):
        return DateTimeDomain()


class BinaryCoerceOpaque(BinaryCoerce):
    """
    Validate and coerce to :class:`OpaqueDomain`.
    """

    adapts(OpaqueDomain, OpaqueDomain)

    def __call__(self):
        # This is the default implementation; we duplicate it here
        # to emphasize it.  `OpaqueDomain` represents not a single
        # domain, but a family of (incompatible) domains.  We assume
        # that opaque domains are compatible when they are equal;
        # the engines should either satisfy this assumption or override
        # this implementation.
        if self.ldomain == self.rdomain:
            return self.ldomain
        return None


def coerce(*domains):
    """
    Reduces a list of domains to a single common domain.

    `domains` (a list of :class:`htsql.domain.Domain`)
        List of domains.

    Returns the most specialized domain covering the given domains;
    ``None`` if the common domain could not be determined.
    """
    # FIXME: maybe overriding a builtin function is not a great idea?
    # It is deprecated though.

    # Sanity check on the argument.
    assert isinstance(list(domains), listof(Domain))

    # The function should never be called with an empty list of `domains`,
    # but we handle this case for completeness; returning `None` is
    # the only option in this case.
    if not domains:
        return None

    # Now apply `BinaryCoerce` to the first two domains in the list,
    # then to the result of the operation and the third domain in the list,
    # and so on.  Finally, apply `UnaryCoerce` to the result:
    #   UnaryCoerce(...(BinaryCoerce(
    #                   BinaryCoerce(domains[0],domains[1]),domains[2]),...))
    domain = domains[0]
    idx = 1
    while idx < len(domains):
        ldomain = domain
        rdomain = domains[idx]
        idx += 1
        binary_coerce = BinaryCoerce(ldomain, rdomain)
        domain = binary_coerce()
        if domain is None:
            break
    if domain is not None:
        unary_coerce = UnaryCoerce(domain)
        domain = unary_coerce()

    return domain


