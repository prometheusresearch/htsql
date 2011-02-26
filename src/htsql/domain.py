#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.domain`
===================

This module defines HTSQL domains.
"""


from .util import maybe, oneof, listof
import re
import decimal
import datetime


class Domain(object):
    """
    Represents an HTSQL domain (data type).

    A domain indicates the type of an object.  Most HTSQL domains correspond
    to SQL data types; some domains are special and used when the actual
    SQL data type is unknown or nonsensical.

    A value of a specific domain could be represented in two forms:

    - as an HTSQL literal;

    - as a native Python object.

    Methods :meth:`parse` and :meth:`dump` translate values from one form
    to the other.
    """

    def parse(self, data):
        """
        Converts an HTSQL literal to a native Python object.

        Raises :exc:`ValueError` if the literal is not in a valid format.

        `data` (a string or ``None``)
            An HTSQL literal representing a value of the given domain.

        Returns a native Python object representing the same value.
        """
        # Sanity check on the argument.
        assert isinstance(data, maybe(str))

        # `None` values are passed through.
        if data is None:
            return None
        # By default, we do not accept any literals; subclasses should
        # override this method.
        raise ValueError("invalid literal")

    def dump(self, value):
        """
        Converts a native Python object to an HTSQL literal.

        `value` (acceptable types depend on the domain)
            A native Python object representing a value of the given domain.

        Returns an HTSQL literal representing the same value.
        """
        # Sanity check on the argument.
        assert value is None
        # By default, only accept `None`; subclasses should override
        # this method.
        return None

    def __str__(self):
        # Domains corresponding to concrete SQL data types may override
        # this method to return the name of the type.
        return self.__class__.__name__.lower()

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self)

    def __eq__(self, other):
        # Used when comparing some of the code objects.  The generic
        # implementations check that the classes are identical and
        # all attributes are equal; concrete SQL domains may override
        # this method to provide engine-specific type equality.
        if not isinstance(other, Domain):
            return False
        return (self.__class__ == other.__class__)

    def __ne__(self, other):
        # Have to override it since we override `__eq__`.
        return not (self == other)

    def __hash__(self):
        # Have to override it since we override `__eq__`.  We use a rough hash
        # which may generate false positives, but relieves us from the need
        # to override `__hash__` for every subclass.
        return hash(self.__class__)


class VoidDomain(Domain):
    """
    Represents a domain without any valid values.

    This domain is assigned to objects when the domain is structurally
    required, but does not have any semantics.
    """


class UntypedDomain(Domain):
    """
    Represents an unknown type.

    This domain is assigned to HTSQL literals temporarily until the actual
    domain could be derived from the context.
    """


class TupleDomain(Domain):
    """
    Represents a table domain.

    This domain is assigned to table expressions.
    """
    # FIXME: add a reference to the underlying `TableEntity`.  This may
    # require importing `TableEntity` from `htsql.entity`, which creates
    # a circular module dependency.  To break it, we will have to split
    # `htsql.domain` into two modules: `htsql.type`, containing `Domain`
    # and all its subclasses representing real database types, and
    # `htsql.domain`, which imports all types from `htsql.type` and
    # adds special domains like `VoidDomain`, `TupleDomain` and
    # `UntypedDomain`.


class BooleanDomain(Domain):
    """
    Represents Boolean data type.

    Valid literal values: ``true``, ``false``.

    Valid native values: `bool` objects.
    """

    def parse(self, data):
        # Sanity check on the argument.
        assert isinstance(data, maybe(str))

        # Convert: `None` -> `None`, `'true'` -> `True`, `'false'` -> `False`.
        if data is None:
            return None
        if data == 'true':
            return True
        if data == 'false':
            return False
        raise ValueError("invalid Boolean literal: expected 'true' or 'false';"
                         " got %r" % data)

    def dump(self, value):
        # Sanity check on the argument.
        assert isinstance(value, maybe(bool))

        # Convert `None` -> `None`, `True` -> `'true'`, `False` -> `'false'`.
        if value is None:
            return None
        if value is True:
            return 'true'
        if value is False:
            return 'false'


class NumberDomain(Domain):
    """
    Represents a numeric data type.

    This is an abstract data type, see :class:`IntegerDomain`,
    :class:`FloatDomain`, :class:`DecimalDomain` for concrete subtypes.

    Class attributes:

    `is_exact` (Boolean)
        Indicates whether the domain represents exact values.

    `radix` (``2`` or ``10``)
        Indicates whether the values are stored in binary or decimal form.
    """

    is_exact = None
    radix = None


class IntegerDomain(NumberDomain):
    """
    Represents a binary integer data type.

    Valid literal values: integers (in base 2) with an optional sign.

    Valid native values: `int` or `long` objects.

    `size` (an integer or ``None``)
        Number of bits used to store a value; ``None`` if not known.
    """

    is_exact = True
    radix = 2

    def __init__(self, size=None):
        # Sanity check on the arguments.
        assert isinstance(size, maybe(int))
        self.size = size

    def parse(self, data):
        # Sanity check on the arguments.
        assert isinstance(data, maybe(str))
        # `None` represents `NULL` both in literal and native format.
        if data is None:
            return None
        # Expect an integer value in base 10.
        try:
            value = int(data, 10)
        except ValueError:
            raise ValueError("invalid integer literal: expected an integer"
                             " in a decimal format; got %r" % data)
        return value

    def dump(self, value):
        # Sanity check on the arguments.
        assert isinstance(value, maybe(oneof(int, long)))
        # `None` represents `NULL` both in literal and native format.
        if value is None:
            return None
        # Represent an integer value as a decimal number.
        return str(value)

    def __eq__(self, other):
        if not isinstance(other, Domain):
            return False
        return (self.__class__ == other.__class__ and self.size == other.size)


class FloatDomain(NumberDomain):
    """
    Represents an IEEE 754 float data type.

    Valid literal values: floating-point numbers in decimal or scientific
    format.

    Valid native values: `float` objects.

    `size` (an integer or ``None``)
        Number of bits used to store a value; ``None`` if not known.
    """

    is_exact = False
    radix = 2

    def __init__(self, size=None):
        # Sanity check on the arguments.
        assert isinstance(size, maybe(int))
        self.size = size

    def parse(self, data):
        # Sanity check on the argument.
        assert isinstance(data, maybe(str))
        # `None` represents `NULL` both in literal and native format.
        if data is None:
            return None
        # Parse the numeric value.
        try:
            value = float(data)
        except ValueError:
            raise ValueError("invalid float literal: %s" % data)
        # Check if we got a finite number.
        # FIXME: the check may break under Python 2.5; also, in Python 2.6
        # could use `math.isinf()` and `math.isnan()`.
        if str(value) in ['inf', '-inf', 'nan']:
            raise ValueError("invalid float literal: %s" % value)
        return value

    def dump(self, value):
        # Sanity check on the argument.
        assert isinstance(value, maybe(float))
        # `None` represents `NULL` both in literal and native format.
        if value is None:
            return None
        # Use `repr` to avoid loss of precision.
        return repr(value)

    def __eq__(self, other):
        if not isinstance(other, Domain):
            return False
        return (self.__class__ == other.__class__ and self.size == other.size)


class DecimalDomain(NumberDomain):
    """
    Represents an exact decimal data type.

    Valid literal values: floating-point numbers in decimal or scientific
    format.

    Valid native values: `decimal.Decimal` objects.

    `precision` (an integer or ``None``)
        Number of significant digits; ``None`` if infinite or not known.

    `scale` (an integer or ``None``)
        Number of significant digits in the fractional part; zero for
        integers, ``None`` if infinite or not known.
    """

    is_exact = True
    radix = 10

    def __init__(self, precision=None, scale=None):
        # Sanity check on the arguments.
        assert isinstance(precision, maybe(int))
        assert isinstance(scale, maybe(int))
        self.precision = precision
        self.scale = scale

    def parse(self, data):
        # Sanity check on the arguments.
        assert isinstance(data, maybe(str))
        # `None` represents `NULL` both in literal and native format.
        if data is None:
            return None
        # Parse the literal (NB: it handles `inf` and `nan` values too).
        try:
            value = decimal.Decimal(data)
        except decimal.InvalidOperation:
            raise ValueError("invalid decimal literal: %s" % data)
        # Verify that we got a finite number.
        if not value.is_finite():
            raise ValueError("invalid decimal literal: %s" % data)
        return value

    def dump(self, value):
        # Sanity check on the argument.
        assert isinstance(value, maybe(decimal.Decimal))
        # `None` represents `NULL` both in literal and native format.
        if value is None:
            return None
        # Handle `inf` and `nan` values.
        if value.is_nan():
            return 'nan'
        elif value.is_infinite() and value > 0:
            return 'inf'
        elif value.is_infinite() and value < 0:
            return '-inf'
        # Produce a decimal representation of the number.
        return str(value)

    def __eq__(self, other):
        if not isinstance(other, Domain):
            return False
        return (self.__class__ == other.__class__ and
                self.precision == other.precision and
                self.scale == other.scale)


class StringDomain(Domain):
    """
    Represents a string data type.

    Valid literal values: all literal values.

    Valid native values: `str` objects in UTF-8 encoding;
    the `NUL` character is not allowed.

    `length` (an integer or ``None``)
        The maximum length of the value; ``None`` if infinite or not known.

    `is_varying` (Boolean)
        Indicates whether values are fixed-length or variable-length.
    """


    def __init__(self, length=None, is_varying=True):
        # Sanity check on the arguments.
        assert isinstance(length, maybe(int))
        assert isinstance(is_varying, bool)
        self.length = length
        self.is_varying = is_varying

    def parse(self, data):
        # Sanity check on the argument.
        assert isinstance(data, maybe(str))
        # `None` represents `NULL` both in literal and native format.
        if data is None:
            return None
        # No conversion is required for string values.
        return data

    def dump(self, value):
        # Sanity check on the argument.
        assert isinstance(value, maybe(str))
        if value is not None:
            assert '\0' not in value
            assert value.decode('utf-8', 'ignore').encode('utf-8') == value
        # `None` represents `NULL` both in literal and native format.
        if value is None:
            return None
        # No conversion is required for string values.
        return value

    def __eq__(self, other):
        if not isinstance(other, Domain):
            return False
        return (self.__class__ == other.__class__ and
                self.length == other.length and
                self.is_varying == other.is_varying)


class EnumDomain(Domain):
    """
    Represents an enumeration data type.

    An enumeration domain has a predefined set of valid string values.

    `labels` (a list of strings)
        List of valid values.
    """

    def __init__(self, labels):
        assert isinstance(labels, listof(str))
        self.labels = labels

    def parse(self, data):
        # Sanity check on the argument.
        assert isinstance(data, maybe(str))
        # `None` represents `NULL` both in literal and native format.
        if data is None:
            return None
        # Check if the value belongs to the fixed list of valid values.
        if data not in self.labels:
            raise ValueError("invalid enum literal: expected one of %s; got %r"
                             % (", ".join(repr(label)
                                          for label in self.labels), data))
        # No conversion is required.
        return data

    def dump(self, value):
        # Sanity check on the argument.
        assert isinstance(value, maybe(str))
        if value is not None:
            assert value in self.labels
        # `None` represents `NULL` both in literal and native format.
        if value is None:
            return None
        # No conversion is required.
        return value

    def __eq__(self, other):
        if not isinstance(other, Domain):
            return False
        # Here we check equality by comparing the labels.  Concrete SQL enum
        # types should probably override this method to provide engine-specific
        # comparison.
        return (self.__class__ == other.__class__ and
                self.labels == other.labels)


class DateDomain(Domain):
    """
    Represents a date data type.

    Valid literal values: valid date values in the form `YYYY-MM-DD`.

    Valid native values: `datetime.date` objects.
    """

    # Regular expression to match YYYY-MM-DD.
    pattern = r'^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})$'
    regexp = re.compile(pattern)

    def parse(self, data):
        # Sanity check on the argument.
        assert isinstance(data, maybe(str))
        # `None` represents `NULL` both in literal and native format.
        if data is None:
            return None
        # Parse `data` as YYYY-MM-DD.
        match = self.regexp.match(data)
        if match is None:
            raise ValueError("invalid date literal: expected a valid date"
                             " in a 'YYYY-MM-DD' format; got %r" % data)
        year = int(match.group('year'))
        month = int(match.group('month'))
        day = int(match.group('day'))
        # Generate a `datetime.date` value; may fail if the date is not valid.
        try:
            value = datetime.date(year, month, day)
        except ValueError, exc:
            raise ValueError("invalid date literal: %s" % exc)
        return value

    def dump(self, value):
        # Sanity check on the argument.
        assert isinstance(value, maybe(datetime.date))
        # `None` represents `NULL` both in literal and native format.
        if value is None:
            return None
        # `str` on `datetime.date` gives us the date in YYYY-MM-DD format.
        return str(value)


class OpaqueDomain(Domain):
    """
    Represents an unsupported SQL data type.
    
    Note: this is the only SQL domain with values that cannot be serialized
    using :meth:`dump`.
    """


