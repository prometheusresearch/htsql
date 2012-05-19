#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql.core.domain`
========================

This module defines HTSQL domains.
"""


from .util import (maybe, oneof, listof, UTC, FixedTZ,
                   Printable, Clonable, Comparable)
import re
import math
import decimal
import datetime


class Domain(Comparable, Clonable, Printable):
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

    family = 'unknown'

    def __init__(self):
        pass

    def __basis__(self):
        return ()

    def parse(self, data):
        """
        Converts an HTSQL literal to a native Python object.

        Raises :exc:`ValueError` if the literal is not in a valid format.

        `data` (a Unicode string or ``None``)
            An HTSQL literal representing a value of the given domain.

        Returns a native Python object representing the same value.
        """
        # Sanity check on the argument.
        assert isinstance(data, maybe(unicode))

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
        return self.family


class VoidDomain(Domain):
    """
    Represents a domain without any valid values.

    This domain is assigned to objects when the domain is structurally
    required, but does not have any semantics.
    """
    family = 'void'


class UntypedDomain(Domain):
    """
    Represents an unknown type.

    This domain is assigned to HTSQL literals temporarily until the actual
    domain could be derived from the context.
    """
    family = 'untyped'


class Profile(Comparable, Clonable, Printable):

    def __init__(self, domain, **attributes):
        assert isinstance(domain, Domain)
        self.domain = domain
        for key in attributes:
            setattr(self, key, attributes[key])
        self.attributes = attributes

    def __basis__(self):
        return (self.domain,)

    def __str__(self):
        return str(self.domain)


class EntityDomain(Domain):

    family = 'entity'


class RecordDomain(Domain):

    family = 'record'

    def __init__(self, fields):
        assert isinstance(fields, listof(Profile))
        self.fields = fields

    def __basis__(self):
        return (tuple(self.fields),)

    def __str__(self):
        return "{%s}" % ", ".join(str(field) for field in self.fields)


class ListDomain(Domain):

    family = 'list'

    def __init__(self, item_domain):
        assert isinstance(item_domain, Domain)
        self.item_domain = item_domain

    def __basis__(self):
        return (self.item_domain,)

    def __str__(self):
        return "/%s" % self.item_domain


class IdentityDomain(Domain):

    family = 'identity'

    pattern = r"""
        (?P<ws> \s+ ) |
        (?P<symbol> \. | \[ | \( | \] | \) ) |
        (?P<unquoted> [\w-]+ ) |
        (?P<quoted> ' (?: [^'\0] | '')* ' )
    """
    regexp = re.compile(pattern, re.X|re.U)

    def __init__(self, fields):
        assert isinstance(fields, listof(Domain))
        self.fields = fields
        self.arity = 0
        for field in fields:
            if isinstance(field, IdentityDomain):
                self.arity += field.arity
            else:
                self.arity += 1

    def __basis__(self):
        return (tuple(self.fields),)

    def __str__(self):
        return "[%s]" % ".".join(str(field) for field in self.fields)

    def parse(self, data):
        # Sanity check on the arguments.
        assert isinstance(data, maybe(unicode))
        # `None` represents `NULL` both in literal and native format.
        if data is None:
            return None
        tokens = []
        start = 0
        while start < len(data):
            match = self.regexp.match(data, start)
            if match is None:
                raise ValueError("unexpected character %r" % data[start])
            start = match.end()
            if match.group('ws'):
                continue
            token = match.group()
            tokens.append(token)
        tokens.append(None)
        stack = []
        value = []
        arity = 0
        while tokens:
            token = tokens.pop(0)
            while token in u'[(':
                stack.append((value, arity, token))
                value = []
                arity = 0
                token = tokens.pop(0)
            if token is None or token in u'[(]).':
                raise ValueError("ill-formed locator")
            if token.startswith(u'\'') and token.endswith(u'\''):
                token = token[1:-1].replace(u'\'\'', u'\'')
            value.append((token, None))
            arity += 1
            token = tokens.pop(0)
            while token in u'])':
                if not stack:
                    raise ValueError("ill-formed locator")
                parent_value, parent_arity, parent_bracket = stack.pop()
                if ((token == u']' and parent_bracket != u'[') or
                    (token == u')' and parent_bracket != u'(')):
                    raise ValueError("ill-formed locator")
                parent_value.append((value, arity))
                value = parent_value
                arity += parent_arity
                token = tokens.pop(0)
            if (token is not None or stack) and token != u'.':
                raise ValueError("ill-formed locator")
        def collect(raw, arity, identity):
            value = []
            if arity != identity.arity:
                raise ValueError("ill-formed locator")
            for field in identity.fields:
                if isinstance(field, IdentityDomain):
                    total_arity = 0
                    items = []
                    while total_arity < field.arity:
                        assert raw
                        item, item_arity = raw.pop(0)
                        if total_arity == 0 and item_arity == field.arity:
                            items = item
                            total_arity = item_arity
                        elif item_arity is None:
                            total_arity += 1
                            items.append(item)
                        else:
                            total_arity += item_arity
                            items.append(item)
                    if total_arity > field.arity:
                        raise ValueError("ill-formed locator")
                    item = collect(items, total_arity, field)
                    value.append(item)
                else:
                    if not raw:
                        raise ValueError("ill-formed locator")
                    item, item_arity = raw.pop(0)
                    if item_arity is not None:
                        raise ValueError("ill-formed locator")
                    item = field.parse(item)
                    assert item is not None
                    value.append(item)
            return tuple(value)
        return collect(value, arity, self)

    def dump(self, value):
        assert isinstance(value, maybe(tuple))
        if value is None:
            return None
        def convert(value, fields, is_flattened=True):
            assert isinstance(value, tuple) and len(value) == len(fields)
            chunks = []
            is_simple = all(not isinstance(field, IdentityDomain)
                            for field in fields[1:])
            for item, field in zip(value, fields):
                if isinstance(field, IdentityDomain):
                    is_label_flattened = False
                    if len(field.fields) == 1:
                        is_label_flattened = True
                    if is_simple:
                        is_label_flattened = True
                    chunk = convert(item, field.fields, is_label_flattened)
                    chunks.append(chunk)
                else:
                    chunk = field.dump(item)
                    chunks.append(chunk)
            data = u".".join(chunks)
            if not is_flattened:
                data = u"(%s)" % data
            return data
        return convert(value, self.fields)


class BooleanDomain(Domain):
    """
    Represents Boolean data type.

    Valid literal values: ``true``, ``false``.

    Valid native values: `bool` objects.
    """
    family = 'boolean'

    def parse(self, data):
        # Sanity check on the argument.
        assert isinstance(data, maybe(unicode))

        # Convert: `None` -> `None`, `'true'` -> `True`, `'false'` -> `False`.
        if data is None:
            return None
        if data == u'true':
            return True
        if data == u'false':
            return False
        raise ValueError("invalid Boolean literal: expected 'true' or 'false';"
                         " got %r" % data.encode('utf-8'))

    def dump(self, value):
        # Sanity check on the argument.
        assert isinstance(value, maybe(bool))

        # Convert `None` -> `None`, `True` -> `'true'`, `False` -> `'false'`.
        if value is None:
            return None
        if value is True:
            return u'true'
        if value is False:
            return u'false'


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

    family = 'number'
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

    family = 'integer'
    is_exact = True
    radix = 2

    def __init__(self, size=None):
        # Sanity check on the arguments.
        assert isinstance(size, maybe(int))
        self.size = size

    def __basis__(self):
        return (self.size,)

    def parse(self, data):
        # Sanity check on the arguments.
        assert isinstance(data, maybe(unicode))
        # `None` represents `NULL` both in literal and native format.
        if data is None:
            return None
        # Expect an integer value in base 10.
        try:
            value = int(data, 10)
        except ValueError:
            raise ValueError("invalid integer literal: expected an integer"
                             " in a decimal format; got %r"
                             % data.encode('utf-8'))
        return value

    def dump(self, value):
        # Sanity check on the arguments.
        assert isinstance(value, maybe(oneof(int, long)))
        # `None` represents `NULL` both in literal and native format.
        if value is None:
            return None
        # Represent an integer value as a decimal number.
        return unicode(value)


class FloatDomain(NumberDomain):
    """
    Represents an IEEE 754 float data type.

    Valid literal values: floating-point numbers in decimal or scientific
    format.

    Valid native values: `float` objects.

    `size` (an integer or ``None``)
        Number of bits used to store a value; ``None`` if not known.
    """

    family = 'float'
    is_exact = False
    radix = 2

    def __init__(self, size=None):
        # Sanity check on the arguments.
        assert isinstance(size, maybe(int))
        self.size = size

    def __basis__(self):
        return (self.size,)

    def parse(self, data):
        # Sanity check on the argument.
        assert isinstance(data, maybe(unicode))
        # `None` represents `NULL` both in literal and native format.
        if data is None:
            return None
        # Parse the numeric value.
        try:
            value = float(data)
        except ValueError:
            raise ValueError("invalid float literal: %s"
                             % data.encode('utf-8'))
        # Check if we got a finite number.
        if math.isinf(value) or math.isnan(value):
            raise ValueError("invalid float literal: %s" % value)
        return value

    def dump(self, value):
        # Sanity check on the argument.
        assert isinstance(value, maybe(float))
        # `None` represents `NULL` both in literal and native format.
        if value is None:
            return None
        # Use `repr` to avoid loss of precision.
        return unicode(repr(value))


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

    family = 'decimal'
    is_exact = True
    radix = 10

    def __init__(self, precision=None, scale=None):
        # Sanity check on the arguments.
        assert isinstance(precision, maybe(int))
        assert isinstance(scale, maybe(int))
        self.precision = precision
        self.scale = scale

    def __basis__(self):
        return (self.precision, self.scale)

    def parse(self, data):
        # Sanity check on the arguments.
        assert isinstance(data, maybe(unicode))
        # `None` represents `NULL` both in literal and native format.
        if data is None:
            return None
        # Parse the literal (NB: it handles `inf` and `nan` values too).
        try:
            value = decimal.Decimal(data)
        except decimal.InvalidOperation:
            raise ValueError("invalid decimal literal: %s"
                             % data.encode('utf-8'))
        # Verify that we got a finite number.
        if not value.is_finite():
            raise ValueError("invalid decimal literal: %s"
                             % data.encode('utf-8'))
        return value

    def dump(self, value):
        # Sanity check on the argument.
        assert isinstance(value, maybe(decimal.Decimal))
        # `None` represents `NULL` both in literal and native format.
        if value is None:
            return None
        # Handle `inf` and `nan` values.
        if value.is_nan():
            return u'nan'
        elif value.is_infinite() and value > 0:
            return u'inf'
        elif value.is_infinite() and value < 0:
            return u'-inf'
        # Produce a decimal representation of the number.
        return unicode(value)


class StringDomain(Domain):
    """
    Represents a string data type.

    Valid literal values: all literal values.

    Valid native values: `unicode` objects; the `NUL` character is not allowed.

    `length` (an integer or ``None``)
        The maximum length of the value; ``None`` if infinite or not known.

    `is_varying` (Boolean)
        Indicates whether values are fixed-length or variable-length.
    """
    family = 'string'

    def __init__(self, length=None, is_varying=True):
        # Sanity check on the arguments.
        assert isinstance(length, maybe(int))
        assert isinstance(is_varying, bool)
        self.length = length
        self.is_varying = is_varying

    def __basis__(self):
        return (self.length, self.is_varying)

    def parse(self, data):
        # Sanity check on the argument.
        assert isinstance(data, maybe(unicode))
        # `None` represents `NULL` both in literal and native format.
        if data is None:
            return None
        # No conversion is required for string values.
        return data

    def dump(self, value):
        # Sanity check on the argument.
        assert isinstance(value, maybe(unicode))
        if value is not None:
            assert u'\0' not in value
        # `None` represents `NULL` both in literal and native format.
        if value is None:
            return None
        # No conversion is required for string values.
        return value


class EnumDomain(Domain):
    """
    Represents an enumeration data type.

    An enumeration domain has a predefined set of valid string values.

    `labels` (a list of Unicode strings)
        List of valid values.
    """
    family = 'enum'

    def __init__(self, labels):
        assert isinstance(labels, listof(unicode))
        self.labels = labels

    def __basis__(self):
        return (tuple(self.labels),)

    def parse(self, data):
        # Sanity check on the argument.
        assert isinstance(data, maybe(unicode))
        # `None` represents `NULL` both in literal and native format.
        if data is None:
            return None
        # Check if the value belongs to the fixed list of valid values.
        if data not in self.labels:
            raise ValueError("invalid enum literal: expected one of %s; got %r"
                             % (", ".join(repr(label.encode('utf-8'))
                                          for label in self.labels),
                                data.encode('utf-8')))
        # No conversion is required.
        return data

    def dump(self, value):
        # Sanity check on the argument.
        assert isinstance(value, maybe(unicode))
        if value is not None:
            assert value in self.labels
        # `None` represents `NULL` both in literal and native format.
        if value is None:
            return None
        # No conversion is required.
        return value


class DateDomain(Domain):
    """
    Represents a date data type.

    Valid literal values: valid date values in the form `YYYY-MM-DD`.

    Valid native values: `datetime.date` objects.
    """
    family = 'date'

    # Regular expression to match YYYY-MM-DD.
    pattern = r'''(?x)
        ^ \s*
        (?P<year> \d{4} )
        - (?P<month> \d{2} )
        - (?P<day> \d{2} )
        \s* $
    '''
    regexp = re.compile(pattern)

    def parse(self, data):
        # Sanity check on the argument.
        assert isinstance(data, maybe(unicode))
        # `None` represents `NULL` both in literal and native format.
        if data is None:
            return None
        # Parse `data` as YYYY-MM-DD.
        match = self.regexp.match(data)
        if match is None:
            raise ValueError("invalid date literal: expected a valid date"
                             " in a 'YYYY-MM-DD' format; got %r"
                             % data.encode('utf-8'))
        year = int(match.group('year'))
        month = int(match.group('month'))
        day = int(match.group('day'))
        # Generate a `datetime.date` value; may fail if the date is not valid.
        try:
            value = datetime.date(year, month, day)
        except ValueError, exc:
            raise ValueError("invalid date literal: %s" % exc.args[0])
        return value

    def dump(self, value):
        # Sanity check on the argument.
        assert isinstance(value, maybe(datetime.date))
        # `None` represents `NULL` both in literal and native format.
        if value is None:
            return None
        # `unicode` on `datetime.date` gives us the date in YYYY-MM-DD format.
        return unicode(value)


class TimeDomain(Domain):
    """
    Represents a time data type.

    Valid literal values: valid time values in the form `HH:MM[:SS[.SSSSSS]]`.

    Valid native values: `datetime.time` objects.
    """
    family = 'time'

    # Regular expression to match HH:MM:SS.SSSSSS.
    pattern = r'''(?x)
        ^ \s*
        (?P<hour> \d{1,2} )
        : (?P<minute> \d{2} )
        (?: : (?P<second> \d{2} )
            (?: \. (?P<microsecond> \d+ ) )? )?
        \s* $
    '''
    regexp = re.compile(pattern)

    def parse(self, data):
        # Sanity check on the argument.
        assert isinstance(data, maybe(unicode))
        # `None` represents `NULL` both in literal and native format.
        if data is None:
            return None
        # Parse `data` as HH:MM:SS.SSS.
        match = self.regexp.match(data)
        if match is None:
            raise ValueError("invalid time literal: expected a valid time"
                             " in a 'HH:SS:MM.SSSSSS' format; got %r"
                             % data.encode('utf-8'))
        hour = int(match.group('hour'))
        minute = int(match.group('minute'))
        second = match.group('second')
        if second is not None:
            second = int(second)
        else:
            second = 0
        microsecond = match.group('microsecond')
        if microsecond is not None:
            if len(microsecond) < 6:
                microsecond += '0'*(6-len(microsecond))
            microsecond = microsecond[:6]
            microsecond = int(microsecond)
        else:
            microsecond = 0
        # Generate a `datetime.time` value; may fail if the time is not valid.
        try:
            value = datetime.time(hour, minute, second, microsecond)
        except ValueError, exc:
            raise ValueError("invalid time literal: %s" % exc.args[0])
        return value

    def dump(self, value):
        # Sanity check on the argument.
        assert isinstance(value, maybe(datetime.time))
        # `None` represents `NULL` both in literal and native format.
        if value is None:
            return None
        # `unicode` on `datetime.date` gives us the date in HH:MM:SS.SSSSSS
        # format.
        return unicode(value)


class DateTimeDomain(Domain):
    """
    Represents a date and time data type.

    Valid literal values: valid date and time values in the form
    `YYYY-MM-DD HH:MM[:SS[.SSSSSS]]`.

    Valid native values: `datetime.datetime` objects.
    """
    family = 'datetime'

    # Regular expression to match YYYY-MM-DD HH:MM:SS.SSSSSS.
    pattern = r'''(?x)
        ^ \s*
        (?P<year> \d{4} )
        - (?P<month> \d{2} )
        - (?P<day> \d{2} )
        (?:
            (?: \s+ | [tT] )
            (?P<hour> \d{1,2} )
            : (?P<minute> \d{2} )
            (?: : (?P<second> \d{2} )
                (?: \. (?P<microsecond> \d+ ) )? )?
        )?
        (?:
          \s*
          (?: (?P<tz_utc> Z ) |
              (?P<tz_sign> [+-] )
              (?P<tz_hour> \d{1,2} )
              (?: :?
                  (?P<tz_minute> \d{2} )
              )? )
        )?
        \s* $
    '''
    regexp = re.compile(pattern)

    def parse(self, data):
        # Sanity check on the argument.
        assert isinstance(data, maybe(unicode))
        # `None` represents `NULL` both in literal and native format.
        if data is None:
            return None
        # Parse `data` as YYYY-DD-MM HH:MM:SS.SSSSSS.
        match = self.regexp.match(data)
        if match is None:
            raise ValueError("invalid datetime literal: expected a valid"
                             " date/time in a 'YYYY-MM-DD HH:SS:MM.SSSSSS'"
                             " format; got %r" % data.encode('utf-8'))
        year = int(match.group('year'))
        month = int(match.group('month'))
        day = int(match.group('day'))
        hour = match.group('hour')
        hour = int(hour) if hour is not None else 0
        minute = match.group('minute')
        minute = int(minute) if minute is not None else 0
        second = match.group('second')
        second = int(second) if second is not None else 0
        microsecond = match.group('microsecond')
        if microsecond is not None:
            if len(microsecond) < 6:
                microsecond += '0'*(6-len(microsecond))
            microsecond = microsecond[:6]
            microsecond = int(microsecond)
        else:
            microsecond = 0
        tz_utc = match.group('tz_utc')
        tz_sign = match.group('tz_sign')
        tz_hour = match.group('tz_hour')
        tz_minute = match.group('tz_minute')
        if tz_utc is not None:
            tz = UTC()
        elif tz_sign is not None:
            tz_hour = int(tz_hour)
            tz_minute = int(tz_minute) if tz_minute is not None else 0
            offset = tz_hour*60+tz_minute
            if tz_sign == '-':
                offset = -offset
            tz = FixedTZ(offset)
        else:
            tz = None
        # Generate a `datetime.datetime` value; may fail if the value is
        # invalid.
        try:
            value = datetime.datetime(year, month, day, hour, minute, second,
                                      microsecond, tz)
        except ValueError, exc:
            raise ValueError("invalid datetime literal: %s" % exc.args[0])
        return value

    def dump(self, value):
        # Sanity check on the argument.
        assert isinstance(value, maybe(datetime.datetime))
        # `None` represents `NULL` both in literal and native format.
        if value is None:
            return None
        # `unicode` on `datetime.datetime` gives us the value in ISO format.
        return unicode(value)


class OpaqueDomain(Domain):
    """
    Represents an unsupported SQL data type.

    Note: this is the only SQL domain with values that cannot be serialized
    using :meth:`dump`.
    """
    family = 'opaque'


