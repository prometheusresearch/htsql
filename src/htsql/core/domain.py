#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from .util import (maybe, oneof, listof, Clonable, Hashable, Printable,
        TextBuffer, to_literal, urlquote, isfinite)
import re
import decimal
import datetime
import weakref
import keyword
import operator


class DomainMeta(type):
    # Make sure `str(domain.__class__)` produces a usable domain family name.

    def __str__(cls):
        name = cls.__name__.lower()
        if name != 'domain' and name.endswith('domain'):
            name = name[:-len('domain')]
        return name


class Domain(Clonable, Hashable, Printable, metaclass=DomainMeta):
    """
    An HTSQL data type.

    A domain specifies the type of a value.  Most HTSQL domains correspond to
    SQL data types, others represent HTSQL containers (record, list) or
    used only in special circumstances.

    A value of a specific domain could be represented in two forms:

    - as an HTSQL literal (``text``);

    - as a native Python object (``data``).

    Methods :meth:`parse` and :meth:`dump` translate values from one form
    to the other.
    """

    __slots__ = ()

    def __init__(self):
        # Required by `Clonable` interface.
        pass

    def __basis__(self):
        return ()

    @staticmethod
    def parse(text):
        """
        Converts an HTSQL literal to a native Python object.

        Raises :exc:`ValueError` if the literal is not in a valid format.

        `text`: ``unicode`` or ``None``
            An HTSQL literal representing a value of the given domain.

        *Returns*
            A native Python object representing the same value.
        """
        assert isinstance(text, maybe(str))

        # `None` values are passed through.
        if text is None:
            return None
        # By default, a domain has no valid literals; subclasses should
        # override this method.
        raise ValueError("invalid literal")

    @staticmethod
    def dump(data):
        """
        Converts a native Python object to an HTSQL literal.

        `data`: (acceptable types depend on the domain)
            A native Python object representing a value of the given domain.

        *Returns*: ``unicode`` or ``None``
            An HTSQL literal representing the same value.
        """
        # By default, we do not accept any values except `None`; subclasses
        # should override this method.
        assert data is None
        return None

    def __str__(self):
        # The class name with `Domain` suffix stripped, in lower case.
        return str(self.__class__)


#
# Value representation.
#


class Value(Clonable, Printable):
    """
    Represents data and its type.

    `domain`: :class:`Domain`
        The data type.

    `data`
        The data value.

    Instances of :class:`Value` are iterable and permit ``len()``
    operator when the data type is :class:`ListDomain`.

    In Boolean context, an instance of :class:`Value` evaluates
    to ``False`` if and only if ``data`` is ``None``.
    """

    def __init__(self, domain, data):
        assert isinstance(domain, Domain)
        self.domain = domain
        self.data = data

    def __str__(self):
        # Dump:
        #   'text': domain

        # Convert to literal form and wrap with quotes when appropriate.
        text = ContainerDomain.dump_entry(self.data, self.domain)
        # Make sure the output is printable.
        text = urlquote(text, "")
        return text

    def __iter__(self):
        if not (isinstance(self.domain, ListDomain) and self.data is not None):
            raise TypeError("not a list value")
        return iter(self.data)

    def __len__(self):
        if not (isinstance(self.domain, ListDomain) and self.data is not None):
            raise TypeError("not a list value")
        return len(self.data)

    def __getitem__(self, key):
        if not (isinstance(self.domain, ListDomain) and self.data is not None):
            raise TypeError("not a list value")
        return self.data[key]

    def __bool__(self):
        return bool(self.data)


class Profile(Clonable, Printable):
    """
    Describes the structure of data.

    `domain`: :class:`Domain`
        The data type.

    `attributes`
        Extra stuctural metadata.  Each entry of ``attributes`` becomes
        an attribute of the :class:`Profile` instance.
    """

    def __init__(self, domain, **attributes):
        assert isinstance(domain, Domain)
        self.domain = domain
        for key in attributes:
            setattr(self, key, attributes[key])
        self.attributes = attributes

    def __str__(self):
        return str(self.domain)


class Product(Value):
    """
    Represents data and associated metadata.

    `meta`: :class:`Profile`
        Structure of the data.

    `data`
        The data value.

    `attributes`
        Extra runtime metadata.  Each entry of ``attributes``
        becomes an attribute of the :class:`Product` instance.
    """

    def __init__(self, meta, data, **attributes):
        assert isinstance(meta, Profile)
        super(Product, self).__init__(meta.domain, data)
        self.meta = meta
        for key in attributes:
            setattr(self, key, attributes[key])
        self.attributes = attributes


#
# Domains with no values.
#


class NullDomain(Domain):
    """
    A domain with no values (except ``null``).

    This is an abstract class.
    """

    __slots__ = ()


class VoidDomain(NullDomain):
    """
    A domain without any valid values.

    This domain is could be used when a domain object is required structurally,
    but has no semantics.
    """

    __slots__ = ()


class EntityDomain(NullDomain):
    """
    The type of class entities.

    Since class entities are not observable directly in HTSQL model,
    this domain does not support any values.
    """

    __slots__ = ()


#
# Scalar domains.
#


class UntypedDomain(Domain):
    """
    Represents a yet undetermined type.

    This domain is assigned to HTSQL literals temporarily until the actual
    domain could be derived from the context.
    """

    __slots__ = ()

    @staticmethod
    def parse(text):
        # Sanity check on the argument.
        assert isinstance(text, maybe(str))
        # No conversion is required.
        return text

    @staticmethod
    def dump(data):
        # Sanity check on the argument.
        assert isinstance(data, maybe(str))
        # No conversion is required.
        return data


class BooleanDomain(Domain):
    """
    A Boolean data type.

    Valid literals: ``'true'``, ``'false'``.

    Valid native objects: ``bool`` values.
    """

    __slots__ = ()

    @staticmethod
    def parse(text):
        assert isinstance(text, maybe(str))

        # `None` -> `None`, `'true'` -> `True`, `'false'` -> `False`.
        if text is None:
            return None
        if text == 'true':
            return True
        if text == 'false':
            return False
        raise ValueError("invalid Boolean literal: expected 'true' or 'false';"
                         " got %r" % text)

    @staticmethod
    def dump(data):
        assert isinstance(data, maybe(bool))

        # `None` -> `None`, `True` -> `'true'`, `False` -> `'false'`.
        if data is None:
            return None
        if data is True:
            return 'true'
        if data is False:
            return 'false'


class NumberDomain(Domain):
    """
    A numeric data type.

    This is an abstract superclass for integer, float and decimal numbers.

    Class attributes:

    `is_exact`: ``bool``
        Indicates whether the domain represents exact values.

    `radix`: ``2`` or ``10``
        Indicates whether the values are stored in binary or decimal form.
    """

    __slots__ = ()

    is_exact = None
    radix = None


class IntegerDomain(NumberDomain):
    """
    A binary integer data type.

    Valid literals: integers (in base 10) with an optional sign.

    Valid native objects: ``int`` or ``long`` values.

    `size`: ``int`` or ``None``
        Number of bits used to store a value; ``None`` if not known.
    """

    __slots__ = ('size',)

    is_exact = True
    radix = 2

    def __init__(self, size=None):
        assert isinstance(size, maybe(int))
        self.size = size

    def __basis__(self):
        return (self.size,)

    @staticmethod
    def parse(text):
        assert isinstance(text, maybe(str))
        # `null` is represented by `None` in both forms.
        if text is None:
            return None
        # Expect an integer value in base 10.
        try:
            data = int(text, 10)
        except ValueError:
            raise ValueError("invalid integer literal: expected an integer"
                             " in a decimal format; got %r"
                             % text)
        return data

    def dump(self, data):
        assert isinstance(data, maybe(oneof(int, int)))
        # `null` is represented by `None` in both forms.
        if data is None:
            return None
        # Represent the value as a decimal number.
        return str(data)


class FloatDomain(NumberDomain):
    """
    An IEEE 754 float data type.

    Valid literals: floating-point numbers in decimal or exponential format.

    Valid native objects: ``float`` values.

    `size`: ``int`` or ``None``
        Number of bits used to store a value; ``None`` if not known.
    """

    __slots__ = ('size',)

    is_exact = False
    radix = 2

    def __init__(self, size=None):
        assert isinstance(size, maybe(int))
        self.size = size

    def __basis__(self):
        return (self.size,)

    @staticmethod
    def parse(text):
        assert isinstance(text, maybe(str))
        # `None` represents `null` both in literal and native formats.
        if text is None:
            return None
        # Parse the numeric data.
        try:
            data = float(text)
        except ValueError:
            raise ValueError("invalid float literal: %s"
                             % text)
        # Check if we got a finite number.
        if not isfinite(data):
            raise ValueError("invalid float literal: %s" % data)
        return data

    def dump(self, data):
        assert isinstance(data, maybe(float))
        # `None` represents `null` both in literal and native format.
        if data is None:
            return None
        # Check that we got a real number.
        assert isfinite(data)
        # Use `repr` to avoid loss of precision.
        return str(repr(data))


class DecimalDomain(NumberDomain):
    """
    An exact decimal data type.

    Valid literals: floating-point numbers in decimal or exponential format.

    Valid native objects: ``decimal.Decimal`` objects.

    `precision`: ``int`` or ``None``
        Number of significant digits; ``None`` if infinite or not known.

    `scale`: ``int`` or ``None``
        Number of significant digits in the fractional part; zero for integers,
        ``None`` if infinite or not known.
    """

    __slots__ = ('precision', 'scale')

    is_exact = True
    radix = 10

    def __init__(self, precision=None, scale=None):
        assert isinstance(precision, maybe(int))
        assert isinstance(scale, maybe(int))
        self.precision = precision
        self.scale = scale

    def __basis__(self):
        return (self.precision, self.scale)

    @staticmethod
    def parse(text):
        assert isinstance(text, maybe(str))
        # `None` represents `NULL` both in literal and native format.
        if text is None:
            return None
        # Parse the literal (NB: accepts `inf` and `nan` too).
        try:
            data = decimal.Decimal(text)
        except decimal.InvalidOperation:
            raise ValueError("invalid decimal literal: %s"
                             % text)
        # Verify that we got a finite number.
        if not isfinite(data):
            raise ValueError("invalid decimal literal: %s"
                             % text)
        return data

    @staticmethod
    def dump(data):
        assert isinstance(data, maybe(decimal.Decimal))
        # `None` represents `NULL` both in literal and native format.
        if data is None:
            return None
        # Check that we got a real number.
        assert isfinite(data)
        # Produce a decimal representation of the number.
        return str(data)


class TextDomain(Domain):
    """
    A text data type.

    Valid literals: any.

    Valid native object: `unicode` values; the `NUL` character is not allowed.

    `length`: ``int`` or ``None``
        The maximum length of the value; ``None`` if infinite or not known.

    `is_varying`: ``bool``
        Indicates whether values are fixed-length or variable-length.
    """

    __slots__ = ('length', 'is_varying')

    def __init__(self, length=None, is_varying=True):
        assert isinstance(length, maybe(int))
        assert isinstance(is_varying, bool)
        self.length = length
        self.is_varying = is_varying

    def __basis__(self):
        return (self.length, self.is_varying)

    @staticmethod
    def parse(text):
        assert isinstance(text, maybe(str))
        # `None` represents `null` both in literal and native format.
        if text is None:
            return None
        # No conversion is required for text values.
        return text

    @staticmethod
    def dump(data):
        assert isinstance(data, maybe(str))
        if data is not None:
            assert '\0' not in data
        # `None` represents `null` both in literal and native format.
        if data is None:
            return None
        # No conversion is required for string values.
        return data


class EnumDomain(Domain):
    """
    An enumeration data type.

    An enumeration domain has a predefined finite set of valid text values.

    `labels`: [``unicode``]
        List of valid values.
    """

    __slots__ = ('labels',)

    # NOTE: HTSQL enum type is structural, but some SQL databases implement
    # enums as nominal types (e.g. PostgreSQL).  In practice, it should not be
    # a problem since it is unlikely that two nominally different enum types
    # would have the same set of labels.

    def __init__(self, labels):
        assert isinstance(labels, listof(str))
        self.labels = labels

    def __basis__(self):
        return (tuple(self.labels),)

    def parse(self, text):
        assert isinstance(text, maybe(str))
        # `None` represents `null` both in literal and native format.
        if text is None:
            return None
        # Check if the input belongs to the fixed list of valid values.
        if text not in self.labels:
            raise ValueError("invalid enum literal: expected one of %s; got %r"
                             % (", ".join(repr(label)
                                          for label in self.labels),
                                text))
        # No conversion is required.
        return text

    def dump(self, data):
        assert isinstance(data, maybe(str))
        if data is not None:
            assert data in self.labels
        # `None` represents `NULL` both in literal and native format.
        if data is None:
            return None
        # No conversion is required.
        return data

    def __str__(self):
        # enum('label', ...)
        return "%s(%s)" % (self.__class__,
                            ", ".join(to_literal(label)
                                       for label in self.labels))


#
# Date/time domains.
#


class UTC(datetime.tzinfo):
    # The UTC timezone.

    def utcoffset(self, dt):
        return datetime.timedelta(0)

    def dst(self, dt):
        return datetime.timedelta(0)

    def tzname(self, dt):
        return "Z"


class FixedTZ(datetime.tzinfo):
    # A timezone with a fixed offset.

    def __init__(self, offset):
        self.offset = offset    # in minutes

    def utcoffset(self, dt):
        return datetime.timedelta(minutes=self.offset)

    def dst(self, dt):
        return datetime.timedelta(0)

    def tzname(self, dt):
        hour = abs(self.offset) / 60
        minute = abs(self.offset) % 60
        sign = '+'
        if self.offset < 0:
            sign = '-'
        if minute:
            return "%s%02d:%02d" % (sign, hour, minute)
        else:
            return "%s%d" % (sign, hour)


class DateDomain(Domain):
    """
    A date data type.

    Valid literals: valid dates in the form ``YYYY-MM-DD``.

    Valid native objects: ``datetime.date`` values.
    """

    __slots__ = ()

    # Regular expression to match YYYY-MM-DD.
    regexp = re.compile(r'''(?x)
        ^ \s*
        (?P<year> \d{4} )
        - (?P<month> \d{2} )
        - (?P<day> \d{2} )
        \s* $
    ''')

    @staticmethod
    def parse(text, regexp=regexp):
        assert isinstance(text, maybe(str))
        # `None` represents `null` both in literal and native format.
        if text is None:
            return None
        # Parse `text` as YYYY-MM-DD.
        match = regexp.match(text)
        if match is None:
            raise ValueError("invalid date literal: expected a valid date"
                             " in a 'YYYY-MM-DD' format; got %r"
                             % text)
        year = int(match.group('year'))
        month = int(match.group('month'))
        day = int(match.group('day'))
        # Generate a `datetime.date` value; may fail if the date is not valid.
        try:
            data = datetime.date(year, month, day)
        except ValueError as exc:
            raise ValueError("invalid date literal: %s" % exc.args[0])
        return data

    @staticmethod
    def dump(data):
        assert isinstance(data, maybe(datetime.date))
        # `None` represents `null` both in literal and native format.
        if data is None:
            return None
        # `unicode` on `datetime.date` gives us the date in YYYY-MM-DD format.
        return str(data)


class TimeDomain(Domain):
    """
    A time data type.

    Valid literals: valid time values in the form ``HH:MM[:SS[.SSSSSS]]``.

    Valid native objects: ``datetime.time`` values.
    """

    __slots__ = ()

    # Regular expression to match HH:MM:SS.SSSSSS.
    regexp = re.compile(r'''(?x)
        ^ \s*
        (?P<hour> \d{1,2} )
        : (?P<minute> \d{2} )
        (?: : (?P<second> \d{2} )
            (?: \. (?P<microsecond> \d+ ) )? )?
        \s* $
    ''')

    @staticmethod
    def parse(text, regexp=regexp):
        assert isinstance(text, maybe(str))
        # `None` represents `null` both in literal and native format.
        if text is None:
            return None
        # Parse `text` as HH:MM:SS.SSS.
        match = regexp.match(text)
        if match is None:
            raise ValueError("invalid time literal: expected a valid time"
                             " in a 'HH:SS:MM.SSSSSS' format; got %r"
                             % text)
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
            data = datetime.time(hour, minute, second, microsecond)
        except ValueError as exc:
            raise ValueError("invalid time literal: %s" % exc.args[0])
        return data

    @staticmethod
    def dump(data):
        assert isinstance(data, maybe(datetime.time))
        # `None` represents `null` both in literal and native format.
        if data is None:
            return None
        # `unicode` on `datetime.date` gives us the date in HH:MM:SS.SSSSSS
        # format.
        return str(data)


class DateTimeDomain(Domain):
    """
    A date+time data type.

    Valid literals: valid date+time values in the form
    ``YYYY-MM-DD HH:MM[:SS[.SSSSSS]]``.

    Valid native objects: ``datetime.datetime`` values.
    """

    __slots__ = ()

    # Regular expression to match YYYY-MM-DD HH:MM:SS.SSSSSS.
    regexp = re.compile(r'''(?x)
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
    ''')

    @staticmethod
    def parse(text, regexp=regexp):
        assert isinstance(text, maybe(str))
        # `None` represents `null` both in literal and native format.
        if text is None:
            return None
        # Parse `text` as YYYY-DD-MM HH:MM:SS.SSSSSS.
        match = regexp.match(text)
        if match is None:
            raise ValueError("invalid datetime literal: expected a valid"
                             " date/time in a 'YYYY-MM-DD HH:SS:MM.SSSSSS'"
                             " format; got %r" % text)
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
        # Generate a `datetime.datetime` value; may fail if the input is
        # invalid.
        try:
            data = datetime.datetime(year, month, day, hour, minute, second,
                                      microsecond, tz)
        except ValueError as exc:
            raise ValueError("invalid datetime literal: %s" % exc.args[0])
        return data

    @staticmethod
    def dump(data):
        assert isinstance(data, maybe(datetime.datetime))
        # `None` represents `null` both in literal and native format.
        if data is None:
            return None
        # `unicode` on `datetime.datetime` gives us the value in ISO format.
        return str(data)


class OpaqueDomain(Domain):
    """
    An unsupported SQL data type.

    This domain is used for SQL data types not supported by HTSQL.

    Valid literals: any.

    Valid native objects: any.
    """

    __slots__ = ()

    @staticmethod
    def parse(text):
        assert isinstance(text, maybe(str))
        # We do not know what to do with the input, so pass it through and
        # hope for the best.
        return text

    @staticmethod
    def dump(data, regexp=re.compile(r"[\0-\x1F\x7F]")):
        # `None` represents `null` both in literal and native format.
        if data is None:
            return None
        # Try to produce a passable textual representation; no guarantee
        # it could be given back to the database.
        text = str(data)
        # Make sure the output is printable.
        if regexp.search(text) is not None:
            text = re.escape(text)
        return text


#
# Containers.
#


class Record(tuple):
    """
    A record value.

    :class:`Record` is implemented as a tuple with named fields.
    """

    # Forbid dynamic attributes.
    __slots__ = ()
    # List of field names (`None` when the field has no name).
    __fields__ = ()

    @classmethod
    def make(cls, name, fields, _cache=weakref.WeakValueDictionary()):
        """
        Generates a :class:`Record` subclass with the given fields.

        `name`: ``str``, ``unicode`` or ``None``.
            The name of the new class.

        `fields`: list of ``str``, ``unicode`` or ``None``.
            List of desired field names (``None`` for a field to have no name).

            A field may get no or another name assigned if the desired field
            name is not available for some reason; e.g., if it is is already
            taked by another field or if it coincides with a Python keyword.

        *Returns*: subclass of :class:`Record`
            The generated class.
        """
        assert isinstance(name, maybe(oneof(str, str)))
        assert isinstance(fields, listof(maybe(oneof(str, str))))

        # Check if the type has been generated already.
        cache_key = (name, tuple(fields))
        try:
            return _cache[cache_key]
        except KeyError:
            pass
        # Check if the name is a valid identifier.
        if name is not None and not re.match(r'\A(?!\d)\w+\Z', name):
            name = None
        # If the name is a Python keyword, prepend it with `_`.
        if name is not None and keyword.iskeyword(name):
            name = name+'_'
        # If the name is not given or not available, use `'Record'`.
        if name is None:
            name = cls.__name__

        # Names already taken.
        seen = set()
        # Process all field names.
        for idx, field in enumerate(fields):
            if field is None:
                continue
            # Only permit valid identifiers.
            if not re.match(r'\A(?!\d)\w+\Z', field):
                field = None
            # Do not allow special names (starting with `__`).
            elif field.startswith('__'):
                field = None
            else:
                # Python keywords are prefixed with `_`.
                if keyword.iskeyword(field):
                    field = field+'_'
                # Skip names already taken.
                if field in seen:
                    field = None
                # Store the normalized name.
                fields[idx] = field
                seen.add(field)

        # Prepare the class namespace and generate the class.
        bases = (cls,)
        content = {}
        content['__slots__'] = ()
        content['__fields__'] = tuple(fields)
        # For each named field, add a respective descriptor to the class
        # namespace.
        for idx, field in enumerate(fields):
            if field is None:
                continue
            content[field] = property(operator.itemgetter(idx))
        # Generate and return the new class.
        record_class = type(name, bases, content)
        _cache[cache_key] = record_class
        return record_class

    def __repr__(self):
        # Dump:
        #   record_name(field_name=..., [N]=...)
        return ("%s(%s)"
                % (self.__class__.__name__,
                   ", ".join("%s=%r" % (name or '[%s]' % idx, value)
                             for idx, (name, value)
                                in enumerate(zip(self.__fields__, self)))))

    def __getnewargs__(self):
        # Pickle serialization.
        return tuple(self)


class EntryBuffer(TextBuffer):
    # Parser for container literals.

    # Disable automatic whitespace recognition.
    skip_regexp = None

    def pull_entries(self, left, right):
        # Parse a container literal with the given brackets.

        # Container entries.
        entries = []
        # Skip whitespace.
        self.pull(r"[\s]+")
        # Get the left bracket.
        if self.pull(left) is None:
            raise self.fail()
        # Skip whitespace.
        self.pull(r"[\s]+")
        # Until we pull the right bracket.
        while self.pull(right) is None:
            # Pull a [,] separator.
            if entries:
                if not self.pull(r"[,]"):
                    raise self.fail()
                self.pull(r"[\s]+")
            # Permit orphan [,] followed by the right bracket.
            if not self.peek(right):
                # Try an atomic entry:
                #   `null`, `true`, `false`, an unquoted number or
                #   a quoted literal.
                block = self.pull(r" ['] (?: [^'\0] | [']['] )* ['] |"
                                  r" null | true | false |"
                                  r" [+-]? (?: \d+ (?: [.] \d* )? | [.] \d+ )"
                                  r" (?: [eE] [+-]? \d+ )?")
                if block is not None:
                    entries.append(block)
                else:
                    # Otherwise, must be a nested container.
                    chunks = self.pull_chunks()
                    entries.append("".join(chunks))
                # Skip whitespace.
                self.pull(r"[\s]+")
        # Skip trailing whitespace.
        self.pull(r"[\s]+")
        # Make sure no garbage after the closing bracket.
        if self:
            raise self.fail()

        return entries

    def pull_chunks(self):
        # Parse a nested container as a series of text blocks.
        chunks = []
        # Get the left bracket.
        left = self.pull(r"[({\[]")
        if left is None:
            raise self.fail()
        chunks.append(left)
        # Until we find the right bracket.
        while not self.peek(r"[)}\]]"):
            # Extract any unquoted characters.
            chunk = self.pull(r"[^(){}\[\]']+")
            if chunk is not None:
                chunks.append(chunk)
            # Extract a quoted literal.
            if self.peek(r"[']"):
                chunk = self.pull(r" ['] (?: [^'\0] | [']['] )* [']")
                if chunk is None:
                    raise self.fail()
                chunks.append(chunk)
            # Extract a nested container.
            elif self.peek(r"[({\["):
                chunks.extend(self.pull_chunks())
        # Pull the right bracket.
        if left == "(":
            right = self.pull(r"[)]")
        elif left == "{":
            right = self.pull(r"[}]")
        elif left == r"[":
            right = self.pull(r"[\]]")
        if right is None:
            raise self.fail()
        chunks.append(right)
        return chunks

    def fail(self):
        raise ValueError("ill-formed container literal: %s"
                         % self.text)


class ContainerDomain(Domain):
    """
    A container type.

    This is an abstract superclass for container domains.
    """

    __slots__ = ()

    @staticmethod
    def parse_entry(text, domain):
        # Unquotes and parses a container entry.

        # Unquote a quoted literal.
        if text[0] == text[-1] == "'":
            text = text[1:-1].replace("''", "'")
        # Verify that nested container entries are indeed containers.
        elif text[0] == "(" and text[-1] == ")":
            if not isinstance(domain, (ListDomain, UntypedDomain)):
                raise ValueError("list entry for %s: %s"
                                 % (domain, text))
        elif text[0] == "{" and text[-1] == "}":
            if not isinstance(domain, (RecordDomain, UntypedDomain)):
                raise ValueError("record entry for %s: %s"
                                 % (domain, text))
        elif text[0] == "[" and text[-1] == "]":
            if not isinstance(domain, (IdentityDomain, UntypedDomain)):
                raise ValueError("identity entry for %s: %s"
                                 % (domain, text))
        # Validate unquoted values.
        elif text == "true" or text == "false":
            if not isinstance(domain, (BooleanDomain, UntypedDomain)):
                raise ValueError("boolean entry for %s: %s"
                                 % (domain, text))
        elif text == "null":
            text = None
        else:
            # Must be an unquoted number.
            if not isinstance(domain, (NumberDomain, UntypedDomain)):
                raise ValueError("numeric entry for %s: %s"
                                 % (domain, text))
        # Parse the entry.
        return domain.parse(text)

    @staticmethod
    def dump_entry(data, domain):
        # Serializes an individual container entry.

        # Start with the regular literal.
        text = domain.dump(data)
        if text is None:
            # Using unquoted `null` string is safe here because a regular text
            # value will be quoted.
            return "null"
        elif isinstance(domain, (BooleanDomain, NumberDomain,
                                 ListDomain, RecordDomain)):
            # Boolean and numeric literals are safe because they
            # do not contain special characters.  Lists and records
            # are recognized by counting the brackets.
            return text
        elif isinstance(domain, IdentityDomain):
            # Identity values are wrapped with `[]`; the outer
            # brackets will be stripped by `IdentityDomain.parse()`.
            return "[%s]" % text
        else:
            # All the others are wrapped in single quotes.
            return "'%s'" % text.replace("'", "''")


class ListDomain(ContainerDomain):
    """
    A variable-size collection of homogenous entries.

    Valid literals: quoted entries, comma-separated and wrapped in
    ``(`` and ``)``.

    Valid native objects: ``list`` values.

    `item_domain`: :class:`Domain`
        The type of entries.
    """

    __slots__ = ('item_domain',)

    def __init__(self, item_domain):
        assert isinstance(item_domain, Domain)
        self.item_domain = item_domain

    def __basis__(self):
        return (self.item_domain,)

    def __str__(self):
        # list(item)
        return "%s(%s)" % (self.__class__, self.item_domain)

    def parse(self, text):
        assert isinstance(text, maybe(str))
        # `None` means `null` both in literal and native forms.
        if text is None:
            return None
        # Extract raw entries.
        buffer = EntryBuffer(text)
        entries = buffer.pull_entries(r"[(]", r"[)]")
        # Parse the entries.
        return [self.parse_entry(entry, self.item_domain) for entry in entries]

    def dump(self, data):
        assert isinstance(data, maybe(list))
        # `None` means `null` both in literal and native forms.
        if data is None:
            return None
        # Serialize individual entries and wrap with `()`.
        if len(data) == 1:
            return "(%s,)" % self.dump_entry(data[0], self.item_domain)
        return "(%s)" % ", ".join(self.dump_entry(entry, self.item_domain)
                                    for entry in data)


class RecordDomain(ContainerDomain):
    """
    A fixed-size collection of heterogenous entries.

    Valid literals: quoted entries, comma-separated and wrapped in
    ``{`` and ``}``.

    Valid native objects: ``tuple`` values.

    `fields`: [:class:`Profile`]
        The types and other structural metadata of the record fields.
    """

    __slots__ = ('fields',)

    def __init__(self, fields):
        assert isinstance(fields, listof(Profile))
        self.fields = fields
    def __basis__(self):
        basis = []
        for field in self.fields:
            basis.append(field.domain)
            basis.append(field.tag)
        return tuple(basis)

    def __str__(self):
        # record(field, ...)
        return "%s(%s)" % (self.__class__,
                            ", ".join(str(field)
                                      for field in self.fields))

    def parse(self, text):
        assert isinstance(text, maybe(str))
        # `None` means `null` both in literal and native forms.
        if text is None:
            return None
        # Extract raw entries.
        buffer = EntryBuffer(text)
        entries = buffer.pull_entries(r"[{]", r"[}]")
        # Verify that we got the correct number of them.
        if len(entries) < len(self.fields):
            raise ValueError("not enough fields: expected %s, got %s"
                             % (len(self.fields), len(entries)))
        if len(entries) > len(self.fields):
            raise ValueError("too many fields: expected %s, got %s"
                             % (len(self.fields), len(entries)))
        # Prepare the record constructor.
        field_tags = [getattr(field, 'tag') for field in self.fields]
        record_class = Record.make(None, field_tags)
        # Parse the entries and return them as a record.
        return record_class(self.parse_entry(entry, field.domain)
                            for field, entry in zip(self.fields, entries))

    def dump(self, data):
        assert isinstance(data, maybe(tuple))
        assert data is None or len(data) == len(self.fields)
        # `None` means `null` both in literal and native forms.
        if data is None:
            return None
        # Serialize individual fields and wrap with `{}`.
        return "{%s}" % ", ".join(self.dump_entry(entry, field.domain)
                                    for entry, field in zip(data, self.fields))


#
# Identity domain.
#


class ID(tuple):
    """
    An identity value.

    :class:`ID` is a tuple with a string representation that produces
    the identity value in literal form.
    """

    __slots__ = ()

    @classmethod
    def make(cls, dump, _cache=weakref.WeakValueDictionary()):
        """
        Generates a :class:`ID` subclass with the given string serialization.

        `dump`
            Implementation of ``unicode()`` operator.

        *Returns*: subclass of :class:`ID`
            The generated class.
        """
        # Check if the type was already generated.
        try:
            return _cache[dump]
        except KeyError:
            pass

        # Generate a subclass with custom `__str__` implementation.
        name = cls.__name__
        bases = (cls,)
        content = {}
        content['__slots__'] = ()
        content['__str__'] = (lambda self, dump=dump: dump(self))
        id_class = type(name, bases, content)
        # Cache and return the result.
        _cache[dump] = id_class
        return id_class

    def __repr__(self):
        # ID(...)
        return "%s(%s)" % (self.__class__.__name__,
                           ", ".join(repr(item) for item in self))

    def __getnewargs__(self):
        # Pickle serialization.
        return tuple(self)


class LabelGroup(list):
    # Represents a raw identity value; that is, not aligned with
    # the label structure.  Used for parsing identity literals.

    __slots__ = ('width',)

    def __init__(self, iterable):
        list.__init__(self, iterable)
        # Calculate the number of leaf labels.
        width = 0
        for item in self:
            if isinstance(item, LabelGroup):
                width += item.width
            else:
                width += 1
        self.width = width

    def __str__(self):
        # Serialize back to the literal form.
        return ".".join("(%s)" % item if isinstance(item, LabelGroup) else
                         str(item) if re.match(r"(?u)\A[\w-]+\Z", item) else
                         "'%s'" % item.replace("'", "''")
                         for item in self)


class LabelBuffer(TextBuffer):
    # Parser for identity literals.

    # Whitespace characters to strip.
    skip_regexp = re.compile(r"\s+")

    def pull_identity(self):
        # Parse `.`-separated labels.
        group = self.pull_label_group()
        # Make sure nothing left.
        if self:
            raise self.fail()
        # Unwrap outer `[]`.
        if len(group) == 1 and isinstance(group[0], LabelGroup):
            group = group[0]
        return group

    def pull_label_group(self):
        # Parse a group of `.`-separated labels.
        labels = [self.pull_label()]
        while self.pull(r"[.]") is not None:
            labels.append(self.pull_label())
        return LabelGroup(labels)

    def pull_label(self):
        # Parse unquoted, quoted and composite labels.
        block = self.pull(r"[\[(] | [\w-]+ | ['] (?: [^'\0] | [']['] )* [']")
        if block is None:
            raise self.fail()
        # Composite labels.
        if block == "[":
            label = self.pull_label_group()
            if self.pull(r"\]") is None:
                raise self.fail()
        elif block == "(":
            label = self.pull_label_group()
            if self.pull(r"\)") is None:
                raise self.fail()
        # Quoted labels.
        elif block[0] == block[-1] == "'":
            label = block[1:-1].replace("''", "'")
        # Unquoted labels.
        else:
            label = block
        return label

    def fail(self):
        raise ValueError("ill-formed identity literal: %s"
                         % self.text)


class IdentityDomain(ContainerDomain):
    """
    A unique identifier of a database entity.

    Valid literals: identity constructors as in HTSQL grammar; outer brackets
    are optional and always stripped.

    Valid native objects: ``tuple`` values.

    `labels`: [:class:`Domain`]
        The type of labels that form the identity value.

    `width`: ``int``
        The number of leaf labels.

    `leaves`: [[``int``]]
        Paths (as tuple indexes) to leaf labels.
    """

    __slots__ = ('labels', 'width', 'leaves')

    def __init__(self, labels):
        assert isinstance(labels, listof(Domain))
        self.labels = labels
        # Find the number of and the paths to leaf labels.
        self.width = 0
        self.leaves = []
        for idx, label in enumerate(labels):
            if isinstance(label, IdentityDomain):
                self.width += label.width
                for leaf in label.leaves:
                    self.leaves.append([idx]+leaf)
            else:
                self.width += 1
                self.leaves.append([idx])

    def __basis__(self):
        return (tuple(self.labels),)

    def __str__(self):
        # identity(label, ...)
        return "%s(%s)" % (self.__class__,
                            ", ".join(str(label)
                                       for label in self.labels))

    def parse(self, text):
        # Sanity check on the arguments.
        assert isinstance(text, maybe(str))
        # `None` represents `NULL` both in literal and native format.
        if text is None:
            return None
        # Parse a raw identity value.
        buffer = LabelBuffer(text)
        group = buffer.pull_label_group()
        if buffer:
            raise buffer.fail()
        # Make sure we got the right number of labels.
        if group.width < self.width:
            raise ValueError("not enough labels: expected %s, got %s"
                             % (self.width, group.width))
        if group.width > self.width:
            raise ValueError("too many labels: expected %s, got %s"
                             % (self.width, group.width))
        # Reconcile the raw entries with the identity structure.
        return self.align(group)

    def align(self, group):
        # Aligns a group of raw entries with the identity structure and
        # parses leaf entries.
        assert isinstance(group, LabelGroup)
        assert group.width == self.width
        # The position of the next entry to process.
        idx = 0
        # Processed entries.
        data = []
        # For each label in the identity.
        for label in self.labels:
            assert idx < len(group)
            # For a composite label, gather enough entries to fit the label
            # and align them with the nested identity.
            if isinstance(label, IdentityDomain):
                subgroup = []
                subwidth = 0
                while subwidth < label.width:
                    entry = group[idx]
                    idx += 1
                    subgroup.append(entry)
                    if isinstance(entry, LabelGroup):
                        subwidth += entry.width
                    else:
                        subwidth += 1
                if subwidth > label.width:
                    raise ValueError("misshapen %s: %s" % (self, group))
                # Unwrap a single composite entry; or wrap multiple entries
                # in a single group.
                if len(subgroup) == 1 and isinstance(subgroup[0], LabelGroup):
                    subgroup = subgroup[0]
                else:
                    subgroup = LabelGroup(subgroup)
                # Process a nested identity.
                data.append(label.align(subgroup))
            else:
                # Process a leaf label.
                entry = group[idx]
                idx += 1
                if isinstance(entry, LabelGroup):
                    raise ValueError("misshapen %s: %s" % (self, group))
                data.append(label.parse(entry))
        # Generate an `ID` instance.
        id_class = ID.make(self.dump)
        return id_class(data)

    def dump(self, data, regexp=re.compile(r'\A [\w-]+ \Z', re.X|re.U)):
        assert isinstance(data, maybe(tuple))
        # `None` means `null` both in literal and native forms.
        if data is None:
            return None
        # Sanity check on the value.  Note that a label value cannot be `None`.
        assert len(data) == len(self.labels)
        assert all(entry is not None for entry in data)
        # Serialize the labels.
        chunks = []
        is_simple = all(not isinstance(label, IdentityDomain)
                        for label in self.labels[1:])
        for entry, label in zip(data, self.labels):
            if isinstance(label, IdentityDomain):
                # Composite label values are generally wrapped with `()` unless
                # there is just one composite label and it is at the head,
                # or if the nested identity contains only one label.
                is_flattened = (is_simple or len(label.labels) == 1)
                chunk = label.dump(entry)
                if not is_flattened:
                    chunk = "(%s)" % chunk
            else:
                # Leaf labels are converted to literal form and quoted
                # if the literal contains a non-alphanumeric character.
                chunk = label.dump(entry)
                if regexp.match(chunk) is None:
                    chunk = "'%s'" % chunk.replace("'", "''")
            chunks.append(chunk)
        return ".".join(chunks)


