#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
This module provides utilities for data validation and conversion.
"""


from util import DB, maybe, oneof
import re


class Val(object):
    """
    Validators check if a given value conforms the specified format.

    A validator acts as function that takes a value, checks if it conforms
    the format, normalizes and returns the value.  Example::

        validator = IntVal()
        value = validator(value)

    If the value does not conform the format, :exc:`ValueError` is raised.

    Attribute `hint` gives a short textual description of the format.

    :class:`Val` is the base abstract class for validators.  Its subclasses
    provide validators for specific formats.

    To create a validator for a new format, create a subclass of :class:`Val`
    and override the :meth:`__call__()` method.  The method should accept
    values of any type.  If the value does not conform to the format,
    :exc:`ValueError` should be raised; otherwise the value should be
    normalized and returned.

    Example::

        class IntVal(Val):

            hint = "an integer"

            def __call__(self, value):
                if isinstance(value, str):
                    value = int(value)
                if not isinstance(value, int):
                    raise ValueError("an integer is expected")
                return value
    """

    hint = None

    def __call__(self, value):
        # Override when subclassing.
        raise NotImplementedError()

    def get_hint(self):
        # Override when the hint is not static.
        return self.hint


class AnyVal(Val):
    """
    A no-op validator.
    """

    hint = """an arbitrary value"""

    def __call__(self, value):
        # All values are passed through.
        return value


class StrVal(Val):
    """
    Verifies if the value is a UTF-8 encoded string.

    `pattern` (a regular expression or ``None``)
        Checks if the string matches the given regular expression.

    `is_nullable` (Boolean)
        If set, ``None`` values are permitted.
    """

    hint = """a string"""

    def __init__(self, pattern=None, is_nullable=False):
        # Sanity check on the arguments.
        assert isinstance(pattern, maybe(str))
        assert isinstance(is_nullable, bool)

        self.pattern = pattern
        self.is_nullable = is_nullable

    def __call__(self, value):
        # `None` is allowed if the `is_nullable` flag is set.
        if value is None:
            if self.is_nullable:
                return None
            else:
                raise ValueError("the null value is not permitted")

        # A byte or a Unicode string is expected.
        if not isinstance(value, (str, unicode)):
            raise ValueError("a string value is expected; got %r" % value)

        # Byte strings must be UTF-8 encoded.
        if isinstance(value, str):
            try:
                value.decode('utf-8')
            except UnicodeDecodeError, exc:
                raise ValueError("unable to decode %r: %s" % (value, exc))

        # Unicode strings are UTF-8 encoded.
        if isinstance(value, unicode):
            value = value.encode('utf-8')

        # Verify that the value does not contain the NUL character.
        if '\0' in value:
            raise ValueError("NUL character is not allowed; got %r" % value)

        # Check if the value matches the pattern.
        if self.pattern is not None:
            if not re.match(self.pattern, value):
                raise ValueError("%r does not match pattern %r"
                                 % (value, self.pattern))

        # We are done, return a string.
        assert isinstance(value, str)
        return value


class WordVal(Val):
    """
    Verifies if the value is a word.

    A word is a string containing alphanumeric characters, dashes,
    underscores, or spaces.  In the normalized form, underscores and
    spaces are replaced with dashes.

    `is_nullable` (Boolean)
        If set, ``None`` values are permitted.
    """

    hint = """a word"""

    # A regular expression for matching words.
    pattern = re.compile(r'^[0-9a-zA-Z_ -]+$')

    def __init__(self, is_nullable=False):
        # Sanity check on the arguments.
        assert isinstance(is_nullable, bool)

        self.is_nullable = is_nullable

    def __call__(self, value):
        # `None` is allowed if the `is_nullable` flag is set.
        if value is None:
            if self.is_nullable:
                return None
            else:
                raise ValueError("the null value is not permitted")

        # A byte or a Unicode string is expected.
        if not isinstance(value, (str, unicode)):
            raise ValueError("a string value is expected; got %r" % value)

        # Byte strings must be UTF-8 encoded.
        if isinstance(value, str):
            try:
                value.decode('utf-8')
            except UnicodeDecodeError, exc:
                raise ValueError("unable to decode %r: %s" % (value, exc))

        # Unicode strings are UTF-8 encoded.
        if isinstance(value, unicode):
            value = value.encode('utf-8')

        # Check if the string is a word.
        if not self.pattern.match(value):
            raise ValueError("a string containing alphanumeric characters,"
                             " dashes, underscores or spaces is expected;"
                             " got %r" % value)

        # Normalize and return the value.
        value = value.replace('_', '-').replace(' ', '-')
        return value



class BoolVal(Val):
    """
    Verifies if the value is Boolean.

    Besides ``True`` and ``False`` constants, the following values
    are accepted:

    * ``1``, ``'1'``, ``'true'`` (as ``True``);
    * ``0``, ``''``, ``'0'``, ``'false'`` (as ``False``).

    `is_nullable` (Boolean)
        If set, ``None`` values are permitted.
    """

    hint = """a Boolean value"""

    def __init__(self, is_nullable=False):
        # Sanity check on the arguments.
        assert isinstance(is_nullable, bool)

        self.is_nullable = is_nullable

    def __call__(self, value):
        # `None` is allowed if the `is_nullable` flag is set.
        if value is None:
            if self.is_nullable:
                return None
            else:
                raise ValueError("the null value is not permitted")

        # Valid values are: False, True, 0, 1, '', '0', '1', 'false', 'true';
        # anything else triggers an exception.
        if value in [0, '', '0', 'false']:
            value = False
        if value in [1, '1', 'true']:
            value = True
        if not isinstance(value, bool):
            raise ValueError("a Boolean value is expected; got %r" % value)

        # The value must be Boolean here.
        assert isinstance(value, bool)
        return value


class IntVal(Val):
    """
    Verifies if the value is an integer.

    Strings containing numeric characters are also accepted.

    `min_bound` (integer or ``None``)
        If set, check that the value is greater or equal to `min_bound`.

    `max_bound` (integer or ``None``)
        If set, check that the value is less or equal to `max_bound`.

    `is_nullable` (Boolean)
        If set, ``None`` values are permitted.
    """


    hint = """an integer"""

    def __init__(self, min_bound=None, max_bound=None, is_nullable=False):
        # Sanity check on the arguments.
        assert isinstance(min_bound, maybe(oneof(int, long)))
        assert isinstance(max_bound, maybe(oneof(int, long)))
        assert isinstance(is_nullable, bool)

        self.min_bound = min_bound
        self.max_bound = max_bound
        self.is_nullable = is_nullable

    def __call__(self, value):
        # `None` is allowed if the `is_nullable` flag is set.
        if value is None:
            if self.is_nullable:
                return None
            else:
                raise ValueError("the null value is not permitted")

        # Convert string values; a non-numeric string triggers `ValueError`.
        if isinstance(value, (str, unicode)):
            value = int(value)
        if not isinstance(value, (int, long)):
            raise ValueError("an integer value is required; got %r" % value)

        # Check the boundary conditions.
        if self.min_bound is not None and value < self.min_bound:
            raise ValueError("a value greater or equal to %s is required;"
                             " got %s" % (self.min_bound, value))
        if self.max_bound is not None and value > self.max_bound:
            raise ValueError("a value less or equal to %s is required;"
                             " got %s" % (self.max_bound, value))

        # `value` is an integer or a long integer here.
        assert isinstance(value, (int, long))
        return value


class UIntVal(IntVal):
    """
    Verifies if the value is a non-negative integer.
    """

    hint = """a non-negative integer"""

    def __init__(self, max_bound=None, is_nullable=False):
        super(UIntVal, self).__init__(0, max_bound, is_nullable)


class PIntVal(IntVal):
    """
    Verifies if the value is a positive integer.
    """

    hint = """a positive integer"""

    def __init__(self, max_bound=None, is_nullable=False):
        super(PIntVal, self).__init__(1, max_bound, is_nullable)


class DBVal(Val):
    """
    Verifies if the value is a connection URI.

    For description of the connection URI format, see
    :meth:`htsql.util.DB.parse`.

    `is_nullable` (Boolean)
        If set, ``None`` values are permitted.
    """

    hint = """a connection URI"""

    def __init__(self, is_nullable=False):
        # Sanity check on the arguments.
        assert isinstance(is_nullable, bool)

        self.is_nullable = is_nullable

    def __call__(self, value):
        # `None` is allowed if the `is_nullable` flag is set.
        if value is None:
            if self.is_nullable:
                return None
            else:
                raise ValueError("the null value is not permitted")

        # The `DB` class provides its own conversion routine.
        return DB.parse(value)


