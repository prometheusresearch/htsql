#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.validator`
======================

This module provides utilities for data validation and conversion.
"""


from util import DB, maybe, oneof, listof
import re


class Validator(object):
    """
    Validators check if a given value conforms the specified format.

    A validator acts as function that takes a value, checks if it conforms
    the format, normalizes and returns the value.  Example::

        validator = IntVal()
        value = validator(value)

    If the value does not conform the format, :exc:`ValueError` is raised.

    Attribute `hint` gives a short textual description of the format.

    :class:`Validator` is the base abstract class for validators.  Its
    subclasses provide validators for specific formats.

    To create a validator for a new format, create a subclass of
    :class:`Validator` and override the :meth:`__call__()` method.  The
    method should accept values of any type.  If the value does not conform
    to the format, :exc:`ValueError` should be raised; otherwise the value
    should be normalized and returned.

    Example::

        class IntVal(Validator):

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


class AnyVal(Validator):
    """
    A no-op validator.
    """

    hint = """an arbitrary value"""

    def __call__(self, value):
        # All values are passed through.
        return value


class StrVal(Validator):
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

        # Translate Unicode strings to UTF-8 encoded byte strings.
        if isinstance(value, unicode):
            value = value.encode('utf-8')

        # Verify that the value does not contain the NUL character.
        if '\0' in value:
            raise ValueError("NUL character is not allowed; got %r" % value)

        # Check if the value matches the pattern.
        if self.pattern is not None:
            regexp = re.compile(self.pattern)
            if not regexp.match(value):
                raise ValueError("%r does not match pattern %r"
                                 % (value, self.pattern))

        # We are done, return a string.
        assert isinstance(value, str)
        return value


class WordVal(Validator):
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
    regexp = re.compile(r'^[0-9a-zA-Z_ -]+$')

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

        # Translate Unicode strings to UTF-8 encoded byte strings.
        if isinstance(value, unicode):
            value = value.encode('utf-8')

        # Check if the string matches the word pattern.
        if not self.regexp.match(value):
            raise ValueError("a string containing alphanumeric characters,"
                             " dashes, underscores or spaces is expected;"
                             " got %r" % value)

        # Normalize and return the value.
        value = value.replace('_', '-').replace(' ', '-')
        return value


class ChoiceVal(Validator):
    """
    Verifies if the value belongs to a specified set of string constants.

    ``choices`` (a list of strings)
        List of valid choices.

    `is_nullable` (Boolean)
        If set, ``None`` values are permitted.
    """

    hint = """a choice (%s)"""

    def __init__(self, choices, is_nullable=False):
        # Sanity check on the arguments.
        assert isinstance(choices, listof(str))
        assert isinstance(is_nullable, bool)

        self.choices = choices
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

        # Translate Unicode strings to UTF-8 encoded byte strings.
        if isinstance(value, unicode):
            value = value.encode('utf-8')

        # Check if the value belongs to the specified set of choices.
        if value not in self.choices:
            choice_list = ", ".join(repr(choice) for choice in self.choices)
            raise ValueError("one of %s expected; got %r"
                             % (choice_list, value))

        return value


class BoolVal(Validator):
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


class IntVal(Validator):
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


class FloatVal(Validator):
    """
    Verifies if the value is an integer.

    Strings representing numeric values in a decimal or a scientific format
    are also accepted.

    `min_bound` (float or ``None``)
        If set, check that the value is greater or equal to `min_bound`.

    `max_bound` (float or ``None``)
        If set, check that the value is less or equal to `max_bound`.

    `is_nullable` (Boolean)
        If set, ``None`` values are permitted.
    """

    hint = """a float number"""

    def __init__(self, min_bound=None, max_bound=None, is_nullable=False):
        # Sanity check on the arguments.
        assert isinstance(min_bound, maybe(float))
        assert isinstance(max_bound, maybe(float))
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

        # Convert string and integer values.
        if isinstance(value, (str, unicode)):
            value = float(value)
        if isinstance(value, (int, long)):
            value = float(value)
        if not isinstance(value, float):
            raise ValueError("a float value is required; got %r" % value)

        # Check the boundary conditions.
        if self.min_bound is not None and value < self.min_bound:
            raise ValueError("a value greater or equal to %s is required;"
                             " got %s" % (self.min_bound, value))
        if self.max_bound is not None and value > self.max_bound:
            raise ValueError("a value less or equal to %s is required;"
                             " got %s" % (self.max_bound, value))

        # `value` is a float number here.
        assert isinstance(value, float)
        return value


class UFloatVal(IntVal):
    """
    Verifies if the value is a non-negative float number.
    """

    hint = """a non-negative float number"""

    def __init__(self, max_bound=None, is_nullable=False):
        super(UFloatVal, self).__init__(0, max_bound, is_nullable)


class SeqVal(Validator):
    """
    Verifies if the value is a list with each list item conforming
    the specified format.

    Also accepted are strings that agree with the following grammar::

        value ::= <empty> | item ( [,] value )*
        item  ::= <any non-space character or comma>+
                | ['] ( <any non-quote character> | [']['] )* [']

    That is, the string must represent a comma-separated list of elements.
    If an element contains no whitespace characters and no commas, it could
    be represented literally; otherwise it should be quited with ``'`` and
    any single quote character should be duplicated.

    `item_validator` (:class:`Validator`)
        Validator for the sequence elements.

    `length` (an integer or ``None``)
        If set, check that the length of the sequence is equal to `length`.

    `is_nullable` (Boolean)
        If set, ``None`` values are permitted.
    """

    hint = """a sequence [%s]"""

    # A regular expression to match tokens when the sequence is represented
    # by a string.
    pattern = r"""
        (?P<space> \s+ ) |
        (?P<comma> , ) |
        (?P<item> [^ \t\r\n',]+ | ' (?: [^'] | '')* ' )
    """
    regexp = re.compile(pattern, re.X)

    def __init__(self, item_validator, length=None, is_nullable=False):
        # Sanity check on the arguments.
        assert isinstance(item_validator, Validator)
        assert isinstance(length, maybe(int))
        assert isinstance(is_nullable, bool)

        self.item_validator = item_validator
        self.length = length
        self.is_nullable = is_nullable

    def get_hint(self):
        # Provide short description of the form:
        #   a sequence [{item hint}]
        item_hint = self.item_validator.get_hint()
        return self.hint % item_hint

    def __call__(self, value):
        # `None` is allowed if the `is_nullable` flag is set.
        if value is None:
            if self.is_nullable:
                return None
            else:
                raise ValueError("the null value is not permitted")

        # Translate Unicode strings to UTF-8 byte strings.
        if isinstance(value, unicode):
            value = value.encode('utf-8')

        # If the value is a string, parse it and extract the elements.
        if isinstance(value, str):

            # List of elements.
            items = []
            # The beginning of the next token.
            start = 0
            # The current parsing state.
            is_comma_expected = False

            # Parse the string till it ends.  Error conditions are signalled
            # by ending the loop prematurely.
            while start < len(value):
                # Fetch the next token.
                match = self.regexp.match(value)
                if match is None:
                    break
                # A simple state machine.  The `is_comma_expected` variable
                # represents the machine state; the token type represents
                # conditions.  The transition table:
                #   is_comma_expected is on:
                #       token type is 'space' => no-op
                #       token type is 'comma' => turn is_comma_expected off
                #       token type is 'item'  => ERROR
                #   is_comma_expected is off:
                #       token type is 'space' => no-op
                #       token type is 'comma' => ERROR
                #       token type is 'item'  => extract the element,
                #                                turn is_comma_expected on
                if match.group('item') is not None:
                    if is_comma_expected:
                        break
                    item = match.group('item')
                    if item[0] == item[-1] == '\'':
                        item = item[1:-1].replace('\'\'', '\'')
                    items.append(item)
                    is_comma_expected = True
                elif match.group('comma') is not None:
                    if not is_comma_expected:
                        break
                    is_comma_expected = False
                # Move to the beginning of the next token.
                start = match.end()

            # If the loop ended prematurely, it must have been
            # a parsing error.
            if start < len(value):
                raise ValueError("a comma-separated list is expected;"
                                 " got %r" % value)

            # Now `value` is a list of elements.
            value = items

        # By this step, `value` must be converted to a proper list.
        if not isinstance(value, list):
            raise ValueError("a list is expected; got %r" % value)

        # Check the length is specified.
        if self.length is not None:
            if len(value) != self.length:
                raise ValueError("a sequence of length %s is expected;"
                                 " got %r" % (self.length, value))

        # Validate and normalize the list elements.
        items = []
        for idx, item in enumerate(value):
            try:
                item = self.item_validator(item)
            except ValueError, exc:
                raise ValueError("invalid sequence item"
                                 " #%s (%s)" % (idx+1, exc))
            items.append(item)
        value = items

        # Here `value` is a list of normalized elements.
        return value


class MapVal(Validator):
    """
    Verifies if the value is a dictionary with keys and items conforming
    the specified formats.

    Also accepted are strings that agree with the following grammar::

        value     ::= <empty> | key ( [:] | [=] ) item ( [,] value )*
        key, item ::= <any non-space character except for [:], [=] or [,]>+
                    | ['] ( <any non-quote character> | [']['] )* [']

    That is, the string must represent a comma-separated list of ``key=item``
    pairs.  The key and the item could be quoted or unquoted.  An unquoted
    element contains no whitespace, ``:``, ``=``, ``,`` characters.  A quoted
    element is enclosed with ``'`` and has any single quote character
    duplicated.

    `key_validator` (:class:`Validator`)
        Validator for the mapping keys.

    `item_validator` (:class:`Validator`)
        Validator for the mapping values.

    `is_nullable` (Boolean)
        If set, ``None`` values are permitted.
    """

    hint = """a mapping {%s -> %s}"""

    # A regular expression to match tokens when the mapping is represented
    # by a string.
    pattern = r"""
        (?P<space> \s+ ) |
        (?P<comma> , ) |
        (?P<colon> [:=] ) |
        (?P<item> [^ \t\r\n',:]+ | ' (?: [^'] | '')* ' )
    """
    regexp = re.compile(pattern, re.X)

    def __init__(self, key_validator, item_validator, is_nullable=False):
        # Sanity check on the arguments.
        assert isinstance(key_validator, Validator)
        assert isinstance(item_validator, Validator)
        assert isinstance(is_nullable, bool)

        self.key_validator = key_validator
        self.item_validator = item_validator
        self.is_nullable = is_nullable

    def get_hint(self):
        # Provide short description of the form:
        #   a mapping {{key hint} -> {item hint}}
        key_hint = self.key_validator.get_hint()
        item_hint = self.item_validator.get_hint()
        return self.hint % (key_hint, item_hint)

    def __call__(self, value):
        # `None` is allowed if the `is_nullable` flag is set.
        if value is None:
            if self.is_nullable:
                return None
            else:
                raise ValueError("the null value is not permitted")

        # Translate Unicode strings to UTF-8 encoded byte strings.
        if isinstance(value, unicode):
            value = value.encode('utf-8')

        # If the value is a string, parse it and extract the elements.
        if isinstance(value, str):

            # List of `(key, item)` pairs.
            pairs = []
            # The beginning of the next token.
            start = 0
            # The current parsing state.
            is_key_expected = True
            is_colon_expected = False
            is_item_expected = False
            is_comma_expected = False
            # Keeps the current key till we extract the corresponding item.
            current_key = None

            # Parse the string till it ends.  Error conditions are signalled
            # by ending the loop prematurely.
            while start < len(value):
                # Fetch the next token.
                match = self.regexp.match(value)
                if match is None:
                    break
                # This loop represents a simple state machine.  The
                # `is_key/colon/item/comma_expected` variables keep the
                # current state; the token type represents conditions.
                # The transition table:
                #   state is 'key':
                #       token type is 'space' => no-op
                #       token type is 'colon' => ERROR
                #       token type is 'comma' => ERROR
                #       token type is 'item'  => extract the element
                #                                as the current key,
                #                                set state to 'colon'
                #   state is 'colon':
                #       token type is 'space' => no-op
                #       token type is 'colon' => set state to 'item'
                #       token type is 'comma' => ERROR
                #       token type is 'item'  => ERROR
                #   state is 'item':
                #       token type is 'space' => no-op
                #       token type is 'colon' => ERROR
                #       token type is 'comma' => ERROR
                #       token type is 'item'  => extract the element
                #                                as the item corresponding
                #                                to the current key,
                #                                set state to 'comma'
                #   state is 'comma':
                #       token type is 'space' => no-op
                #       token type is 'colon' => ERROR
                #       token type is 'comma' => set state to 'key'
                #       token type is 'item'  => ERROR
                # Note that the final state must be either 'comma' or 'key'.
                if match.group('item') is not None:
                    item = match.group('item')
                    if item[0] == item[-1] == '\'':
                        item = item[1:-1].replace('\'\'', '\'')
                    if is_key_expected:
                        current_key = item
                        is_key_expected = False
                        is_colon_expected = True
                    elif is_item_expected:
                        pairs.append((current_key, item))
                        current_key = None
                        is_item_expected = False
                        is_comma_expected = True
                    else:
                        break
                elif match.group('colon') is not None:
                    if not is_colon_expected:
                        break
                    is_colon_expected = False
                    is_item_expected = True
                elif match.group('comma') is not None:
                    if not is_comma_expected:
                        break
                    is_comma_expected = False
                    is_key_expected = True
                # Move to the next token.
                start = match.end()

            # Check if the parsing loop ended prematurely, or if the final
            # state is invalid.
            if start < len(value) or is_colon_expected or is_item_expected:
                raise ValueError("a comma-separated list of key=value pairs"
                                 " is expected; got %r" % value)

            # Now `value` is a dictionary.
            value = dict(pairs)

        # By this time, `value` must be a dictionary.
        if not isinstance(value, dict):
            raise ValueError("a dictionary is required; got %r" % value)

        # Validate and normalize the mapping keys and values.  Note that
        # we need to check for duplicate keys since normalization may
        # convert two distinct keys to the same normalized key.
        pairs = []
        key_set = set()
        for key in sorted(value):
            try:
                validated_key = self.key_validator(key)
            except ValueError, exc:
                raise ValueError("invalid mapping key (%s)" % exc)
            if validated_key in key_set:
                raise ValueError("duplicate mapping key %r" % key)
            key_set.add(validated_key)
            item = value[key]
            try:
                validated_item = self.item_validator(item)
            except ValueError, exc:
                raise ValueError("invalid mapping item for key %r (%s)"
                                 % (key, item))
            pairs.append((validated_key, validated_item))
        value = dict(pairs)

        # Here `value` is a mapping with normalized keys and items.
        return value


class ClassVal(Validator):
    """
    Verifies if the value is an instance of the specified class.

    `class_type`
        A class or a type object.

    `is_nullable` (Boolean)
        If set, ``None`` values are permitted.
    """

    hint = """a class instance (%s)"""

    def __init__(self, class_type, is_nullable=False):
        # Sanity check on the arguments.
        assert isinstance(class_type, type)
        assert isinstance(is_nullable, bool)

        self.class_type = class_type
        self.is_nullable = is_nullable

    def get_hint(self):
        # Provide short description of the form
        #   a class instance({class name})
        return self.hint % self.class_type.__name__

    def __call__(self, value):
        # `None` is allowed if the `is_nullable` flag is set.
        if value is None:
            if self.is_nullable:
                return None
            else:
                raise ValueError("the null value is not permitted")

        # Check if the value is an instance of the specified class.
        if not isinstance(value, self.class_type):
            raise ValueError("an instance of %s is expected; got %r"
                             % (self.class_type.__name__, value))

        return value


class DBVal(Validator):
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


