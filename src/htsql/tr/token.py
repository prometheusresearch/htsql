#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.token`
=====================

This module defines token types used by the HTSQL scanner.
"""


from ..mark import Mark
from ..util import Printable


class Token(Printable):
    """
    Represents a lexical token.

    This is an abstract class.  To add a concrete token type, create a
    subclass of :class:`Token` and override the following class attributes:

    `name` (a string)
        The name of the token category.  Must be a valid identifier since
        it is used by the scanner as the name of the pattern group.

    `pattern` (a string)
        The regular expression to match the token (in the verbose format).

    `is_ws` (Boolean)
        If set, indicates that the token is to be discarded.

    `is_end` (Boolean)
        If set, forces the scanner to stop the processing.

    When adding a subclass of :class:`Token`, you may also want to override
    methods :meth:`unquote` and :meth:`quote`.

    The constructor of :class:`Token` accepts the following parameters:

    `value` (a string)
        The token value.

    `mark` (:class:`htsql.mark.Mark`)
        The location of the token in the original query.
    """

    name = None
    pattern = None
    is_ws = False
    is_end = False

    @classmethod
    def unquote(cls, value):
        """
        Converts a raw string that matches the token pattern to a token value.
        """
        return value

    @classmethod
    def quote(cls, value):
        """
        Reverses :meth:`unquote`.
        """
        return value

    def __init__(self, value, mark):
        assert isinstance(value, str)
        assert isinstance(mark, Mark)

        self.value = value
        self.mark = mark

    def __str__(self):
        return self.value


class SpaceToken(Token):
    """
    Represents a whitespace token.

    In HTSQL, whitespace characters are space, tab, LF, CR, FF, VT and
    those Unicode characters that are classified as space.

    Whitespace tokens are discarded by the scanner without passing them
    to the parser.
    """

    name = 'whitespace'
    pattern = r""" \s+ """
    # Do not pass the token to the scanner.
    is_ws = True


class NameToken(Token):
    """
    Represents a name token.

    In HTSQL, a name is a sequence of alphanumeric characters
    that does not start with a digit.  Alphanumeric characters include
    characters ``a``-``z``, ``A``-``Z``, ``0``-``9``, ``_``, and those
    Unicode characters that are classified as alphanumeric.
    """

    name = 'name'
    pattern = r""" (?! \d) \w+ """


class StringToken(Token):
    """
    Represents a string literal token.

    In HTSQL, a string literal is a sequence of arbitrary characters
    enclosed in single quotes (``'``).  Use a pair of single quote
    characters (``''``) to represent a single quote in a string.
    """

    name = 'string'
    # Note: we do not permit `NUL` characters in a string literal.
    pattern = r""" ' (?: [^'\0] | '')* ' """

    @classmethod
    def unquote(cls, value):
        # Strip leading and trailing quotes and replace `''` with `'`.
        return value[1:-1].replace('\'\'', '\'')

    @classmethod
    def quote(cls, value):
        # Replace all occurences of `'` with `''`, enclose the string
        # in the quotes.
        return '\'%s\'' % value.replace('\'', '\'\'')


class NumberToken(Token):
    """
    Represents a number literal token.

    HTSQL supports number literals in integer, float and scientific notations.
    """

    name = 'number'
    pattern = r""" (?: \d* \.)? \d+ [eE] [+-]? \d+ | \d* \. \d+ | \d+ \.? """


class SymbolToken(Token):
    """
    Represents a symbol token.

    HTSQL employs the following groups of symbols:

    *comparison operators*
        ``=``, ``!=``, ``==``, ``!==``, ``~``, ``!~``,
        ``~~``, ``!~~``, ``^~``, ``!^~``, ``^~~``, ``!^~~``,
        ``$~``, ``!$~``, ``$~~``, ``!$~~``, ``=~``, ``!=~``,
        ``=~~``, ``!=~~``. ``<``, ``<=``, ``>``, ``>=``.

    *logical operators*
        ``!``, ``&``, ``|``, ``->``, ``?``.

    *arithmetic operators*
        ``+``, ``-``, ``*``, ``/``, ``^``.

    *assignment operator*
        ``:=``.

    *punctuation*
        ``(``, ``)``, ``[``, ``]``, ``{``, ``}``,
        ``.``, ``,``, ``/``, ``:``.
    """

    name = 'symbol'
    pattern = r"""
        =~~ | =~ | \^~~ | \^~ | \$~~ | \$~ | ~~ | ~ |
        !=~~ | !=~ | !\^~~ | !\^~ | !\$~~ | !\$~ | !~~ | !~ |
        <= | < | >= | > | == | = | !== | != | ! |
        & | \| | -> | \. | , | \? | \^ | / | \* | \+ | - |
        \( | \) | \{ | \} | \[ | \] | := | :
    """


class EndToken(Token):
    """
    Represents the end token.

    The end token is emitted when the scanner reached the end of the input
    string and forces the scanner to stop.
    """

    name = 'end'
    pattern = r""" $ """
    is_end = True


