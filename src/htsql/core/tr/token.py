#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql.core.tr.token`
==========================

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
        The name of the token category.

    The constructor of :class:`Token` accepts the following parameters:

    `value` (a Unicode string)
        The token value.

    `mark` (:class:`htsql.core.mark.Mark`)
        The location of the token in the original query.
    """

    name = None

    def __init__(self, value, mark):
        assert isinstance(value, unicode)
        assert isinstance(mark, Mark)

        self.value = value
        self.mark = mark

    def __unicode__(self):
        return self.value

    def __str__(self):
        return self.value.encode('utf-8')


class NameToken(Token):
    """
    Represents a name token.

    In HTSQL, a name is a sequence of alphanumeric characters
    that does not start with a digit.  Alphanumeric characters include
    characters ``a``-``z``, ``A``-``Z``, ``0``-``9``, ``_``, and those
    Unicode characters that are classified as alphanumeric.
    """

    name = 'name'


class StringToken(Token):
    """
    Represents a string literal token.

    In HTSQL, a string literal is a sequence of arbitrary characters
    enclosed in single quotes (``'``).  Use a pair of single quote
    characters (``''``) to represent a single quote in a string.
    """

    name = 'string'


class UnquotedStringToken(StringToken):
    pass


class NumberToken(Token):
    """
    Represents a number literal token.

    HTSQL supports number literals in integer, float and scientific notations.
    """

    name = 'number'


class SymbolToken(Token):
    """
    Represents a symbol token.

    HTSQL employs the following groups of symbols:

    *comparison operators*
        ``=``, ``!=``, ``==``, ``!==``, ``~``, ``!~``,
        ``<``, ``<=``, ``>``, ``>=``.

    *logical operators*
        ``!``, ``&``, ``|``, ``?``.

    *arithmetic operators*
        ``+``, ``-``, ``*``, ``/``, ``^``.

    *assignment operator*
        ``:=``.

    *punctuation*
        ``(``, ``)``, ``[``, ``]``, ``{``, ``}``,
        ``.``, ``,``, ``->``, ``/``, ``:``, ``$``, ``@``.
    """

    name = 'symbol'


class EndToken(Token):
    """
    Represents the end token.

    The end token is emitted when the scanner reached the end of the input
    string and forces the scanner to stop.
    """

    name = 'end'


