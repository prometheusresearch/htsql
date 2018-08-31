#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ..util import Clonable, Hashable, Printable, YAMLable
import urllib.request, urllib.parse, urllib.error


class Token(Clonable, Hashable, Printable, YAMLable):
    """
    A lexical token.

    `code`: ``unicode``
        The token type indicator; for operator symbols and punctuation
        characters, coincides with the token value.  By convention, code
        equal to `""` indicates EOF.

    `text`: ``unicode``
        The token value.
    """

    def __init__(self, code, text):
        assert isinstance(code, str)
        assert isinstance(text, str)
        self.code = code
        self.text = text

    def __basis__(self):
        return (self.code, self.text)

    def __bool__(self):
        # `False` for EOF token; `True` otherwise.
        return bool(self.code)

    def __str__(self):
        # '$', '`<code>`:<text>' or '%<code>:<text>'
        chunks = []
        if not self.code:
            chunks.append("$")
        elif self.code.isalpha():
            chunks.append("%"+self.code)
        else:
            chunks.append("`%s`" % self.code.replace("`", "``"))
        if self.text:
            chunks.append(":")
            text = self.text
            text = urllib.parse.quote(text, safe='')
            chunks.append(text)
        return "".join(chunks)

    def __yaml__(self):
        yield ('code', self.code)
        if self.code.isalpha() or self.code != self.text:
            yield ('text', self.text)


#
# Token codes recognized by HTSQL scanner.
#

# The query end.
END = ''

# A sequence of alphanumeric characters that does not start with a digit.
NAME = 'NAME'

# A sequence of characters enclosed in single quotes.
STRING = 'STRING'

# An unsigned integer number.
INTEGER = 'INTEGER'

# An unsigned decimal number.
DECIMAL = 'DECIMAL'

# An unsigned number in exponentional notation.
FLOAT = 'FLOAT'

# A sequence of alphanumeric characters (including `-`) in an identity literal.
LABEL = 'LABEL'

# Various operator and punctuation symbols.
SYMBOLS = [
    # comparison operators
    '=', '!=', '==', '!==', '~', '!~', '<', '<=', '>', '>=',

    # logical operators
    '!', '&', '|',

    # arithmetic operators
    '+', '-', '*', '/',

    # flow operators
    '^', '?', '->', '@',

    # assignment operator
    ':=',

    # punctuation
    '(', ')', '[', ']', '{', '}', '.', ',', ':', ';', '$',
]

# A signalling token for `+` and `-` direction indicators.
DIRSIG = 'DIRSIG'

# A signalling token for `/` `:` sequence starting a pipe notation.
PIPESIG = 'PIPESIG'

# A signalling token for the LHS of assignment operator (`:=`).
LHSSIG = 'LHSSIG'


