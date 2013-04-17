#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ..util import Clonable, Hashable, Printable, YAMLable
import urllib


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
        assert isinstance(code, unicode)
        assert isinstance(text, unicode)
        self.code = code
        self.text = text

    def __basis__(self):
        return (self.code, self.text)

    def __nonzero__(self):
        # `False` for EOF token; `True` otherwise.
        return bool(self.code)

    def __unicode__(self):
        # '$', '`<code>`:<text>' or '%<code>:<text>'
        chunks = []
        if not self.code:
            chunks.append(u"$")
        elif self.code.isalpha():
            chunks.append(u"%"+self.code)
        else:
            chunks.append(u"`%s`" % self.code.replace(u"`", u"``"))
        if self.text:
            chunks.append(u":")
            text = self.text.encode('utf-8')
            text = urllib.quote(text, safe='')
            text = text.decode('utf-8')
            chunks.append(text)
        return u"".join(chunks)

    def __yaml__(self):
        yield ('code', self.code)
        if self.code.isalpha() or self.code != self.text:
            yield ('text', self.text)


#
# Token codes recognized by HTSQL scanner.
#

# The query end.
END = u''

# A sequence of alphanumeric characters that does not start with a digit.
NAME = u'NAME'

# A sequence of characters enclosed in single quotes.
STRING = u'STRING'

# An unsigned integer number.
INTEGER = u'INTEGER'

# An unsigned decimal number.
DECIMAL = u'DECIMAL'

# An unsigned number in exponentional notation.
FLOAT = u'FLOAT'

# A sequence of alphanumeric characters (including `-`) in an identity literal.
LABEL = u'LABEL'

# Various operator and punctuation symbols.
SYMBOLS = [
    # comparison operators
    u'=', u'!=', u'==', u'!==', u'~', u'!~', u'<', u'<=', u'>', u'>=',

    # logical operators
    u'!', u'&', u'|',

    # arithmetic operators
    u'+', u'-', u'*', u'/',

    # flow operators
    u'^', u'?', u'->', u'@',

    # assignment operator
    u':=',

    # punctuation
    u'(', u')', u'[', u']', u'{', u'}', u'.', u',', u':', u';', u'$',
]

# A signalling token for `+` and `-` direction indicators.
DIRSIG = u'DIRSIG'

# A signalling token for `/` `:` sequence starting a pipe notation.
PIPESIG = u'PIPESIG'

# A signalling token for the LHS of assignment operator (`:=`).
LHSSIG = u'LHSSIG'


