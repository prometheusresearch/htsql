#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql.core.tr.scan`
=========================

This module implements the HTSQL scanner.
"""


from .token import (Token, NameToken, StringToken, UnquotedStringToken,
        NumberToken, SymbolToken, EndToken)
from .error import ScanError, ParseError
from ..mark import Mark
from ..util import maybe, listof
import re
import urllib2


class TokenStream(object):
    """
    A sequence of tokens.

    :class:`TokenStream` wraps a list of tokens with a convenient interface
    for consumption and look-ahead.

    `tokens` (a list of :class:`htsql.core.tr.token.Token` objects)
        A list of tokens.
    """

    def __init__(self, tokens):
        assert isinstance(tokens, listof(Token))

        # The list of tokens.
        self.tokens = tokens
        # The position of the active token.
        self.idx = 0

    def peek(self, token_class=None, values=None, ahead=0,
             do_pop=False, do_force=False):
        """
        Returns the active token.

        If the parameter `ahead` is non-zero, the method will return
        the next `ahead` token after the active one.

        When `token_class` is set, the method checks if the token is an
        instance of the given class.  When `values` is set, the method
        checks if the token value belongs to the given list of values.
        If any of the checks fail, the method either raises
        :exc:`htsql.core.tr.error.ParseError` or returns ``None`` depending
        on the value of the `do_force` parameter.

        This method advances the active pointer to the next token if
        `do_pop` is enabled.

        `token_class` (a subclass of :class:`htsql.core.tr.token.Token` or ``None``)
            If not ``None``, the method checks that the returned token
            is an instance of `token_class`.

        `values` (a list of Unicode strings or ``None``)
            If not ``None``, the method checks that the value of the
            returned token belongs to the list.

        `ahead` (an integer)
            The position of the returned token relative to the active one.

        `do_pop` (Boolean)
            If set, the method will advance the active pointer.

        `do_force` (Boolean)
            This flag affects the method behavior when any of the token
            checks fail.  If set, the method will raise
            :exc:`htsql.core.tr.error.ParseError`; otherwise it will return
            ``None``.
        """
        # Sanity check on the arguments.
        assert token_class is None or issubclass(token_class, Token)
        assert isinstance(values, maybe(listof(unicode)))
        assert token_class is not None or values is None
        assert isinstance(ahead, int) and ahead >= 0
        assert self.idx+ahead < len(self.tokens)
        assert isinstance(do_pop, bool)
        assert isinstance(do_force, bool)

        # Get the token we are going to return.  It is the responsibility
        # of the caller to ensure that the index is in the list bounds.
        token = self.tokens[self.idx+ahead]

        # Indicates if the token passed the given tests.
        is_expected = True
        if token_class is not None:
            # Check the token type.
            if not isinstance(token, token_class):
                is_expected = False
            else:
                if values is not None:
                    # Check the token value.
                    if token.value not in values:
                        is_expected = False

        # The token failed the checks.
        if not is_expected:
            # If `do_force` is not set, return `None`; otherwise generate
            # an error message of the form:
            #   expected {token_class.NAME} ('{values[0]}', ...);
            #   got {token.NAME} '{token.value}'
            if not do_force:
                return None
            expected = "%s" % token_class.name.upper()
            if values:
                if len(values) == 1:
                    expected = "%s %r" % (expected, values[0].encode('utf-8'))
                else:
                    expected = "%s (%s)" % (expected,
                                        ", ".join(repr(value.encode('utf-8'))
                                                  for value in values))
            got = "%s %r" % (token.name.upper(), token.value.encode('utf-8'))
            raise ParseError("expected %s; got %s" % (expected, got),
                             token.mark)
        # Advance the pointer.
        if do_pop:
            self.idx += ahead+1

        return token

    def pop(self, token_class=None, values=None, do_force=True):
        """
        Returns the active token and advances the pointer to the next token.

        When `token_class` is set, the method checks if the token is an
        instance of the given class.  When `values` is set, the method
        checks if the token value belongs to the given list of values.
        If any of the checks fail, the method raises
        :exc:`htsql.core.tr.error.ParseError`.

        `token_class` (a subclass of :class:`htsql.core.tr.token.Token` or ``None``)
            If not ``None``, the method checks that the active token
            is an instance of `token_class`.

        `values` (a list of strings or ``None``)
            If not ``None``, the method checks that the value of the active
            token belongs to the list.

        `do_force` (Boolean)
            This flag affects the method behavior when any of the token
            checks fail.  If set, the method will raise
            :exc:`htsql.core.tr.error.ParseError`; otherwise it will return
            ``None``.
        """
        return self.peek(token_class, values,
                         do_pop=True, do_force=do_force)


class ScanRule(object):

    def __init__(self, pattern, token_class,
                 unquote=None, jump=None, error=None, is_end=False):
        assert isinstance(pattern, str)
        assert isinstance(token_class, maybe(type))
        assert isinstance(jump, maybe(str))
        assert isinstance(error, maybe(str))
        assert isinstance(is_end, bool)
        self.pattern = pattern
        self.token_class = token_class
        self.unquote = unquote
        self.jump = jump
        self.error = error
        self.is_end = is_end


class ScanGroup(object):

    def __init__(self, name, *rules):
        assert isinstance(name, str)
        rules = list(rules)
        assert isinstance(rules, listof(ScanRule))
        self.name = name
        self.rules = rules
        self.init_regexp()

    def init_regexp(self):
        patterns = []
        for idx, rule in enumerate(self.rules):
            pattern = r"(?P<_%s> %s)" % (idx, rule.pattern)
            patterns.append(pattern)
        pattern = r"|".join(patterns)
        self.regexp = re.compile(pattern, re.X|re.U)


class Scanner(object):
    """
    Implements the HTSQL scanner.

    The scanner tokenizes the input query and produces a stream of tokens.

    The first step of scanning is decoding ``%``-escape sequences.  Then
    the scanner splits the input query to a list of tokens.  The following
    token types are emitted:

    *NAME*
        Represents an HTSQL identifier: a sequence of alphanumeric characters
        that does not start with a digit.  Alphanumeric characters include
        characters ``a``-``z``, ``A``-``Z``, ``0``-``9``, ``_``, and those
        Unicode characters that are classified as alphanumeric.

    *NUMBER*
        Represents a number literal: integer, float and scientific notations
        are recognized.

    *STRING*
        Represents a string literal enclosed in single quotes; any single
        quote character should be doubled.

    *SYMBOL*
        Represents a valid symbol in HTSQL grammar; one of the following
        symbols:

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

    There are also two special token types:

    *WHITESPACE*
        Represents a whitespace character, one of: space, tab, LF, CR, FF, VT,
        and those Unicode characters that are classified as space.  The
        whitespace token are immediately discarded by the scanner and never
        emitted.

    *END*
        Indicates the end of the query; it is always the last token emitted.

    The :class:`Scanner` constructor takes the following argument:

    `input`
        An HTSQL query.
    """

    groups = [
            ScanGroup('top',
                ScanRule(r""" \s+ """, None),
                ScanRule(r""" (?! \d) \w+ """, NameToken),
                ScanRule(r""" ' (?: [^'\0] | '')* ' """, StringToken,
                         unquote=(lambda v: v[1:-1].replace(u'\'\'', u'\''))),
                ScanRule(r""" ' """, None,
                         error="""cannot find a matching quote mark"""),
                ScanRule(r""" (?: \d* \.)? \d+ [eE] [+-]? \d+ |"""
                         r""" \d* \. \d+ | \d+ \.? """, NumberToken),
                ScanRule(r""" ~ | !~ | <= | < | >= | > | == | = | !== | != |"""
                         r""" ! | & | \| | -> | \. | , | \? | \^ | / | \* |"""
                         r""" \+ | - | \( | \) | \{ | \} | := | : | \$ | @ """,
                         SymbolToken),
                ScanRule(r""" \[ """, SymbolToken, jump='locator'),
                ScanRule(r""" \] """, None,
                         error="""cannot find a matching '['"""),
                ScanRule(r""" $ """, EndToken, is_end=True)),
            ScanGroup('locator',
                ScanRule(r""" \s+ """, None),
                ScanRule(r""" \[ | \( """, SymbolToken,
                         jump='locator'),
                ScanRule(r""" \] | \) """, SymbolToken,
                         is_end=True),
                ScanRule(r""" \. """, SymbolToken),
                ScanRule(r""" [\w-]+ """, UnquotedStringToken),
                ScanRule(r""" ' (?: [^'\0] | '')* ' """, StringToken,
                         unquote=(lambda v: v[1:-1].replace(u'\'\'', u'\''))),
                ScanRule(r""" ' """, None,
                         error="""cannot find a matching quote mark"""),
                ScanRule(r""" $ """, None,
                         error="""cannot find a matching ']'""")),
    ]

    # The regular expression to match %-escape sequences.
    escape_pattern = r"""%(?P<code>[0-9A-Fa-f]{2})?"""
    escape_regexp = re.compile(escape_pattern)

    def __init__(self, input):
        # We accept both 8-bit and Unicode strings.  Since 8-bit strings
        # could contain %-escaped UTF-8 octets, we postpone decoding the
        # input till we process %-escapes.
        assert isinstance(input, (str, unicode))
        if isinstance(input, unicode):
            input = input.encode('utf-8')
        self.input = input

    def unquote(self, data):
        # Decode %-escape sequences.

        def replace(match):
            # Two hexdecimal digits that encode a byte value.
            code = match.group('code')
            # Complain if we get `%` not followed by two hexdecimal digits.
            if not code:
                # Prepare the marker object: convert the input to Unicode
                # and adjust the pointers to respect multi-byte characters.
                input = match.string.decode('utf-8', 'ignore')
                start, end = match.span()
                start = len(match.string[:start].decode('utf-8', 'ignore'))
                end = len(match.string[:end].decode('utf-8', 'ignore'))
                mark = Mark(input, start, end)
                raise ScanError("symbol '%' must be followed by two hexdecimal"
                                " digits", mark)
            # Return the character corresponding to the escape sequence.
            return chr(int(code, 16))

        # Apply `replace` to all `%`-escape sequences and return the result.
        data = self.escape_regexp.sub(replace, data)
        return data

    def scan(self):
        """
        Tokenizes the query; returns a :class:`TokenStream` instance.

        In case of syntax errors, raises :exc:`htsql.core.tr.error.ScanError`.
        """
        # Decode %-escape sequences.
        input = self.unquote(self.input)
        # Now that all UTF-8 octets are unquoted, we could convert
        # the input to Unicode.
        try:
            input = input.decode('utf-8')
        except UnicodeDecodeError, exc:
            # Prepare an error message.
            start = len(input[:exc.start].decode('utf-8', 'ignore'))
            end = len(input[:exc.end].decode('utf-8', 'ignore'))
            mark = Mark(input.decode('utf-8', 'replace'), start, end)
            raise ScanError("cannot convert a byte sequence %s to UTF-8: %s"
                            % (urllib2.quote(exc.object[exc.start:exc.end]),
                               exc.reason), mark)

        # The beginning of the next token (and the start of the mark slice).
        start = 0
        stack = [self.groups[0]]
        group_by_name = dict((group.name, group) for group in self.groups)
        # The list of generated tokens.
        tokens = []

        while stack:
            group = stack[-1]
            match = group.regexp.match(input, start)
            if match is None:
                mark = Mark(input, start, start+1)
                raise ScanError("unexpected symbol %r"
                                % input[start].encode('utf-8'), mark)
            end = match.end()
            mark = Mark(input, start, end)
            for idx, rule in enumerate(group.rules):
                value = match.group("_%s" % idx)
                if value is not None:
                    break
            else:
                # Unreachable
                assert False
            if rule.unquote is not None:
                value = rule.unquote(value)
            if rule.token_class is not None:
                token = rule.token_class(value, mark)
                tokens.append(token)
            if rule.is_end:
                stack.pop()
            if rule.jump is not None:
                assert rule.jump in group_by_name
                stack.append(group_by_name[rule.jump])
            if rule.error is not None:
                raise ScanError(rule.error, mark)
            # Advance the pointer to the beginning of the next token.
            start = end

        return TokenStream(tokens)


def scan(input):
    """
    Tokenizes the input HTSQL query or expression.
    
    Returns a stream of tokens (a :class:`TokenStream` instance).

    In case of syntax errors, raises :exc:`htsql.core.tr.error.ScanError`.
    """
    scanner = Scanner(input)
    return scanner.scan()


