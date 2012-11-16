#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ..cache import once
from .grammar import LexicalGrammar
from .decode import decode


@once
def prepare_scan():
    """
    Returns a lexical scanner for HTSQL grammar.
    """

    # Start a new grammar.
    grammar = LexicalGrammar()

    # Regular context.
    query = grammar.add_rule('query')

    # Whitespace characters and comments (discarded).
    query.add_token(r'''
        SPACE:  [\s]+ | [#] [^\0\r\n]*
    ''', is_junk=True)

    # A sequence of characters encloses in single quotes.
    query.add_token(r'''
        STRING: ['] ( [^'\0] | [']['] )* [']
    ''', unquote=(lambda t: t[1:-1].replace("''", "'")))

    # An opening quote character without a closing quote.
    query.add_token(r'''
        BAD_STRING: [']
    ''', error="cannot find a matching quote mark")

    # A number in exponential notation.
    query.add_token(r'''
        FLOAT:  ( [0-9]+ ( [.] [0-9]* )? | [.] [0-9]+ ) [eE] [+-]? [0-9]+
    ''')

    # A number with a decimal point.
    query.add_token(r'''
        DECIMAL:
                [0-9]+ [.] [0-9]* | [.] [0-9]+
    ''')

    # An unsigned integer number.
    query.add_token(r'''
        INTEGER:
                [0-9]+
    ''')

    # A sequence of alphanumeric characters (not starting with a digit).
    query.add_token(r'''
        NAME:   [\w]+
    ''')

    # Operators and punctuation characters.  The token code coincides
    # with the token value.
    query.add_token(r'''
        SYMBOL: [~] | [!][~] | [<][=] | [<] | [>][=] | [>] |
                [=][=] | [=] | [!][=][=] | [!][=] |
                [\^] | [?] | [-][>] | [@] | [:][=] |
                [!] | [&] | [|] | [+] | [-] | [*] | [/] |
                [(] | [)] | [{] | [}] | [.] | [,] | [:] | [;] | [$]
    ''', is_symbol=True)

    # The `[` character starts an identity constructor.
    query.add_token(r'''
        LBRACKET:
                [\[]
    ''', is_symbol=True, push='identity')

    # An unmatched `]`.
    query.add_token(r'''
        BAD_RBRACKET:
                [\]]
    ''', error="cannot find a matching '['")

    # The input end.
    query.add_token(r'''
        END:    $
    ''', is_symbol=True, pop=1)

    # Identity constructor context.
    identity = grammar.add_rule('identity')

    # Whitespace characters (discarded).
    identity.add_token(r'''
        SPACE:  [\s]+
    ''', is_junk=True)

    # Start of a nested label group.
    identity.add_token(r'''
        LBRACKET:
                [\[] | [(]
    ''', is_symbol=True, push='identity')

    # End of a label group or the identity constructor.
    identity.add_token(r'''
        RBRACKET:
                [\]] | [)]
    ''', is_symbol=True, pop=1)

    # Label separator.
    identity.add_token(r'''
        SYMBOL: [.]
    ''', is_symbol=True)

    # Unquoted sequence of alphanumeric characters and dashes.
    identity.add_token(r'''
        LABEL:  [\w-]+
    ''')

    # A sequence of characters encloses in single quotes.
    identity.add_token(r'''
        STRING: ['] ( [^'\0] | [']['] )* [']
    ''', unquote=(lambda t: t[1:-1].replace("''", "'")))

    # An opening quote character without a closing quote.
    identity.add_token(r'''
        BAD_STRING: [']
    ''', error="cannot find a matching quote mark")

    # A reference indicator.
    identity.add_token(r'''
        REFERENCE:
                [$]
    ''', is_symbol=True, push='name')

    # Unexpected end of input.
    identity.add_token(r'''
        END:    $
    ''', error="cannot find a matching ']'")

    # A context for an identifier following the `$` indicator
    # in an identity constructor.  We need a separate rule because
    # `%NAME` and `%LABEL` productions intersect.
    name = grammar.add_rule('name')

    # Whitespace characters (discarded).
    name.add_token(r'''
        SPACE:  [\s]+
    ''', is_junk=True)

    # An integer number; not expected here, but ensures that the following
    # `%NAME` production does not start with a digit.
    name.add_token(r'''
        INTEGER:
                [0-9]+
    ''', pop=1)

    # A sequence of alphanumeric characters (not starting with a digit).
    name.add_token(r'''
        NAME:   [\w]+
    ''', pop=1)

    # Anything else.
    name.add_token(r'''
        OTHER:  ()
    ''', is_junk=True, pop=1)

    # Add a `%DIRSIG` token in front of `+` and `-` direction indicators
    # to distinguish them from addition/subtraction operators.
    grammar.add_signal('''
        DIRSIG: ( `+` | `-` )+ ( `:` | `,` | `;` | `)` | `}` )
    ''')

    # Add `%PIPESIG` in front of `/:` pipe indicator to prevent it from
    # being recognized as a division operator.
    grammar.add_signal('''
        PIPESIG:
                `/` `:`
    ''')

    # Add `%LHSSIG` in front of a left-hand side of an assignment expression.
    grammar.add_signal('''
        LHSSIG: `$`? %NAME ( `.` `$`? %NAME )*
                ( `(` ( `$`? %NAME ( `,` `$`? %NAME )* `,`? )? `)` )?
                `:=`
    ''')

    # Generate and return the scanner.
    return grammar()


def scan(text, start=None):
    """
    Tokenizes the input query string.

    `text`: ``str`` or ``unicode``
        A raw query string.

    `start`: ``unicode`` or ``None``
        The initial lexical rule (by default, the first rule in the grammar).

    *Returns*: [:class:`.Token`]
        List of tokens.
    """
    # Remove transmission artefacts.
    text = decode(text)
    # Make a scanner for HTSQL grammar.
    scan = prepare_scan()
    # Tokenize the query.
    return scan(text, start)


