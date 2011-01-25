#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.parser`
======================

This module implements the HTSQL parser.
"""


from ..mark import Mark
from .scan import scan
from .token import NameToken, StringToken, NumberToken, SymbolToken, EndToken
from .syntax import (QuerySyntax, SegmentSyntax, SelectorSyntax,
                     SieveSyntax, OperatorSyntax, FunctionOperatorSyntax,
                     FunctionCallSyntax, GroupSyntax, SpecifierSyntax,
                     IdentifierSyntax, WildcardSyntax,
                     StringSyntax, NumberSyntax)


class Parser(object):
    """
    Implements an HTSQL parser.

    A parser takes a stream of tokens from the HTSQL scanner and
    produces a syntax node.

    This is an abstract class; see subclasses of :class:`Parser` for
    implementations of various parts of the HTSQL grammar.

    `input` (a string)
        The input HTSQL expression.
    """

    # FIXME: get rid of the metaclass and `<<`.  Implement `Parser`
    # as an adapter with matching by rule of an LL(n) grammar.

    class __metaclass__(type):
        # Implements a shortcut:
        #   Parser << tokens
        # to call
        #   Parser.process(tokens)

        def __lshift__(parser_class, tokens):
            return parser_class.process(tokens)

    def __init__(self, input):
        assert isinstance(input, str)
        self.input = input

    def parse(self):
        """
        Parses the input expression; returns the corresponding syntax node.
        """
        # Tokenize the input query.
        tokens = scan(self.input)
        # Parse the input query.
        syntax = self.process(tokens)
        # Ensure that we reached the end of the token stream.
        tokens.pop(EndToken)
        return syntax

    @classmethod
    def process(cls, tokens):
        """
        Takes a stream of tokens; returns a syntax node.

        This function does not have to exhaust the token stream.

        The ``<<`` operator could be used as a synonym for :meth:`process`;
        the following expressions are equivalent::

            Parser << tokens
            Parser.process(tokens)

        `tokens` (:class:`htsql.tr.scanner.TokenStream`)
            The stream of tokens to parse.
        """
        # Override in subclasses.
        raise NotImplementedError()


class QueryParser(Parser):
    """
    Parses an HTSQL query.

    Here is the grammar of HTSQL::

        input           ::= query END

        query           ::= '/' segment? format?
        segment         ::= selector | specifier selector? filter?
        filter          ::= '?' test
        format          ::= '/' ':' identifier

        test            ::= test direction | test application | or_test
        direction       ::= ( '+' | '-' )
        application     ::= ':' identifier ( or_test | call )?
        or_test         ::= and_test ( '|' and_test )*
        and_test        ::= implies_test ( '&' implies_test )*
        implies_test    ::= unary_test ( '->' unary_test )?
        unary_test      ::= '!' unary_test | comparison

        comparison      ::= expression ( ( '=~~' | '=~' | '^~~' | '^~' |
                                           '$~~' | '$~' | '~~' | '~' |
                                           '!=~~' | '!=~' | '!^~~' | '!^~' |
                                           '!$~~' | '!$~' | '!~~' | '!~' |
                                           '<=' | '<' | '>=' |  '>' |
                                           '==' | '=' | '!==' | '!=' )
                                         expression )?

        expression      ::= term ( ( '+' | '-' ) term )*
        term            ::= factor ( ( '*' | '/' ) factor )*
        factor          ::= ( '+' | '-' ) factor | power
        power           ::= sieve ( '^' power )?

        sieve           ::= specifier selector? filter?
        specifier       ::= atom ( '.' identifier call? )* ( '.' '*' )?
        atom            ::= '*' | selector | group | identifier call? | literal

        group           ::= '(' test ')'
        call            ::= '(' tests? ')'
        selector        ::= '{' tests? '}'
        tests           ::= test ( ',' test )* ','?

        identifier      ::= NAME
        literal         ::= STRING | NUMBER

    Note that this grammar is almost LL(1); one notable exception is
    the postfix ``+`` and ``-`` operators.
    """

    @classmethod
    def process(cls, tokens):
        # Parse the productions:
        #   query           ::= '/' segment? format?
        #   format          ::= '/' ':' identifier
        head_token = tokens.pop(SymbolToken, ['/'])
        segment = None
        format = None
        if not tokens.peek(EndToken):
            segment = SegmentParser << tokens
        if tokens.peek(SymbolToken, ['/']):
            tokens.pop(SymbolToken, ['/'])
            tokens.pop(SymbolToken, [':'])
            format = IdentifierParser << tokens
        mark = Mark.union(head_token, segment, format)
        query = QuerySyntax(segment, format, mark)
        return query


class SegmentParser(Parser):
    """
    Parses a `segment` production.
    """

    @classmethod
    def process(cls, tokens):
        # Parses the productions:
        #   segment         ::= selector | specifier selector? filter?
        #   filter          ::= '?' test
        base = None
        selector = None
        filter = None
        if tokens.peek(SymbolToken, ['{']):
            selector = SelectorParser << tokens
        else:
            base = SpecifierParser << tokens
            if tokens.peek(SymbolToken, ['{']):
                selector = SelectorParser << tokens
            if tokens.peek(SymbolToken, ['?'], do_pop=True):
                filter = TestParser << tokens
        mark = Mark.union(base, selector, filter)
        segment = SegmentSyntax(base, selector, filter, mark)
        return segment


class TestParser(Parser):
    """
    Parses a `test` production.
    """

    @classmethod
    def process(cls, tokens):
        # Parses the productions:
        #   test            ::= test direction | test application | or_test
        #   direction       ::= ( '+' | '-' )
        #   application     ::= ':' identifier ( or_test | call )?
        test = OrTestParser << tokens
        while tokens.peek(SymbolToken, ['+', '-', ':']):
            if tokens.peek(SymbolToken, ['+', '-']):
                symbol_token = tokens.pop(SymbolToken, ['+', '-'])
                symbol = symbol_token.value
                mark = Mark.union(test, symbol_token)
                test = OperatorSyntax(symbol, test, None, mark)
            else:
                symbol_token = tokens.pop(SymbolToken, [':'])
                identifier = IdentifierParser << tokens
                arguments = [test]
                if tokens.peek(SymbolToken, ['(']):
                    tokens.pop(SymbolToken, ['('])
                    while not tokens.peek(SymbolToken, [')']):
                        argument = TestParser << tokens
                        arguments.append(argument)
                        if not tokens.peek(SymbolToken, [')']):
                            tokens.pop(SymbolToken, [',', ')'])
                    tail_token = tokens.pop(SymbolToken, [')'])
                    mark = Mark.union(test, tail_token)
                else:
                    ahead = 0
                    while tokens.peek(SymbolToken, ['+', '-'], ahead=ahead):
                        ahead += 1
                    if not (tokens.peek(SymbolToken,
                                        [':', ',', ')', '}'], ahead=ahead) or
                            tokens.peek(EndToken, ahead=ahead)):
                        argument = OrTestParser << tokens
                        arguments.append(argument)
                    mark = Mark.union(test, identifier, arguments[-1])
                test = FunctionOperatorSyntax(identifier, arguments, mark)
        return test


class OrTestParser(Parser):
    """
    Parses an `or_test` production.
    """

    @classmethod
    def process(cls, tokens):
        # Parses the production:
        #   or_test         ::= and_test ( '|' and_test )*
        test = AndTestParser << tokens
        while tokens.peek(SymbolToken, ['|']):
            symbol_token = tokens.pop(SymbolToken, ['|'])
            symbol = symbol_token.value
            left = test
            right = AndTestParser << tokens
            mark = Mark.union(left, right)
            test = OperatorSyntax(symbol, left, right, mark)
        return test


class AndTestParser(Parser):
    """
    Parses an `and_test` production.
    """

    @classmethod
    def process(cls, tokens):
        # Parses the production:
        #   and_test        ::= implies_test ( '&' implies_test )*
        test = ImpliesTestParser << tokens
        while tokens.peek(SymbolToken, ['&']):
            symbol_token = tokens.pop(SymbolToken, ['&'])
            symbol = symbol_token.value
            left = test
            right = ImpliesTestParser << tokens
            mark = Mark.union(left, right)
            test = OperatorSyntax(symbol, left, right, mark)
        return test


class ImpliesTestParser(Parser):
    """
    Parses an `implies_test` production.
    """

    @classmethod
    def process(cls, tokens):
        # Parses the production:
        #   implies_test    ::= unary_test ( '->' unary_test )?
        test = UnaryTestParser << tokens
        if not tokens.peek(SymbolToken, ['->']):
            return test
        symbol_token = tokens.pop(SymbolToken, ['->'])
        symbol = symbol_token.value
        left = test
        right = UnaryTestParser << tokens
        mark = Mark.union(left, right)
        test = OperatorSyntax(symbol, left, right, mark)
        return test


class UnaryTestParser(Parser):
    """
    Parses a `unary_test` production.
    """

    @classmethod
    def process(cls, tokens):
        # Parses the production:
        #   unary_test      ::= '!' unary_test | comparison
        symbol_tokens = []
        while tokens.peek(SymbolToken, ['!']):
            symbol_token = tokens.pop(SymbolToken, ['!'])
            symbol_tokens.append(symbol_token)
        test = ComparisonParser << tokens
        while symbol_tokens:
            symbol_token = symbol_tokens.pop()
            symbol = symbol_token.value
            mark = Mark.union(symbol_token, test)
            test = OperatorSyntax(symbol, None, test, mark)
        return test


class ComparisonParser(Parser):
    """
    Parses a `comparison` production.
    """

    @classmethod
    def process(cls, tokens):
        # Parses the production:
        #   comparison  ::= expression ( ( '=~~' | '=~' | '^~~' | '^~' |
        #                                  '$~~' | '$~' | '~~' | '~' |
        #                                  '!=~~' | '!=~' | '!^~~' | '!^~' |
        #                                  '!$~~' | '!$~' | '!~~' | '!~' |
        #                                  '<=' | '<' | '>=' |  '>' |
        #                                  '==' | '=' | '!==' | '!=' )
        #                                expression )?
        expression = ExpressionParser << tokens
        symbol_token = tokens.peek(SymbolToken,
                                   ['=~~', '=~', '^~~', '^~',
                                    '$~~', '$~', '~~', '~',
                                    '!=~~', '!=~', '!^~~', '!^~',
                                    '!$~~', '!$~', '!~~', '!~',
                                    '<=', '<', '>=', '>',
                                    '==', '=', '!==', '!='], do_pop=True)
        if symbol_token is None:
            return expression
        symbol = symbol_token.value
        left = expression
        right = ExpressionParser << tokens
        mark = Mark.union(left, right)
        comparison = OperatorSyntax(symbol, left, right, mark)
        return comparison


class ExpressionParser(Parser):
    """
    Parses an `expression` production.
    """

    @classmethod
    def process(cls, tokens):
        # Parses the production:
        #   expression      ::= term ( ( '+' | '-' ) term )*
        expression = TermParser << tokens
        # Here we perform a look-ahead to distinguish between productions:
        #   test            ::= test direction | test application | or_test
        #   direction       ::= ( '+' | '-' )
        #   application     ::= ':' identifier ( or_test | call )?
        # and
        #   expression      ::= term ( ( '+' | '-' ) term )*
        # We know that the FOLLOWS set of `test` consists of the symbols:
        #   ',', ')', and '}',
        # which never start the `term` non-terminal.
        while tokens.peek(SymbolToken, ['+', '-']):
            ahead = 1
            while tokens.peek(SymbolToken, ['+', '-'], ahead=ahead):
                ahead += 1
            if tokens.peek(SymbolToken, [':', ',', ')', '}'], ahead=ahead):
                break
            symbol_token = tokens.pop(SymbolToken, ['+', '-'])
            symbol = symbol_token.value
            left = expression
            right = TermParser << tokens
            mark = Mark.union(left, right)
            expression = OperatorSyntax(symbol, left, right, mark)
        return expression


class TermParser(Parser):
    """
    Parses a `term` production.
    """

    @classmethod
    def process(cls, tokens):
        # Parses the production:
        #   term            ::= factor ( ( '*' | '/' ) factor )*
        expression = FactorParser << tokens
        while (tokens.peek(SymbolToken, ['*'])
               or (tokens.peek(SymbolToken, ['/'], ahead=0)
                   and not tokens.peek(SymbolToken, [':'], ahead=1))):
            symbol_token = tokens.pop(SymbolToken, ['*', '/'])
            symbol = symbol_token.value
            left = expression
            right = FactorParser << tokens
            mark = Mark.union(left, right)
            expression = OperatorSyntax(symbol, left, right, mark)
        return expression


class FactorParser(Parser):
    """
    Parses a `factor` production.
    """

    @classmethod
    def process(cls, tokens):
        # Parses the production:
        #   factor          ::= ( '+' | '-' ) factor | power
        symbol_tokens = []
        while tokens.peek(SymbolToken, ['+', '-']):
            symbol_token = tokens.pop(SymbolToken, ['+', '-'])
            symbol_tokens.append(symbol_token)
        expression = PowerParser << tokens
        while symbol_tokens:
            symbol_token = symbol_tokens.pop()
            symbol = symbol_token.value
            mark = Mark.union(symbol_token, expression)
            expression = OperatorSyntax(symbol, None, expression, mark)
        return expression


class PowerParser(Parser):
    """
    Parses a `power` production.
    """

    @classmethod
    def process(cls, tokens):
        # Parses the production:
        #   power           ::= sieve ( '^' power )?
        expression = SieveParser << tokens
        # Note that there is some grammar ambiguity here: if the sieve
        # contains a filter part, the sieve parser will greedily consume
        # any `^` expression.
        if not tokens.peek(SymbolToken, ['^']):
            return expression
        symbol_token = tokens.pop(SymbolToken, ['^'])
        symbol = symbol_token.value
        left = expression
        right = PowerParser << tokens
        mark = Mark.union(left, right)
        expression = OperatorSyntax(symbol, None, expression, mark)
        return expression


class SieveParser(Parser):
    """
    Parses a `sieve` production.
    """

    @classmethod
    def process(cls, tokens):
        # Parses the production:
        #   sieve           ::= specifier selector? filter?
        expression = SpecifierParser << tokens
        selector = None
        filter = None
        if tokens.peek(SymbolToken, ['{']):
            selector = SelectorParser << tokens
        if tokens.peek(SymbolToken, ['?']):
            tokens.pop(SymbolToken, ['?'])
            filter = TestParser << tokens
        if selector is None and filter is None:
            return expression
        mark = Mark.union(expression, selector, filter)
        expression = SieveSyntax(expression, selector, filter, mark)
        return expression


class SpecifierParser(Parser):
    """
    Parses a `specifier` production.
    """

    @classmethod
    def process(cls, tokens):
        # Parses the productions:
        #   specifier       ::= atom ( '.' identifier call? )* ( '.' '*' )?
        #   call            ::= '(' test? ')'
        #   tests           ::= test ( ',' test )* ','?
        expression = AtomParser << tokens
        while tokens.peek(SymbolToken, ['.'], do_pop=True):
            if tokens.peek(SymbolToken, ['*']):
                symbol_token = tokens.pop(SymbolToken, ['*'])
                wildcard = WildcardSyntax(symbol_token.mark)
                mark = Mark.union(expression, wildcard)
                expression = SpecifierSyntax(expression, wildcard, mark)
                break
            else:
                identifier = IdentifierParser << tokens
                if tokens.peek(SymbolToken, ['(']):
                    tokens.pop(SymbolToken, ['('])
                    arguments = []
                    while not tokens.peek(SymbolToken, [')']):
                        argument = TestParser << tokens
                        arguments.append(argument)
                        if not tokens.peek(SymbolToken, [')']):
                            tokens.pop(SymbolToken, [',', ')'])
                    tail_token = tokens.pop(SymbolToken, [')'])
                    mark = Mark.union(expression, tail_token)
                    expression = FunctionCallSyntax(expression, identifier,
                                                    arguments, mark)
                else:
                    mark = Mark.union(expression, identifier)
                    expression = SpecifierSyntax(expression, identifier, mark)
        return expression


class AtomParser(Parser):
    """
    Parses an `atom` production.
    """

    @classmethod
    def process(cls, tokens):
        # Parses the productions:
        #   atom        ::= '*' | selector | group | identifier call? | literal
        #   call        ::= '(' tests? ')'
        #   tests       ::= tests ( ',' tests )* ','?
        #   literal     ::= STRING | NUMBER
        if tokens.peek(SymbolToken, ['*']):
            symbol_token = tokens.pop(SymbolToken, ['*'])
            wildcard = WildcardSyntax(symbol_token.mark)
            return wildcard
        elif tokens.peek(SymbolToken, ['(']):
            group = GroupParser << tokens
            return group
        elif tokens.peek(SymbolToken, ['{']):
            selector = SelectorParser << tokens
            return selector
        elif tokens.peek(NameToken):
            identifier = IdentifierParser << tokens
            if tokens.peek(SymbolToken, ['(']):
                tokens.pop(SymbolToken, ['('])
                arguments = []
                while not tokens.peek(SymbolToken, [')']):
                    argument = TestParser << tokens
                    arguments.append(argument)
                    if not tokens.peek(SymbolToken, [')']):
                        tokens.pop(SymbolToken, [',', ')'])
                tail_token = tokens.pop(SymbolToken, [')'])
                mark = Mark.union(identifier, tail_token)
                expression = FunctionCallSyntax(None, identifier,
                                                arguments, mark)
                return expression
            else:
                return identifier
        elif tokens.peek(StringToken):
            token = tokens.pop(StringToken)
            return StringSyntax(token.value, token.mark)
        elif tokens.peek(NumberToken):
            token = tokens.pop(NumberToken)
            return NumberSyntax(token.value, token.mark)
        # We expect it to always produce an error message.
        tokens.pop(NameToken)
        # Not reachable.
        assert False


class GroupParser(Parser):
    """
    Parses a `group` production.
    """

    @classmethod
    def process(self, tokens):
        # Parses the production:
        #   group           ::= '(' test ')'
        head_token = tokens.pop(SymbolToken, ['('])
        expression = TestParser << tokens
        tail_token = tokens.pop(SymbolToken, [')'])
        mark = Mark.union(head_token, tail_token)
        group = GroupSyntax(expression, mark)
        return group


class SelectorParser(Parser):
    """
    Parses a `selector` production.
    """

    @classmethod
    def process(cls, tokens):
        # Parses the productions:
        #   selector        ::= '{' tests? '}'
        #   tests           ::= test ( ',' test )* ','?
        head_token = tokens.pop(SymbolToken, ['{'])
        tests = []
        while not tokens.peek(SymbolToken, ['}']):
            test = TestParser << tokens
            tests.append(test)
            if not tokens.peek(SymbolToken, ['}']):
                # We know it's not going to be '}', but we put it into the list
                # of accepted values to generate a better error message.
                tokens.pop(SymbolToken, [',', '}'])
        tail_token = tokens.pop(SymbolToken, ['}'])
        mark = Mark.union(head_token, tail_token)
        selector = SelectorSyntax(tests, mark)
        return selector


class IdentifierParser(Parser):
    """
    Parses an `identifier` production.
    """

    @classmethod
    def process(cls, tokens):
        # Parses the production:
        #   identifier      ::= NAME
        name_token = tokens.pop(NameToken)
        identifier = IdentifierSyntax(name_token.value, name_token.mark)
        return identifier


def parse(input, Parser=QueryParser):
    """
    Parses the input HTSQL query; returns the corresponding syntax node.

    In case of syntax errors, may raise :exc:`htsql.tr.error.ScanError`
    or :exc:`htsql.tr.error.ParseError`.

    `input` (a string)
        An HTSQL query or an HTSQL expression.

    `Parser` (a subclass of :class:`Parser`)
        The parser to use for parsing the input expression.  By default,
        `input` is treated as a complete HTSQL query.
    """
    parser = Parser(input)
    return parser.parse()


