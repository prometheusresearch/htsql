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
from .syntax import (QuerySyntax, SegmentSyntax, FormatSyntax, SelectorSyntax,
                     OperatorSyntax, SpecifierSyntax, TransformSyntax,
                     FunctionSyntax, GroupSyntax, IdentifierSyntax,
                     WildcardSyntax, ComplementSyntax, ReferenceSyntax,
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

        query           ::= '/' ( segment ( '/' format )? )?
        segment         ::= test
        format          ::= ':' identifier

        test            ::= or_test ( direction | application )*
        direction       ::= '+' | '-'
        application     ::= ':' identifier ( or_test | call )?

        or_test         ::= and_test ( '|' and_test )*
        and_test        ::= implies_test ( '&' implies_test )*
        implies_test    ::= not_test ( '->' not_test )?
        not_test        ::= '!' not_test | comparison

        comparison      ::= expression ( ( '=~~' | '=~' | '^~~' | '^~' |
                                           '$~~' | '$~' | '~~' | '~' |
                                           '!=~~' | '!=~' | '!^~~' | '!^~' |
                                           '!$~~' | '!$~' | '!~~' | '!~' |
                                           '<=' | '<' | '>=' |  '>' |
                                           '==' | '=' | '!==' | '!=' )
                                         expression )?

        expression      ::= term ( ( '+' | '-' ) term )*
        term            ::= factor ( ( '*' | '/' ) factor )*
        factor          ::= ( '+' | '-' ) factor | quotient   
        quotient        ::= sieve ( '^' sieve )?

        sieve           ::= link ( '?' or_test )?
        link            ::= assignment ( '->' assignment )
        assignment      ::= specifier ( ':=' test )?
        specifier       ::= selection ( '.' selection )*
        selection       ::= atom selector?
        atom            ::= '*' index? | '^' | selector | group |
                            identifier call? | reference | literal
        index           ::= NUMBER | '(' NUMBER ')'

        group           ::= '(' test ')'
        call            ::= '(' tests? ')'
        selector        ::= '{' tests? '}'
        tests           ::= test ( ',' test )* ','?
        reference       ::= '$' identifier

        identifier      ::= NAME
        literal         ::= STRING | NUMBER

    Note that this grammar is almost LL(1); one notable exception is
    the postfix ``+`` and ``-`` operators.
    """

    @classmethod
    def process(cls, tokens):
        # Parse the production:
        #   query           ::= '/' segment? ( '/' format )?
        head_token = tokens.pop(SymbolToken, ['/'])
        segment = None
        format = None
        if not tokens.peek(EndToken):
            if tokens.peek(SymbolToken, [':']):
                format = FormatParser << tokens
            else:
                segment = SegmentParser << tokens
                if tokens.peek(SymbolToken, ['/']):
                    tokens.pop(SymbolToken, ['/'])
                    format = FormatParser << tokens
        mark = Mark.union(head_token, segment, format)
        query = QuerySyntax(segment, format, mark)
        return query


class SegmentParser(Parser):
    """
    Parses a `segment` production.
    """

    @classmethod
    def process(cls, tokens):
        # Parses the production:
        #   segment         ::= test
        branch = TestParser << tokens
        mark = branch.mark
        segment = SegmentSyntax(branch, mark)
        return segment


class FormatParser(Parser):
    """
    Parses a `format` production.
    """

    @classmethod
    def process(cls, tokens):
        # Parses the productions:
        #   format          ::= ':' identifier
        head_token = tokens.pop(SymbolToken, [':'])
        identifier = IdentifierParser << tokens
        mark = Mark.union(head_token, identifier)
        format = FormatSyntax(identifier, mark)
        return format


class TestParser(Parser):
    """
    Parses a `test` production.
    """

    @classmethod
    def process(cls, tokens):
        # Parses the productions:
        #   test            ::= or_test ( direction | application )*
        #   direction       ::= '+' | '-'
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
                lbranch = test
                rbranches = []
                if tokens.peek(SymbolToken, ['(']):
                    tokens.pop(SymbolToken, ['('])
                    while not tokens.peek(SymbolToken, [')']):
                        rbranch = TestParser << tokens
                        rbranches.append(rbranch)
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
                        rbranch = OrTestParser << tokens
                        rbranches.append(rbranch)
                    mark = Mark.union(test, identifier, *rbranches)
                test = TransformSyntax(identifier, lbranch, rbranches, mark)
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
            lbranch = test
            rbranch = AndTestParser << tokens
            mark = Mark.union(lbranch, rbranch)
            test = OperatorSyntax(symbol, lbranch, rbranch, mark)
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
            lbranch = test
            rbranch = ImpliesTestParser << tokens
            mark = Mark.union(lbranch, rbranch)
            test = OperatorSyntax(symbol, lbranch, rbranch, mark)
        return test


class ImpliesTestParser(Parser):
    """
    Parses an `implies_test` production.
    """

    @classmethod
    def process(cls, tokens):
        # Parses the production:
        #   implies_test    ::= not_test ( '->' not_test )?
        test = NotTestParser << tokens
        if not tokens.peek(SymbolToken, ['->']):
            return test
        symbol_token = tokens.pop(SymbolToken, ['->'])
        symbol = symbol_token.value
        lbranch = test
        rbranch = NotTestParser << tokens
        mark = Mark.union(lbranch, rbranch)
        test = OperatorSyntax(symbol, lbranch, rbranch, mark)
        return test


class NotTestParser(Parser):
    """
    Parses a `not_test` production.
    """

    @classmethod
    def process(cls, tokens):
        # Parses the production:
        #   not_test        ::= '!' not_test | comparison
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
        lbranch = expression
        rbranch = ExpressionParser << tokens
        mark = Mark.union(lbranch, rbranch)
        comparison = OperatorSyntax(symbol, lbranch, rbranch, mark)
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
        #   test            ::= or_test ( direction | application )*
        #   direction       ::= '+' | '-'
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
            lbranch = expression
            rbranch = TermParser << tokens
            mark = Mark.union(lbranch, rbranch)
            expression = OperatorSyntax(symbol, lbranch, rbranch, mark)
        return expression


class TermParser(Parser):
    """
    Parses a `term` production.
    """

    @classmethod
    def process(cls, tokens):
        # Parses the production:
        #   term            ::= factor ( ( '*' | '/' ) factor )*
        term = FactorParser << tokens
        while (tokens.peek(SymbolToken, ['*'])
               or (tokens.peek(SymbolToken, ['/'], ahead=0)
                   and not tokens.peek(SymbolToken, [':'], ahead=1))):
            symbol_token = tokens.pop(SymbolToken, ['*', '/'])
            symbol = symbol_token.value
            lbranch = term
            rbranch = FactorParser << tokens
            mark = Mark.union(lbranch, rbranch)
            term = OperatorSyntax(symbol, lbranch, rbranch, mark)
        return term


class FactorParser(Parser):
    """
    Parses a `factor` production.
    """

    @classmethod
    def process(cls, tokens):
        # Parses the production:
        #   factor          ::= ( '+' | '-' ) factor | quotient
        symbol_tokens = []
        while tokens.peek(SymbolToken, ['+', '-']):
            symbol_token = tokens.pop(SymbolToken, ['+', '-'])
            symbol_tokens.append(symbol_token)
        factor = QuotientParser << tokens
        while symbol_tokens:
            symbol_token = symbol_tokens.pop()
            symbol = symbol_token.value
            mark = Mark.union(symbol_token, factor)
            factor = OperatorSyntax(symbol, None, factor, mark)
        return factor


class QuotientParser(Parser):
    """
    Parses a `quotient` production.
    """

    @classmethod
    def process(cls, tokens):
        # Parses the production:
        #   quotient        ::= sieve ( '^' sieve )?
        quotient = SieveParser << tokens
        # Note that there is some grammar ambiguity here: if the sieve
        # contains a filter part, the sieve parser will greedily consume
        # any `^` expression.
        if not tokens.peek(SymbolToken, ['^']):
            return quotient
        symbol_token = tokens.pop(SymbolToken, ['^'])
        symbol = symbol_token.value
        lbranch = quotient
        rbranch = SieveParser << tokens
        mark = Mark.union(lbranch, rbranch)
        quotient = OperatorSyntax(symbol, lbranch, rbranch, mark)
        return quotient


class SieveParser(Parser):
    """
    Parses a `sieve` production.
    """

    @classmethod
    def process(cls, tokens):
        # Parses the production:
        #   sieve           ::= link ( '?' or_test )?
        sieve = LinkParser << tokens
        if tokens.peek(SymbolToken, ['?']):
            symbol_token = tokens.pop(SymbolToken, ['?'])
            symbol = symbol_token.value
            lbranch = sieve
            rbranch = OrTestParser << tokens
            mark = Mark.union(lbranch, rbranch)
            sieve = OperatorSyntax(symbol, lbranch, rbranch, mark)
        return sieve


class LinkParser(Parser):
    """
    Parses a `link` production.
    """

    @classmethod
    def process(cls, tokens):
        # Parses the production:
        #   link            ::= assignment ( '->' assignment )
        link = AssignmentParser << tokens
        if tokens.peek(SymbolToken, ['->']):
            symbol_token = tokens.pop(SymbolToken, ['->'])
            symbol = symbol_token.value
            lbranch = link
            rbranch = AssignmentParser << tokens
            mark = Mark.union(lbranch, rbranch)
            link = OperatorSyntax(symbol, lbranch, rbranch, mark)
        return link


class AssignmentParser(Parser):
    """
    Parses an `assignment` production.
    """

    @classmethod
    def process(cls, tokens):
        # Parses the production:
        #   assignment      ::= specifier ( ':=' test )?
        assignment = SpecifierParser << tokens
        if tokens.peek(SymbolToken, [':=']):
            symbol_token = tokens.pop(SymbolToken, [':='])
            symbol = symbol_token.value
            lbranch = assignment
            rbranch = TestParser << tokens
            mark = Mark.union(lbranch, rbranch)
            assignment = OperatorSyntax(symbol, lbranch, rbranch, mark)
        return assignment


class SpecifierParser(Parser):
    """
    Parses a `specifier` production.
    """

    @classmethod
    def process(cls, tokens):
        # Parses the production:
        #   specifier       ::= selection ( '.' selection )*
        specifier = SelectionParser << tokens
        while tokens.peek(SymbolToken, ['.']):
            symbol_token = tokens.pop(SymbolToken, ['.'])
            symbol = symbol_token.value
            lbranch = specifier
            rbranch = SelectionParser << tokens
            mark = Mark.union(lbranch, rbranch)
            specifier = SpecifierSyntax(symbol, lbranch, rbranch, mark)
        return specifier


class SelectionParser(Parser):
    """
    Parses a `selection` production.
    """

    @classmethod
    def process(cls, tokens):
        # Parses the production:
        #   selection       ::= atom selector?
        selection = AtomParser << tokens
        if tokens.peek(SymbolToken, ['{']):
            tokens.pop(SymbolToken, ['{'])
            lbranch = selection
            rbranches = []
            while not tokens.peek(SymbolToken, ['}']):
                rbranch = TestParser << tokens
                rbranches.append(rbranch)
                if not tokens.peek(SymbolToken, ['}']):
                    # We know it's not going to be '}', but we put it into the list
                    # of accepted values to generate a better error message.
                    tokens.pop(SymbolToken, [',', '}'])
            tail_token = tokens.pop(SymbolToken, ['}'])
            mark = Mark.union(selection, tail_token)
            selection = SelectorSyntax(lbranch, rbranches, mark)
        return selection


class AtomParser(Parser):
    """
    Parses an `atom` production.
    """

    @classmethod
    def process(cls, tokens):
        # Parses the productions:
        #   atom            ::= '*' index? | '^' | selector | group |
        #                       identifier call? | reference | literal
        #   index           ::= NUMBER | '(' NUMBER ')'
        #   call            ::= '(' tests? ')'
        #   tests           ::= test ( ',' test )* ','?
        #   reference       ::= '$' identifier
        #   literal         ::= STRING | NUMBER
        if tokens.peek(SymbolToken, ['*']):
            symbol_token = tokens.pop(SymbolToken, ['*'])
            index = None
            if tokens.peek(NumberToken):
                index_token = tokens.pop(NumberToken)
                index = NumberSyntax(index_token.value, index_token.mark)
            elif tokens.peek(SymbolToken, ['(']):
                open_token = tokens.pop(SymbolToken, '(')
                index_token = tokens.pop(NumberToken)
                close_token = tokens.pop(SymbolToken, ')')
                mark = Mark.union(open_token, close_token)
                index = NumberSyntax(index_token.value, mark)
            mark = Mark.union(symbol_token, index)
            wildcard = WildcardSyntax(index, mark)
            return wildcard
        if tokens.peek(SymbolToken, ['^']):
            symbol_token = tokens.pop(SymbolToken, ['^'])
            complement = ComplementSyntax(symbol_token.mark)
            return complement
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
                branches = []
                while not tokens.peek(SymbolToken, [')']):
                    branch = TestParser << tokens
                    branches.append(branch)
                    if not tokens.peek(SymbolToken, [')']):
                        tokens.pop(SymbolToken, [',', ')'])
                tail_token = tokens.pop(SymbolToken, [')'])
                mark = Mark.union(identifier, tail_token)
                function = FunctionSyntax(identifier, branches, mark)
                return function
            else:
                return identifier
        elif tokens.peek(SymbolToken, ['$']):
            head_token = tokens.pop(SymbolToken, ['$'])
            identifier = IdentifierParser << tokens
            mark = Mark.union(head_token, identifier)
            reference = ReferenceSyntax(identifier, mark)
            return reference
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
        branch = TestParser << tokens
        tail_token = tokens.pop(SymbolToken, [')'])
        mark = Mark.union(head_token, tail_token)
        group = GroupSyntax(branch, mark)
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
        branches = []
        while not tokens.peek(SymbolToken, ['}']):
            branch = TestParser << tokens
            branches.append(branch)
            if not tokens.peek(SymbolToken, ['}']):
                # We know it's not going to be '}', but we put it into the list
                # of accepted values to generate a better error message.
                tokens.pop(SymbolToken, [',', '}'])
        tail_token = tokens.pop(SymbolToken, ['}'])
        mark = Mark.union(head_token, tail_token)
        selector = SelectorSyntax(None, branches, mark)
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


