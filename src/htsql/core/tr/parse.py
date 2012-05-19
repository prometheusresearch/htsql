#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql.core.tr.parse`
==========================

This module implements the HTSQL parser.
"""


from ..mark import Mark
from .scan import scan
from .token import (NameToken, StringToken, UnquotedStringToken, NumberToken,
        SymbolToken, EndToken)
from .syntax import (QuerySyntax, SegmentSyntax, CommandSyntax, SelectorSyntax,
        FunctionSyntax, MappingSyntax, OperatorSyntax, QuotientSyntax,
        SieveSyntax, LinkSyntax, HomeSyntax, AssignmentSyntax, SpecifierSyntax,
        LocatorSyntax, LocationSyntax, GroupSyntax, IdentifierSyntax,
        WildcardSyntax, ComplementSyntax, ReferenceSyntax, StringSyntax,
        UnquotedStringSyntax, NumberSyntax)
from .error import ParseError


class Parser(object):
    """
    Implements an HTSQL parser.

    A parser takes a stream of tokens from the HTSQL scanner and
    produces a node of a syntax tree.

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
        assert isinstance(input, (str, unicode))
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
        if not tokens.pop(EndToken, do_force=False):
            token = tokens.pop()
            raise ParseError("expected the end of query; got '%s'" % token,
                             token.mark)
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

        `tokens` (:class:`htsql.core.tr.scan.TokenStream`)
            The stream of tokens to parse.
        """
        # Override in subclasses.
        raise NotImplementedError()


class QueryParser(Parser):
    """
    Parses an HTSQL query.

    Here is the grammar of HTSQL::

        input           ::= segment END

        segment         ::= '/' ( top command* )?
        command         ::= '/' ':' identifier ( '/' top? | call | flow )?

        top             ::= flow ( direction | mapping )*
        direction       ::= '+' | '-'
        mapping         ::= ':' identifier ( flow | call )?

        flow            ::= disjunction ( sieve | quotient | selection )*
        sieve           ::= '?' disjunction
        quotient        ::= '^' disjunction
        selection       ::= selector ( '.' atom )*

        disjunction     ::= conjunction ( '|' conjunction )*
        conjunction     ::= negation ( '&' negation )*
        negation        ::= '!' negation | comparison

        comparison      ::= expression ( ( '~' | '!~' |
                                           '<=' | '<' | '>=' |  '>' |
                                           '==' | '=' | '!==' | '!=' )
                                         expression )?

        expression      ::= term ( ( '+' | '-' ) term )*
        term            ::= factor ( ( '*' | '/' ) factor )*
        factor          ::= ( '+' | '-' ) factor | pointer

        pointer         ::= specifier ( link | assignment )?
        link            ::= '->' flow
        assignment      ::= ':=' top

        specifier       ::= locator ( '.' locator )*
        locator         ::= atom ( '[' location ']' )?
        location        ::= label ( '.' label )*
        label           ::= STRING | '(' location ')' | '[' location ']'

        atom            ::= '@' atom | '*' index? | '^' | selector | group |
                            identifier call? | reference | literal
        index           ::= NUMBER | '(' NUMBER ')'

        group           ::= '(' segment | top ')'
        call            ::= '(' arguments? ')'
        selector        ::= '{' arguments? '}'
        arguments       ::= argument ( ',' argument )* ','?
        argument        ::= segment | top
        reference       ::= '$' identifier

        identifier      ::= NAME
        literal         ::= STRING | NUMBER

    Note that this grammar is almost LL(1); one notable exception is
    the postfix ``+`` and ``-`` operators.
    """

    @classmethod
    def process(cls, tokens):
        # Expect:
        #   input       ::= segment END
        segment = SegmentParser << tokens
        return QuerySyntax(segment, segment.mark)


class SegmentParser(Parser):
    """
    Parses a `segment` production.
    """

    @classmethod
    def process(cls, tokens):
        # Expect:
        #   segment     ::= '/' ( top command* )?
        #   command     ::= '/' ':' identifier ( '/' top? | call | flow )?
        head_token = tokens.pop(SymbolToken, [u'/'], do_force=False)
        if not head_token:
            token = tokens.pop()
            raise ParseError("query must start with symbol '/'", token.mark)
        branch = None
        if not (tokens.peek(EndToken) or
                tokens.peek(SymbolToken, [u',', u')', u'}'])):
            branch = TopParser << tokens
            while tokens.peek(SymbolToken, [u'/']):
                tail_token = tokens.pop(SymbolToken, [u'/'])
                if not (tokens.peek(EndToken) or
                        tokens.peek(SymbolToken, [u',', u')', u'}'])):
                    if not (tokens.pop(SymbolToken, [u':'], do_force=False) and
                            tokens.peek(NameToken)):
                        raise ParseError("symbol '/' must be followed by ':'"
                                         " and an identifier", tail_token.mark)
                    mark = Mark.union(head_token, branch)
                    lbranch = SegmentSyntax(branch, mark)
                    identifier = IdentifierParser << tokens
                    rbranches = []
                    if not (tokens.peek(EndToken) or
                            tokens.peek(SymbolToken, [u',', u')', u'}'])):
                        if tokens.peek(SymbolToken, [u'/']):
                            if not tokens.peek(SymbolToken, [u':'], ahead=1):
                                rbranch_token = tokens.pop(SymbolToken, [u'/'])
                                rbranch = None
                                if not (tokens.peek(EndToken) or
                                        tokens.peek(SymbolToken,
                                                    [u',', u')', u'}'])):
                                    rbranch = TopParser << tokens
                                mark = Mark.union(rbranch_token, rbranch)
                                rbranch = SegmentSyntax(rbranch, mark)
                                rbranches.append(rbranch)
                        elif tokens.peek(SymbolToken, [u'(']):
                            open_token = tokens.pop(SymbolToken, [u'('])
                            while not tokens.peek(SymbolToken, [u')']):
                                if tokens.peek(SymbolToken, [u'/']):
                                    rbranch = SegmentParser << tokens
                                else:
                                    rbranch = TopParser << tokens
                                rbranches.append(rbranch)
                                if tokens.peek(SymbolToken, [u',']):
                                    tokens.pop(SymbolToken, [u','])
                                elif not tokens.peek(SymbolToken, [u')']):
                                    mark = Mark.union(open_token, *rbranches)
                                    raise ParseError("cannot find a matching"
                                                     " ')'", mark)
                            tail_token = tokens.pop(SymbolToken, [u')'])
                        else:
                            rbranch = FlowParser << tokens
                            rbranches.append(rbranch)
                    mark = Mark.union(lbranch, identifier, tail_token,
                                      *rbranches)
                    branch = CommandSyntax(identifier, lbranch, rbranches, mark)
        mark = Mark.union(head_token, branch)
        segment = SegmentSyntax(branch, mark)
        return segment


class TopParser(Parser):
    """
    Parses a `top` production.
    """

    @classmethod
    def process(cls, tokens):
        # Expect:
        #   top         ::= flow ( direction | mapping )*
        #   direction   ::= '+' | '-'
        #   mapping     ::= ':' identifier ( flow | call )?
        #   FOLLOW(mapping) = ['+','-',':',',',')','}','/']
        top = FlowParser << tokens
        while tokens.peek(SymbolToken, [u'+', u'-', u':']):
            # Parse `direction` decorator.
            if tokens.peek(SymbolToken, [u'+', u'-']):
                symbol_token = tokens.pop(SymbolToken, [u'+', u'-'])
                symbol = symbol_token.value
                mark = Mark.union(top, symbol_token)
                top = OperatorSyntax(symbol, top, None, mark)
            # Parse `mapping` application.
            else:
                symbol_token = tokens.pop(SymbolToken, [u':'])
                if not tokens.peek(NameToken):
                    raise ParseError("symbol ':' must be followed by"
                                     " an identifier", symbol_token.mark)
                identifier = IdentifierParser << tokens
                lbranch = top
                rbranches = []
                # Mapping parameters in parentheses.
                if tokens.peek(SymbolToken, [u'(']):
                    open_token = tokens.pop(SymbolToken, [u'('])
                    while not tokens.peek(SymbolToken, [u')']):
                        if tokens.peek(SymbolToken, [u'/']):
                            rbranch = SegmentParser << tokens
                        else:
                            rbranch = TopParser << tokens
                        rbranches.append(rbranch)
                        if tokens.peek(SymbolToken, [u',']):
                            tokens.pop(SymbolToken, [u','])
                        elif not tokens.peek(SymbolToken, [u')']):
                            mark = Mark.union(open_token, *rbranches)
                            raise ParseError("cannot find a matching ')'",
                                             mark)
                    tail_token = tokens.pop(SymbolToken, [u')'])
                    mark = Mark.union(top, tail_token)
                # No parenthesis: either no parameters or a single parameter.
                else:
                    # Determine whether the mapping has a parameter or not.
                    # If not, must be followed by one of: `:` (next mapping),
                    # `,`, `)`, `}` (punctuation), `/` (command) or `+`, `-`
                    # (direction decorators).  We skip through `+` and `-`
                    # since they could also start a parameter as an unary
                    # prefix operator.  For `/`, we need to distinguish between
                    # a trailing `/`, a segment expression and a command.
                    ahead = 0
                    while tokens.peek(SymbolToken, [u'+', u'-'], ahead=ahead):
                        ahead += 1
                    if tokens.peek(SymbolToken, [u'/'], ahead=ahead):
                        ahead += 1
                    if not (tokens.peek(SymbolToken, [u':', u',', u')', u'}'],
                                        ahead=ahead) or
                            tokens.peek(EndToken, ahead=ahead)):
                        rbranch = FlowParser << tokens
                        rbranches.append(rbranch)
                    mark = Mark.union(top, identifier, *rbranches)
                top = MappingSyntax(identifier, lbranch, rbranches, mark)
        return top


class FlowParser(Parser):
    """
    Parses a `flow` production.
    """

    @classmethod
    def process(cls, tokens):
        # Expect:
        #   flow        ::= disjunction ( sieve | quotient | selection )*
        #   sieve       ::= '?' disjunction
        #   quotient    ::= '^' disjunction
        #   selection   ::= selector ( '.' atom )*
        flow = DisjunctionParser << tokens
        while tokens.peek(SymbolToken, [u'?', u'^', u'{']):
            if tokens.peek(SymbolToken, [u'?'], do_pop=True):
                lbranch = flow
                rbranch = DisjunctionParser << tokens
                mark = Mark.union(lbranch, rbranch)
                flow = SieveSyntax(lbranch, rbranch, mark)
            elif tokens.peek(SymbolToken, [u'^'], do_pop=True):
                lbranch = flow
                rbranch = DisjunctionParser << tokens
                mark = Mark.union(lbranch, rbranch)
                flow = QuotientSyntax(lbranch, rbranch, mark)
            elif tokens.peek(SymbolToken, [u'{']):
                open_token = tokens.pop(SymbolToken, [u'{'])
                lbranch = flow
                rbranches = []
                while not tokens.peek(SymbolToken, [u'}']):
                    if tokens.peek(SymbolToken, [u'/']):
                        rbranch = SegmentParser << tokens
                    else:
                        rbranch = TopParser << tokens
                    rbranches.append(rbranch)
                    if tokens.peek(SymbolToken, [u',']):
                        tokens.pop(SymbolToken, [u','])
                    elif not tokens.peek(SymbolToken, [u'}']):
                        mark = Mark.union(open_token, *rbranches)
                        raise ParseError("cannot find a matching '}'",
                                         mark)
                tail_token = tokens.pop(SymbolToken, [u'}'])
                mark = Mark.union(flow, tail_token)
                flow = SelectorSyntax(lbranch, rbranches, mark)
                while tokens.peek(SymbolToken, [u'.'], do_pop=True):
                    lbranch = flow
                    rbranch = AtomParser << tokens
                    mark = Mark.union(flow, rbranch)
                    flow = SpecifierSyntax(lbranch, rbranch, mark)
            else:
                # Not reachable.
                assert False
        return flow


class DisjunctionParser(Parser):
    """
    Parses a `disjunction` production.
    """

    @classmethod
    def process(cls, tokens):
        # Parses the production:
        #   disjunction ::= conjunction ( '|' conjunction )*
        test = ConjunctionParser << tokens
        while tokens.peek(SymbolToken, [u'|']):
            symbol_token = tokens.pop(SymbolToken, [u'|'])
            symbol = symbol_token.value
            lbranch = test
            rbranch = ConjunctionParser << tokens
            mark = Mark.union(lbranch, rbranch)
            test = OperatorSyntax(symbol, lbranch, rbranch, mark)
        return test


class ConjunctionParser(Parser):
    """
    Parses a `conjunction` production.
    """

    @classmethod
    def process(cls, tokens):
        # Expect:
        #   conjunction ::= negation ( '&' negation )*
        test = NegationParser << tokens
        while tokens.peek(SymbolToken, [u'&']):
            symbol_token = tokens.pop(SymbolToken, [u'&'])
            symbol = symbol_token.value
            lbranch = test
            rbranch = NegationParser << tokens
            mark = Mark.union(lbranch, rbranch)
            test = OperatorSyntax(symbol, lbranch, rbranch, mark)
        return test


class NegationParser(Parser):
    """
    Parses a `negation` production.
    """

    @classmethod
    def process(cls, tokens):
        # Expect:
        #   negation    ::= '!' negation | comparison
        symbol_tokens = []
        while tokens.peek(SymbolToken, [u'!']):
            symbol_token = tokens.pop(SymbolToken, [u'!'])
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
        # Expect:
        #   comparison  ::= expression ( ( '~' | '!~' |
        #                                  '<=' | '<' | '>=' |  '>' |
        #                                  '==' | '=' | '!==' | '!=' )
        #                                expression )?
        expression = ExpressionParser << tokens
        symbol_token = tokens.peek(SymbolToken,
                                   [u'~', u'!~', u'<=', u'<', u'>=', u'>',
                                    u'==', u'=', u'!==', u'!='], do_pop=True)
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
        # Expect:
        #   expression  ::= term ( ( '+' | '-' ) term )*
        expression = TermParser << tokens
        # Do a look-ahead to distinguish between infix and postfix `+` or `-`.
        while tokens.peek(SymbolToken, [u'+', u'-']):
            ahead = 1
            while tokens.peek(SymbolToken, [u'+', u'-'], ahead=ahead):
                ahead += 1
            if tokens.peek(SymbolToken, [u':', u',', u')', u'}'], ahead=ahead):
                break
            symbol_token = tokens.pop(SymbolToken, [u'+', u'-'])
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
        # Expect:
        #   term        ::= factor ( ( '*' | '/' ) factor )*
        term = FactorParser << tokens
        while (tokens.peek(SymbolToken, [u'*'])
               or (tokens.peek(SymbolToken, [u'/'], ahead=0)
                   and not (tokens.peek(EndToken, ahead=1) or
                            tokens.peek(SymbolToken, [u':', u',', u')', u'}'],
                                        ahead=1)))):
            symbol_token = tokens.pop(SymbolToken, [u'*', u'/'])
            symbol = symbol_token.value
            lbranch = term
            rbranch = FactorParser << tokens
            mark = Mark.union(lbranch, rbranch)
            term = OperatorSyntax(symbol, lbranch, rbranch, mark)
        return term


class FactorParser(Parser):
    """
    Parses a `pointer` production.
    """

    @classmethod
    def process(cls, tokens):
        # Expect:
        #   factor      ::= ( '+' | '-' ) factor | pointer
        symbol_tokens = []
        while tokens.peek(SymbolToken, [u'+', u'-']):
            symbol_token = tokens.pop(SymbolToken, [u'+', u'-'])
            symbol_tokens.append(symbol_token)
        factor = PointerParser << tokens
        while symbol_tokens:
            symbol_token = symbol_tokens.pop()
            symbol = symbol_token.value
            mark = Mark.union(symbol_token, factor)
            factor = OperatorSyntax(symbol, None, factor, mark)
        return factor


class PointerParser(Parser):
    """
    Parses a `pointer` production.
    """

    @classmethod
    def process(cls, tokens):
        # Expect:
        #   pointer     ::= specifier ( link | assignment )?
        #   link        ::= '->' flow
        #   assignment  ::= ':=' top
        pointer = SpecifierParser << tokens
        if tokens.peek(SymbolToken, [u'->'], do_pop=True):
            lbranch = pointer
            rbranch = FlowParser << tokens
            mark = Mark.union(lbranch, rbranch)
            pointer = LinkSyntax(lbranch, rbranch, mark)
        elif tokens.peek(SymbolToken, [u':='], do_pop=True):
            lbranch = pointer
            rbranch = TopParser << tokens
            mark = Mark.union(lbranch, rbranch)
            pointer = AssignmentSyntax(lbranch, rbranch, mark)
        return pointer


class SpecifierParser(Parser):
    """
    Parses a `specifier` production.
    """

    @classmethod
    def process(cls, tokens):
        # Expect:
        #   specifier   ::= locator ( '.' locator )*
        specifier = LocatorParser << tokens
        while tokens.peek(SymbolToken, [u'.']):
            tokens.pop(SymbolToken, [u'.'])
            lbranch = specifier
            rbranch = LocatorParser << tokens
            mark = Mark.union(lbranch, rbranch)
            specifier = SpecifierSyntax(lbranch, rbranch, mark)
        return specifier


class LocatorParser(Parser):
    """
    Parses a `locator` production.
    """

    @classmethod
    def process(cls, tokens):
        # Expect:
        #   locator         ::= atom ( '[' location ']' )?
        locator = AtomParser << tokens
        if tokens.peek(SymbolToken, [u'[']):
            location = LocationParser << tokens
            mark = Mark.union(locator, location)
            locator = LocatorSyntax(locator, location, mark)
        return locator


class LocationParser(Parser):
    """
    Parses a `location` production.
    """

    @classmethod
    def process(cls, tokens):
        # Expect:
        #   location        ::= label ( '.' label )*
        open_token = tokens.pop(SymbolToken, [u'[', u'('])
        labels = [LabelParser << tokens]
        while tokens.peek(SymbolToken, [u'.']):
            tokens.pop(SymbolToken, [u'.'])
            labels.append(LabelParser << tokens)
        if open_token.value == u'[' and not tokens.peek(SymbolToken, [u']']):
            mark = Mark.union(open_token, *labels)
            raise ParseError("cannot find a matching ']'", mark)
        elif open_token.value == u'(' and not tokens.peek(SymbolToken, [u')']):
            mark = Mark.union(open_token, *labels)
            raise ParseError("cannot find a matching ')'", mark)
        close_token = tokens.pop(SymbolToken, [u']', u')'])
        mark = Mark.union(open_token, close_token)
        location = LocationSyntax(labels, mark)
        return location


class LabelParser(Parser):
    """
    Parses a `label` production.
    """

    @classmethod
    def process(cls, tokens):
        # Expect:
        #   label           ::= STRING | '(' location ')' | '[' location ']'
        if tokens.peek(UnquotedStringToken):
            token = tokens.pop(UnquotedStringToken)
            return UnquotedStringSyntax(token.value, token.mark)
        elif tokens.peek(StringToken):
            token = tokens.pop(StringToken)
            return StringSyntax(token.value, token.mark)
        elif tokens.peek(SymbolToken, [u'[', u'(']):
            label = LocationParser << tokens
            return label
        if tokens.peek(EndToken):
            token = tokens.pop(EndToken)
            raise ParseError("unexpected end of query", token.mark)
        else:
            token = tokens.pop()
            raise ParseError("unexpected symbol '%s'" % token, token.mark)


class AtomParser(Parser):
    """
    Parses an `atom` production.
    """

    @classmethod
    def process(cls, tokens):
        # Expect:
        #   atom        ::= '@' atom | '*' index? | '^' | selector | group |
        #                   identifier call? | reference | literal
        #   index       ::= NUMBER | '(' NUMBER ')'
        #   call        ::= '(' arguments? ')'
        #   arguments   ::= argument ( ',' argument )* ','?
        #   argument    ::= segment | top
        #   reference   ::= '$' identifier
        #   literal     ::= STRING | NUMBER

        # A home indicator.
        if tokens.peek(SymbolToken, [u'@']):
            symbol_token = tokens.pop(SymbolToken, [u'@'])
            atom = AtomParser << tokens
            mark = Mark.union(symbol_token, atom)
            home = HomeSyntax(atom, mark)
            return home
        # A wildcard expression.
        if tokens.peek(SymbolToken, [u'*']):
            symbol_token = tokens.pop(SymbolToken, [u'*'])
            index = None
            if tokens.peek(NumberToken):
                index_token = tokens.pop(NumberToken)
                index = NumberSyntax(index_token.value, index_token.mark)
            mark = Mark.union(symbol_token, index)
            wildcard = WildcardSyntax(index, mark)
            return wildcard
        # A complement atom.
        if tokens.peek(SymbolToken, [u'^']):
            symbol_token = tokens.pop(SymbolToken, [u'^'])
            complement = ComplementSyntax(symbol_token.mark)
            return complement
        # An expression in parentheses.
        elif tokens.peek(SymbolToken, [u'(']):
            group = GroupParser << tokens
            return group
        # A selector.
        elif tokens.peek(SymbolToken, [u'{']):
            selector = SelectorParser << tokens
            return selector
        # An identifier or a function call.
        elif tokens.peek(NameToken):
            identifier = IdentifierParser << tokens
            if tokens.peek(SymbolToken, [u'(']):
                open_token = tokens.pop(SymbolToken, [u'('])
                branches = []
                while not tokens.peek(SymbolToken, [u')']):
                    if tokens.peek(SymbolToken, [u'/']):
                        rbranch = SegmentParser << tokens
                    else:
                        rbranch = TopParser << tokens
                    branches.append(rbranch)
                    if tokens.peek(SymbolToken, [u',']):
                        tokens.pop(SymbolToken, [u','])
                    elif not tokens.peek(SymbolToken, [u')']):
                        mark = Mark.union(open_token, *branches)
                        raise ParseError("cannot find a matching ')'", mark)
                tail_token = tokens.pop(SymbolToken, [u')'])
                mark = Mark.union(identifier, tail_token)
                function = FunctionSyntax(identifier, branches, mark)
                return function
            else:
                return identifier
        # A reference.
        elif tokens.peek(SymbolToken, [u'$']):
            head_token = tokens.pop(SymbolToken, [u'$'])
            if not tokens.peek(NameToken):
                raise ParseError("symbol '$' must be followed by an identifier",
                                 head_token.mark)
            identifier = IdentifierParser << tokens
            mark = Mark.union(head_token, identifier)
            reference = ReferenceSyntax(identifier, mark)
            return reference
        # A string literal.
        elif tokens.peek(StringToken):
            token = tokens.pop(StringToken)
            return StringSyntax(token.value, token.mark)
        # A numeric literal.
        elif tokens.peek(NumberToken):
            token = tokens.pop(NumberToken)
            return NumberSyntax(token.value, token.mark)
        # Can't find anything suitable; produce an error message.
        if tokens.peek(EndToken):
            token = tokens.pop(EndToken)
            raise ParseError("unexpected end of query", token.mark)
        else:
            token = tokens.pop()
            raise ParseError("unexpected symbol '%s'" % token, token.mark)
        # Not reachable.
        assert False


class GroupParser(Parser):
    """
    Parses a `group` production.
    """

    @classmethod
    def process(self, tokens):
        # Expect:
        #   group       ::= '(' segment | top ')'
        head_token = tokens.pop(SymbolToken, [u'('])
        if tokens.peek(SymbolToken, [u'/']):
            branch = SegmentParser << tokens
        else:
            branch = TopParser << tokens
        tail_token = tokens.pop(SymbolToken, [u')'], do_force=False)
        if tail_token is None:
            mark = Mark.union(head_token, branch)
            raise ParseError("cannot find a matching ')'", mark)
        mark = Mark.union(head_token, tail_token)
        group = GroupSyntax(branch, mark)
        return group


class SelectorParser(Parser):
    """
    Parses a `selector` production.
    """

    @classmethod
    def process(cls, tokens):
        # Expect:
        #   selector    ::= '{' arguments? '}'
        #   arguments   ::= argument ( ',' argument )* ','?
        #   argument    ::= segment | top
        head_token = tokens.pop(SymbolToken, [u'{'])
        branches = []
        while not tokens.peek(SymbolToken, [u'}']):
            if tokens.peek(SymbolToken, [u'/']):
                rbranch = SegmentParser << tokens
            else:
                rbranch = TopParser << tokens
            branches.append(rbranch)
            if tokens.peek(SymbolToken, [u',']):
                tokens.pop(SymbolToken, [u','])
            elif not tokens.peek(SymbolToken, [u'}']):
                mark = Mark.union(head_token, *branches)
                raise ParseError("cannot find a matching '}'", mark)
        tail_token = tokens.pop(SymbolToken, [u'}'])
        mark = Mark.union(head_token, tail_token)
        selector = SelectorSyntax(None, branches, mark)
        return selector


class IdentifierParser(Parser):
    """
    Parses an `identifier` production.
    """

    @classmethod
    def process(cls, tokens):
        # Expect:
        #   identifier      ::= NAME
        name_token = tokens.pop(NameToken)
        identifier = IdentifierSyntax(name_token.value, name_token.mark)
        return identifier


def parse(input, Parser=QueryParser):
    """
    Parses the input HTSQL query; returns the corresponding syntax node.

    In case of syntax errors, may raise :exc:`htsql.core.tr.error.ScanError`
    or :exc:`htsql.core.tr.error.ParseError`.

    `input` (a string)
        An HTSQL query or an HTSQL expression.

    `Parser` (a subclass of :class:`Parser`)
        The parser to use for parsing the input expression.  By default,
        `input` is treated as a complete HTSQL query.
    """
    parser = Parser(input)
    return parser.parse()


