#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ..util import (maybe, oneof, listof, omapof, trim_doc, toposort, omap,
        TextBuffer, Printable)
from ..error import Error, Mark, parse_guard, point
from .token import Token
import re


class Pattern(Printable):
    # Abstract regular expression pattern.

    def dfa(self):
        # Converts the pattern to a DFA, which is encoded as:
        #   [ { symbol: state, ...}, ... ]
        # Each list element is a state transition table; the table keys
        # are input symbols, the values are target states.  An entry
        # `None -> None` indicates an exit rule.

        # Build the initial NFA.
        nfa = { 0: [] }
        self.encode(nfa, 0, None)

        # Eliminate zero transitions.

        # Mapping from a state to the set of target states
        # reachable via zero transitions.
        closure = { None: set() }
        # Find immediate zero transitions.
        for src in nfa:
            closure[src] = set(dst for symbol, dst in nfa[src]
                                   if symbol is None)
        # Reorder the states, targets first.
        ordered_states = toposort(sorted(closure, key=(lambda n: n if n is not None else -1)),
                          (lambda s: sorted(closure[s], key=(lambda n: n if n is not None else -1))))
        ordered_states.remove(None)
        # Transitively close zero transitions.
        for src in ordered_states:
            for dst in sorted(closure[src], key=(lambda n: n if n is not None else -1)):
                closure[src] |= closure[dst]
        # Memorize if the initial state contains a zero exit rule.
        is_epsilon = (None in closure[0])
        # Iterate over states, from targets to sources.
        for src in ordered_states:
            # Updated transitions.
            moves = []
            for symbol, dst in nfa[src]:
                if symbol is None:
                    # Replace zero transitions with all transitions from
                    # the target (which is already processed).
                    if dst is not None:
                        moves.extend(nfa[dst])
                    # A zero exit transition is lost here.
                else:
                    # Keep a non-zero transition.
                    moves.append((symbol, dst))
                    # But also add an exit transition if the target had a zero
                    # exit transition.
                    if None in closure[dst]:
                        moves.append((symbol, None))
            nfa[src] = tuple(moves)

        # Remove duplicate states.

        # Iterate while there are states with identical transition tables.
        while len(set(nfa.values())) < len(nfa):
            # A mapping from a transition table to a state number.
            state_by_moves = {}
            # Find two states with the same transition table.
            for copy in sorted(nfa):
                moves = nfa[copy]
                if moves in state_by_moves:
                    state = state_by_moves[moves]
                    break
                else:
                    state_by_moves[moves] = copy
            # Remove the duplicate state and rewrite transition tables.
            del nfa[copy]
            for src in nfa:
                nfa[src] = tuple((symbol, (dst if dst != copy else state))
                                 for symbol, dst in nfa[src])

        # Convert NFA to DFA.

        # The DFA to be constructed.  Each DFA state corresponds to a set
        # of NFA states.  The initial DFA state is the initial NFA state +
        # the exit state if the initial NFA state contained a zero exit
        # transition.
        dfa = [None]
        if not is_epsilon:
            start = (0,)
        else:
            start = (None, 0)
        # For recognized states, a mapping from a set of NFA states to
        # a DFA state number.
        index_by_states = { start: 0 }
        # NFA states to process.
        queue = [start]
        while queue:
            # Set of NFA states.
            states = queue.pop(0)
            # The respective DFA state.
            origin = index_by_states[states]
            # Mapping from a symbol to a set of NFA states.
            moves = {}
            for src in states:
                # If NFA states include an exit state, add an exit rule.
                if src is None:
                    moves[None] = None
                else:
                    # Otherwise, populate the transition table.
                    for symbol, dst in nfa[src]:
                        moves.setdefault(symbol, set()).add(dst)
            # Iterate over the transition table.
            for symbol in sorted(moves, key=(lambda n: n if n is not None else ())):
                # Keep zero exit rules intact.
                if symbol is None:
                    continue
                # Replace a set of NFA states with a DFA state.
                states = tuple(sorted(moves[symbol], key=(lambda n: n if n is not None else -1)))
                if states in index_by_states:
                    target = index_by_states[states]
                else:
                    # Schedule a new DFA state for processing.
                    target = len(dfa)
                    index_by_states[states] = target
                    dfa.append(None)
                    queue.append(states)
                moves[symbol] = target
            dfa[origin] = moves

        return dfa

    def encode(self, nfa, src, dst):
        # Encode the pattern as a path from `src` state to `dst` state in
        # an NFA.
        raise NotImplementedError()


class AltPat(Pattern):
    # Alternation pattern.

    def __init__(self, arms):
        assert isinstance(arms, listof(Pattern)) and len(arms) >= 1
        self.arms = arms

    def __str__(self):
        return "(%s)" % " | ".join(str(arm) for arm in self.arms)

    def encode(self, nfa, src, dst):
        # Encode:
        #   src -> /arm1/ -> dst
        #   src -> /arm2/ -> dst
        #   ...
        for arm in self.arms:
            arm.encode(nfa, src, dst)


class SeqPat(Pattern):
    # Concatenation pattern.

    def __init__(self, arms):
        assert isinstance(arms, listof(Pattern)) and len(arms) >= 1
        self.arms = arms

    def __str__(self):
        return "(%s)" % " ".join(str(arm) for arm in self.arms)

    def encode(self, nfa, src, dst):
        # Encode:
        #   src -> /arm1/ -> * -> /arm2/ -> * -> ... -> * -> /armN/ -> dst
        isrc = None
        idst = src
        for arm in self.arms[:-1]:
            isrc = idst
            idst = len(nfa)
            nfa[idst] = []
            arm.encode(nfa, isrc, idst)
        isrc = idst
        idst = dst
        self.arms[-1].encode(nfa, isrc, idst)


class ModPat(Pattern):
    # Repetition pattern.

    def __init__(self, arm, modifier):
        assert isinstance(arm, Pattern)
        assert modifier in ['?', '*', '+']
        self.arm = arm
        self.modifier = modifier

    def __str__(self):
        return "%s%s" % (self.arm, self.modifier)

    def encode(self, nfa, src, dst):
        # Encode `?`:
        #   src -> dst
        #   src -> /arm/ -> dst
        # Encode `*`:
        #   src -> dst
        #   src -> * -> /arm/ -> * -> dst
        #          ^____________/
        # Encode `+`:
        #   src -> * -> /arm/ -> * -> dst
        #          ^____________/
        if self.modifier in ['?', '*']:
            nfa[src].append((None, dst))
        if self.modifier == '?':
            self.arm.encode(nfa, src, dst)
        else:
            isrc = len(nfa)
            nfa[isrc] = []
            idst = len(nfa)
            nfa[idst] = []
            nfa[src].append((None, isrc))
            nfa[idst].append((None, dst))
            nfa[idst].append((None, isrc))
            self.arm.encode(nfa, isrc, idst)


class SymPat(Pattern):
    # Symbol pattern.

    def __init__(self, symbol):
        assert symbol is not None
        self.symbol = symbol

    def __str__(self):
        return str(self.symbol)

    def encode(self, nfa, src, dst):
        # Encode:
        #   src -> /symbol/ -> dst
        nfa[src].append((self.symbol, dst))


class EpsPat(Pattern):
    # Zero pattern.

    def __str__(self):
        return "()"

    def encode(self, nfa, src, dst):
        # Encode:
        #   src -> dst
        nfa[src].append((None, dst))


class PatternBuffer(TextBuffer):
    # `TextBuffer` extension which can parse regular expression
    # metacharacters and return a `Pattern` instance.

    def pull_alt(self, symbol_pattern, to_symbol=None):
        # Reads an alternation pattern.
        arms = [self.pull_seq(symbol_pattern, to_symbol)]
        while self.pull(r"[|]") is not None:
            arms.append(self.pull_seq(symbol_pattern, to_symbol))
        return AltPat(arms) if len(arms) > 1 else arms[0]

    def pull_seq(self, symbol_pattern, to_symbol=None):
        # Reads a concatenation pattern.
        arms = [self.pull_mod(symbol_pattern, to_symbol)]
        while self.peek(symbol_pattern) or self.peek(r"[(]"):
            arms.append(self.pull_mod(symbol_pattern, to_symbol))
        return SeqPat(arms) if len(arms) > 1 else arms[0]

    def pull_mod(self, symbol_pattern, to_symbol=None):
        # Reads a repetition pattern.
        arm = self.pull_atom(symbol_pattern, to_symbol)
        mod = self.pull(r"[?] | [*] | [+]")
        return ModPat(arm, mod) if mod is not None else arm

    def pull_atom(self, symbol_pattern, to_symbol=None):
        # Reads a pattern group or a symbol.
        # `symbol_pattern`
        #   A (Python) regular expression pattern that matches
        #   a symbol for `SymPat`.
        # `to_symbol`
        #   An optional conversion function for matched symbols.
        if self.pull(r"[(] \s* [)]") is not None:
            return EpsPat()
        if self.pull(r"[(]") is not None:
            pattern = self.pull_alt(symbol_pattern, to_symbol)
            if self.pull(r"[)]") is None:
                raise self.fail("expected ')'")
            return pattern
        symbol = self.pull(symbol_pattern)
        if symbol is None:
            raise self.fail("expected a symbol")
        if to_symbol is not None:
            symbol = to_symbol(symbol)
        return SymPat(symbol)


class LexicalGrammar(Printable):
    """
    Defines a lexical grammar and generates a tokenizer.
    """

    def __init__(self):
        self.rules = omap()
        self.signals = omap()

    def add_rule(self, name):
        """
        Adds and returns a new tokenizer rule.
        """
        assert isinstance(name, str)
        assert name not in self.rules, name
        rule = LexicalRule(name)
        self.rules[rule.name] = rule
        return rule

    def add_signal(self, descriptor):
        """
        Adds and returns a signal rule.

        A signal rule injects a signal token in front of a matched sequence of
        tokens.

        `descriptor`: ``str``
            A string of the form::

                <name>: <pattern>

            where ``<name>`` is the name of a defined signal token,
            ``<pattern>`` is a regular expression for matching a sequence of
            tokens.
        """
        assert isinstance(descriptor, str)

        buffer = PatternBuffer(descriptor)
        name = buffer.pull(r"\w+")
        if name is None:
            raise buffer.fail("expected a token name")
        assert name not in self.signals, name
        if buffer.pull(r"[:]") is None:
            raise buffer.fail("expected ':'")
        pattern = buffer.pull_alt(r"[%] \w+ | [`] (?: [^`] | [`][`] )* [`]",
                                  lambda s: s[1:] if s[0] == '%'
                                            else s[1:-1].replace("``", "`"))
        if buffer:
            raise buffer.fail("expected rule end")
        doc = trim_doc(descriptor)

        signal = LexicalSignal(name, pattern, doc)
        self.signals[signal.name] = signal
        return signal

    def __str__(self):
        # Dump textual representation of the grammar.
        chunks = []
        for rule in self.rules:
            chunks.append(str(rule))
        for signal in self.signals:
            chunks.append(str(signal))
        return "\n\n".join(chunks)

    def __call__(self):
        # Generates a scanner.  At least one tokenizer rule must exist.
        assert self.rules

        # Make a scan table from each tokenizer rule.
        tables = omap()
        for rule in self.rules:
            assert rule.tokens
            patterns = []
            groups = []
            for token in rule.tokens:
                assert token.push is None or token.push in self.rules
                pattern = str(token.pattern)
                pattern = "(?P<%s> %s )" % (token.name, pattern)
                patterns.append(pattern)
                group = ScanTableGroup(token.name, token.error, token.is_junk,
                        token.is_symbol, token.unquote, token.pop, token.push)
                groups.append(group)
            pattern = " | ".join(patterns)
            regexp = re.compile(pattern, re.U|re.X)
            table = ScanTable(rule.name, regexp, groups)
            tables[table.name] = table

        # Define a treatment for each signal token definition.
        treatments = []
        for signal in self.signals:
            treatment = ScanTreatment(signal.name, signal.pattern.dfa())
            treatments.append(treatment)

        # Textual grammar representation.
        doc = str(self)

        return Scanner(tables, treatments, doc)


class LexicalRule(Printable):
    """
    Tokenizer context for a lexical grammar.
    """

    def __init__(self, name):
        assert isinstance(name, str)
        self.name = name
        self.tokens = omap()

    def add_token(self, descriptor, error=None, is_junk=False, is_symbol=False,
                  unquote=None, pop=None, push=None):
        """
        Adds a token pattern.

        `descriptor`: ``str``
            A string of the form::

                <name>: <pattern>

            where ``<name>`` is the token code, ``<pattern>`` is a regular
            expression for matching input characters.

        `error`: ``str`` or ``None``
            If set, an error is raised when the token is recognized.

        `is_junk`: ``bool``
            If set, the token is not emitted.

        `is_symbol`: ``bool``
            If set, ``<name>`` is ignored and the token code is equal to
            the token value.

        `unquote`: ``unicode`` -> ``unicode`` or ``None``
            If set, apply to the token value.

        `pop`: ``int`` or ``None``
            If set, exit from the ``pop`` top tokenizer contexts.

        `push`: ``unicode`` or ``None``
            If set, enter a new tokenizer context.
        """
        assert isinstance(descriptor, str)
        assert isinstance(error, maybe(str))
        assert isinstance(is_junk, bool)
        assert isinstance(is_symbol, bool)
        assert isinstance(pop, maybe(int))
        assert isinstance(push, maybe(str))

        buffer = PatternBuffer(descriptor)
        name = buffer.pull(r"\w+")
        if name is None:
            raise buffer.fail("expected the token name")
        assert name not in self.tokens, name
        if buffer.pull(r"[:]") is None:
            raise buffer.fail("expected ':'")
        pattern = buffer.pull_alt(r"[\[] (?: [^\\\]] | \\. )+ [\]] |"
                                  r" [\^] | [$]")
        if buffer:
            raise buffer.fail("expected rule end")
        doc = trim_doc(descriptor)

        token = LexicalToken(name, pattern, error, is_junk, is_symbol,
                             unquote, pop, push, doc)
        self.tokens[token.name] = token
        return token

    def __str__(self):
        chunks = []
        chunks.append("[%s]" % self.name)
        for token in self.tokens:
            chunks.append(str(token))
        return "\n".join(chunks)


class LexicalToken(Printable):
    # A token matching rule.

    def __init__(self, name, pattern, error, is_junk, is_symbol,
                 unquote, pop, push, doc):
        assert isinstance(name, str)
        assert isinstance(pattern, Pattern)
        assert isinstance(error, maybe(str))
        assert isinstance(is_junk, bool)
        assert isinstance(is_symbol, bool)
        assert isinstance(pop, maybe(int))
        assert isinstance(push, maybe(str))
        assert isinstance(doc, str)
        self.name = name
        self.pattern = pattern
        self.error = error
        self.is_junk = is_junk
        self.is_symbol = is_symbol
        self.unquote = unquote
        self.pop = pop
        self.push = push
        self.doc = doc

    def __str__(self):
        chunks = [self.doc]
        if self.pop or self.push:
            chunks.append(" {")
            if self.pop:
                chunks.append("pop: %s" % self.pop)
            if self.pop and self.push:
                chunks.append("; ")
            if self.push:
                chunks.append("push: %s" % self.push)
            chunks.append("}")
        return "".join(chunks)


class LexicalSignal(Printable):
    # A matching rule for a signal token.

    def __init__(self, name, pattern, doc):
        assert isinstance(name, str)
        assert isinstance(pattern, Pattern)
        assert isinstance(doc, str)
        self.name = name
        self.pattern = pattern
        self.doc = doc

    def __str__(self):
        return self.doc


class ScanTable:
    # Tokenizer context.

    def __init__(self, name, regexp, groups):
        assert isinstance(name, str)
        assert isinstance(regexp, re._pattern_type)
        assert isinstance(groups, listof(ScanTableGroup))
        self.name = name
        self.regexp = regexp
        self.groups = groups


class ScanTableGroup:
    # Matching rule for tokenizer context.

    def __init__(self, name, error, is_junk, is_symbol, unquote, pop, push):
        assert isinstance(name, str)
        assert isinstance(error, maybe(str))
        assert isinstance(is_junk, bool)
        assert isinstance(is_symbol, bool)
        assert isinstance(pop, maybe(int))
        assert isinstance(push, maybe(str))
        self.name = name
        self.error = error
        self.is_junk = is_junk
        self.is_symbol = is_symbol
        self.unquote = unquote
        self.pop = pop
        self.push = push


class ScanTreatment:
    # Post-process treatment rule.

    def __init__(self, name, dfa):
        assert isinstance(name, str)
        self.name = name
        self.dfa = dfa


class Scanner(Printable):
    # Converts a sequence of characters to a sequence of tokens.

    def __init__(self, tables, treatments, doc):
        assert isinstance(tables, omapof(ScanTable))
        assert len(tables) > 0
        assert isinstance(treatments, listof(ScanTreatment))
        assert isinstance(doc, str)

        self.start = next(iter(tables)).name
        self.tables = tables
        self.treatments = treatments
        self.doc = doc

    def __call__(self, text, start=None):
        assert isinstance(text, str)
        assert start is None or start in self.tables
        # The name of the initial tokenizer context.
        if start is None:
            start = self.start
        # The current position of the stream head.
        position = 0
        # Stack of active tokenizer contexts.
        stack = [self.tables[start]]
        # Accumulated tokens.
        tokens = []
        # Keep running until we exit from the last tokenizer contexts.
        while stack:
            # The current context.
            table = stack[-1]
            # Find the next token.
            match = table.regexp.match(text, position)
            # Complain if no token is found.
            if match is None:
                mark = Mark(text, position, position)
                with parse_guard(mark):
                    if position < len(text):
                        raise Error("Got unexpected character %r"
                                    % text[position])
                    else:
                        raise Error("Got unexpected end of input")
            # The position of the next token.
            next_position = match.end()
            # The error context for the new token.
            mark = Mark(text, position, next_position)
            # Find which pattern group matched the token.
            for group in table.groups:
                block = match.group(group.name)
                if block is not None:
                    break
            else:
                # Not reachable.
                assert False
            # Report an error for an error rule.
            if group.error is not None:
                with parse_guard(mark):
                    raise Error(group.error)
            # Generate a new token object.
            if not group.is_junk:
                if group.unquote:
                    block = group.unquote(block)
                code = group.name
                if group.is_symbol:
                    code = block
                token = Token(code, block)
                point(token, mark)
                tokens.append(token)
            # For an exit rule, exit the top context.
            if group.pop:
                stack = stack[:-group.pop]
            # For an enter rule, add a new context.
            if group.push:
                stack.append(self.tables[group.push])
            # Advance the stream head position.
            position = next_position

        # Inject a signal token when a treatment DFA
        # matches a sequence of tokens.
        for treatment in self.treatments:
            dfa = treatment.dfa
            start = 0
            while start < len(tokens):
                end = start
                token = tokens[end]
                state = 0
                while token.code in dfa[state]:
                    state = dfa[state][token.code]
                    end += 1
                    if end >= len(tokens):
                        break
                    token = tokens[end]
                if None in dfa[state]:
                    token = Token(treatment.name, "")
                    point(token, tokens[start])
                    tokens.insert(start, token)
                    start = end+1
                else:
                    start += 1

        return tokens

    def __str__(self):
        return self.doc


class SyntaxGrammar(Printable):
    """
    Defines a syntax grammar and generates a parser.
    """

    def __init__(self):
        self.rules = omap()

    def add_rule(self, descriptor, match=None, fail=None):
        """
        Adds and returns a production rule.

        `descriptor`: ``str``
            A string of the form::

                <name>: <pattern>

            where ``<name>`` is the production name, ``<pattern>`` is a regular
            expression for matching tokens and other productions.

        `match`: :class:`ParseStream` -> [:class:`.Syntax`]
            A function called when the rule matches a sequence of tokens.
            The function returns syntax nodes to replace the matched tokens.

        `fail`: (:class:`ParseStream`, :class:`.Token`) -> exception
            A function called when the rule is unable to finish matching a
            sequence of tokens.  The function takes a stream of nodes and a
            blocking token and returns an exception object.
        """
        assert isinstance(descriptor, str)

        buffer = PatternBuffer(descriptor)
        name = buffer.pull(r"\w+")
        if name is None:
            raise buffer.fail("expected the rule name")
        assert name not in self.rules
        if buffer.pull(r"[:]") is None:
            raise buffer.fail("expected ':'")
        pattern = buffer.pull_alt(r"[%]? \w+ |"
                                  r" [`] (?: [^`] | [`][`] )* [`]",
                                  lambda s:
                                    (s[1:], True)
                                        if s[0] == "%" else
                                    (s[1:-1].replace("``", "`"), True)
                                        if s[0] == s[-1] == "`" else
                                    (s, False))
        if buffer:
            raise buffer.fail("expected rule end")
        dfa = pattern.dfa()
        doc = trim_doc(descriptor)

        rule = SyntaxRule(name, dfa, match, fail, doc)
        self.rules[rule.name] = rule
        return rule

    def __str__(self):
        return "\n".join(str(rule) for rule in self.rules)

    def __call__(self):
        # Generates a parser.
        assert self.rules

        # Check that there are no epsilon rules or undefined non-terminals.
        for rule in self.rules:
            assert None not in rule.dfa[0], rule.name
            for moves in rule.dfa:
                for key in sorted(moves, key=(lambda n: n if n is not None else ())):
                    if key is None:
                        continue
                    symbol, is_terminal = key
                    if not is_terminal:
                        assert symbol in self.rules, symbol

        # Reorder the rules, so that any rule is processed after all
        # non-terminals which can start of the rule.
        order = {}
        for rule in self.rules:
            order[rule] = set()
            for symbol, is_terminal in rule.dfa[0]:
                if is_terminal:
                    continue
                order[rule].add(self.rules[symbol])
        ordered_rules = toposort(list(self.rules), lambda r: order[r])

        # For each rule, find the set of terminal symbols which can start
        # the rule.
        first = {}
        for rule in ordered_rules:
            first[rule.name] = set()
            for symbol, is_terminal in rule.dfa[0]:
                if is_terminal:
                    first[rule.name].add(symbol)
                else:
                    first[rule.name] |= first[symbol]

        # For each rule, generate a state machine.
        tables = omap()
        for rule in self.rules:
            # A sequence of state tables.
            machine = []
            for moves in rule.dfa:
                # Classify all input symbols.
                has_exit = False
                terminals = set()
                nonterminals = set()
                for key in moves:
                    if key is None:
                        has_exit = True
                    else:
                        symbol, is_terminal = key
                        if is_terminal:
                            terminals.add(symbol)
                        else:
                            nonterminals.add(symbol)
                has_one_nonterminal = (len(nonterminals) == 1)

                # Build a new state table as a mapping from
                # token code to a pair `(nonterminal, state)`.
                transitions = {}
                for key in sorted(moves, key=(lambda n: n if n is not None else ())):
                    # Exit and terminal input rules are copied trivially.
                    if key is None:
                        transitions[None] = (None, None)
                    else:
                        target = moves[key]
                        symbol, is_terminal = key
                        if is_terminal:
                            transitions[symbol] = (None, target)
                        # A non-terminal rule is replaced with a "catch-all"
                        # rule if it is the only non-terminal rule in
                        # the current state table and there is no exit rule.
                        # We do this to sift most error conditions down
                        # allowing us to handle them at one place.
                        elif has_one_nonterminal and not has_exit:
                            transitions[None] = (symbol, target)
                        else:
                            # Otherwise, replace a non-terminal rule
                            # with terminals from the FIRST set.
                            for token_code in sorted(first[symbol]):
                                # If there is a conflict between a terminal
                                # and a non-terminal rule, prefer the terminal
                                # rule.
                                if token_code in terminals:
                                    continue
                                # Report a conflict between two non-terminals.
                                assert token_code not in transitions, \
                                        "ambiguous transition by %r in %s" \
                                            % (token_code, symbol)
                                transitions[token_code] = (symbol, target)
                machine.append(transitions)

            table = ParseTable(rule.name, machine, rule.match, rule.fail)
            tables[table.name] = table

        return Parser(tables, str(self))


class SyntaxRule(Printable):
    # A production rule.

    def __init__(self, name, dfa, match, fail, doc):
        assert isinstance(name, str)
        assert isinstance(doc, str)
        self.name = name
        self.dfa = dfa
        self.match = match
        self.fail = fail
        self.doc = doc

    def set_match(self, match):
        assert self.match is None
        self.match = match

    def set_fail(self, fail):
        assert self.fail is None
        self.fail = fail

    def __str__(self):
        return self.doc


class ParseTable:
    # A production rule compiled into a state machine.

    def __init__(self, name, machine, match, fail):
        assert isinstance(name, str)
        self.name = name
        self.machine = machine
        self.match = match
        self.fail = fail


class ParseStream:
    """
    A buffer of :class:`.Token` and :class:`.Syntax` nodes.
    """

    def __init__(self, nodes):
        self.nodes = nodes
        self.index = 0

    def reset(self):
        """
        Rewinds to the beginning of the buffer.
        """
        self.index = 0

    def pull(self):
        """
        Pulls the next node from the buffer.
        """
        assert self.index < len(self.nodes)
        node = self.nodes[self.index]
        self.index += 1
        return node

    def peek(self, code):
        """
        Checks if the next node is a :class:`.Token` object
        with the given `code`.
        """
        if not (self.index < len(self.nodes)):
            return False
        node = self.nodes[self.index]
        if not isinstance(node, Token):
            return False
        return (node.code == code)

    def mark(self, node):
        # Makes a mark covering all the pulled nodes.
        mark = Mark.union(*self.nodes[:self.index])
        point(node, mark)
        return node

    def __bool__(self):
        return (self.index < len(self.nodes))

    def __len__(self):
        return (len(self.nodes) - self.index)


class Parser(Printable):
    # Converts a sequence of tokens to a syntax node.

    def __init__(self, tables, doc):
        assert isinstance(tables, omapof(ParseTable)) and len(tables) > 0
        assert isinstance(doc, str)
        self.start = next(iter(tables)).name
        self.tables = tables
        self.doc = doc

    def __str__(self):
        return self.doc

    def __call__(self, tokens, start=None):
        assert start is None or start in self.tables
        if start is None:
            start = self.start

        # The current production rule.
        table = self.tables[start]
        # The current machine state.
        state = 0
        # Nodes accumulated by the current rule.
        nodes = []
        # Stack of rules being processed.
        stack = []
        # The current token position.
        index = 0

        # Iterate until we finish the top rule.
        while True:
            # Current transition table.
            transitions = table.machine[state]
            assert index < len(tokens)
            token = tokens[index]
            # Check if the current token code is in the transition table.
            if token.code in transitions:
                transition = transitions[token.code]
            # Otherwise, check if the table contains a catch-all transition.
            elif None in transitions:
                transition = transitions[None]
            # Otherwise, report an error.
            else:
                if table.fail is not None:
                    stream = ParseStream(nodes)
                    error = table.fail(stream, token)
                    if error is not None:
                        raise error
                with parse_guard(token):
                    if token:
                        raise Error("Got unexpected input")
                    else:
                        raise Error("Got unexpected end of input")

            # The non-terminal associated with the transition and the next
            # machine state.
            next_symbol, next_state = transition
            # If it is not an exit transition.
            if next_state is not None:
                # If no non-terminal associated with the transition,
                # just pull and store the token node.
                if next_symbol is None:
                    state = next_state
                    nodes.append(token)
                    index += 1
                # Otherwise, keep the current state in the stack and
                # jump to the non-terminal rule.
                else:
                    state = next_state
                    stack.append((table, state, nodes))
                    table = self.tables[next_symbol]
                    state = 0
                    nodes = []

            # Exit transition.
            else:
                assert next_symbol is None
                stream = ParseStream(nodes)
                # Complain if we are exiting from the top rule
                # and there are still tokens left.
                if not stack and token:
                    with parse_guard(token):
                        if table.fail is not None:
                            error = table.fail(stream, token)
                            if error is not None:
                                raise error
                        raise Error("Got unexpected input")
                # Process accumulated nodes.
                if table.match is not None:
                    production = list(table.match(stream))
                    assert not stream, (table.name, nodes)
                else:
                    production = nodes[:]
                # Pull the previous rule from the stack and update
                # its list of accumulated nodes.
                if stack:
                    table, state, nodes = stack.pop()
                    nodes.extend(production)
                else:
                    # If we are at the top rule, we are done.
                    index += 1
                    break

        # Return the generated syntax node.
        assert index == len(tokens)
        node = None
        if production:
            node = production[0]
        return node


