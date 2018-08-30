#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ..cache import once
from .token import (DIRSIG, PIPESIG, LHSSIG, STRING, LABEL, INTEGER, DECIMAL,
        FLOAT)
from .syntax import (Syntax, SkipSyntax, AssignSyntax, SpecifySyntax,
        FunctionSyntax, PipeSyntax, OperatorSyntax, PrefixSyntax, FilterSyntax,
        ProjectSyntax, LinkSyntax, AttachSyntax, DetachSyntax, CollectSyntax,
        DirectSyntax, ComposeSyntax, UnpackSyntax, LiftSyntax, GroupSyntax,
        SelectSyntax, LocateSyntax, RecordSyntax, ListSyntax, IdentitySyntax,
        ReferenceSyntax, IdentifierSyntax, StringSyntax, LabelSyntax,
        IntegerSyntax, DecimalSyntax, FloatSyntax)
from .grammar import SyntaxGrammar
from .scan import scan


@once
def prepare_parse():
    """
    Returns a syntax parser for HTSQL grammar.
    """

    # Start a new grammar.
    grammar = SyntaxGrammar()

    # The top-level production.
    query = grammar.add_rule('''
        query:  assignment `;`?
    ''')

    @query.set_match
    def match_query(stream):
        syntax = stream.pull()  # assignment
        if stream:
            stream.pull()       # `;`
        yield syntax

    # Assignment expression; `flow_assignment` does not consume
    # `:` and `/:` pipes while `assignment` does.
    assignment = grammar.add_rule('''
        assignment:
                ( %LHSSIG specifier `:=` )? pipe
    ''')

    flow_assignment = grammar.add_rule('''
        flow_assignment:
                ( %LHSSIG specifier `:=` )? flow
    ''')

    def match_assignment(stream):
        if stream.peek(LHSSIG):
            stream.pull()           # %LHSSIG
            larm = stream.pull()    # specifier
            stream.pull()           # `:=`
            rarm = stream.pull()    # pipe | flow
            yield stream.mark(AssignSyntax(larm, rarm))
        else:
            yield stream.pull()     # pipe | flow

    assignment.set_match(match_assignment)
    flow_assignment.set_match(match_assignment)

    # The left-hand side of an assignment expression.
    specifier = grammar.add_rule('''
        specifier:
                parameter ( `.` parameter )* ( `(` parameter_list? `)` )?
    ''')

    @specifier.set_match
    def match_specifier(stream):
        larms = []
        larm = stream.pull()        # parameter
        larms.append(larm)
        while stream.peek('.'):
            stream.pull()           # `.`
            larm = stream.pull()    # parameter
            larms.append(larm)
        rarms = None
        if stream.peek('('):
            rarms = []
            stream.pull()               # `(`
            while not stream.peek(')'):
                rarm = stream.pull()    # parameter
                rarms.append(rarm)
            stream.pull()               # `)`
        yield stream.mark(SpecifySyntax(larms, rarms))

    # Assignees and parameters in an assignment expression.
    parameter = grammar.add_rule('''
        parameter:
                identifier | reference
    ''')

    # Lists of parameters/function or record arguments.
    parameter_list = grammar.add_rule('''
        parameter_list:
                parameter ( `,` parameter )* `,`?
    ''')

    assignment_list = grammar.add_rule('''
        assignment_list:
                assignment ( `,` assignment )* `,`?
    ''')

    def match_list(stream):
        yield stream.pull()         # parameter | assignment
        while stream:
            stream.pull()           # `,`
            if stream:
                yield stream.pull() # parameter | assignment

    parameter_list.set_match(match_list)
    assignment_list.set_match(match_list)

    # Segment pipe notation (`<larm> /: <identifier>`).
    pipe = grammar.add_rule('''
        pipe:   flow_pipe
                (
                  %PIPESIG? `/` `:`
                  identifier ( flow_assignment | `(` assignment_list? `)` )?
                )*
    ''')

    @pipe.set_match
    def match_pipe(stream):
        syntax = stream.pull()          # flow_pipe
        while stream:
            if stream.peek(PIPESIG):
                stream.pull()           # %PIPESIG
            larm = syntax
            stream.pull()               # `/`
            stream.pull()               # `:`
            identifier = stream.pull()  # identifier
            rarms = []
            is_open = True
            if stream and not (stream.peek('/') or stream.peek(PIPESIG)):
                if stream.peek('('):
                    is_open = False
                    stream.pull()                   # `(`
                    while not stream.peek(')'):
                        arm = stream.pull()         # assignment
                        rarms.append(arm)
                    stream.pull()                   # `)`
                else:
                    rarm = stream.pull()            # flow_assignment
                    rarms.append(rarm)
            is_flow = False
            syntax = PipeSyntax(identifier, larm, rarms,
                                is_flow, is_open)
            stream.mark(syntax)
        yield syntax

    # Flow pipe notation (`<larm> : <identifier>`) and sort direction
    # indicators.
    flow_pipe = grammar.add_rule('''
        flow_pipe:
                flow
                (
                  %DIRSIG? ( `+` | `-` ) |
                  `:` identifier ( flow_assignment | `(` assignment_list? `)` )?
                )*
    ''')

    @flow_pipe.set_match
    def match_flow_pipe(stream):
        syntax = stream.pull()              # flow
        while stream:
            if stream.peek(DIRSIG):
                stream.pull()               # %DIRSIG
            if stream.peek('+') or stream.peek('-'):
                direction = stream.pull()   # `+` | `-`
                syntax = DirectSyntax(direction.text, syntax)
                stream.mark(syntax)
            else:
                larm = syntax
                stream.pull()               # `:`
                identifier = stream.pull()  # identifier
                rarms = []
                is_open = True
                if stream and not stream.peek(':'):
                    if stream.peek('('):
                        is_open = False
                        stream.pull()                   # `(`
                        while not stream.peek(')'):
                            rarm = stream.pull()        # assignment
                            rarms.append(rarm)
                        stream.pull()                   # `)`
                    elif not (stream.peek('+') or stream.peek('-')):
                        rarm = stream.pull()            # flow_assignment
                        rarms.append(rarm)
                is_flow = True
                syntax = PipeSyntax(identifier, larm, rarms,
                                    is_flow, is_open)
                stream.mark(syntax)
        yield syntax

    # Flow operators (`?` and `^`) and selection (`<larm> {...}`).
    flow = grammar.add_rule('''
        flow:   disjunction
                (
                  ( `?` | `^` ) disjunction |
                  record ( `.` location )*
                )*
    ''')

    @flow.set_match
    def match_flow(stream):
        syntax = stream.pull()              # disjunction
        while stream:
            if stream.peek('?'):
                larm = syntax
                stream.pull()               # `?`
                rarm = stream.pull()        # disjunction
                syntax = FilterSyntax(larm, rarm)
                stream.mark(syntax)
            elif stream.peek('^'):
                larm = syntax
                stream.pull()               # `^`
                rarm = stream.pull()        # disjunction
                syntax = ProjectSyntax(larm, rarm)
                stream.mark(syntax)
            else:
                larm = syntax
                rarm = stream.pull()        # record
                syntax = SelectSyntax(larm, rarm)
                stream.mark(syntax)
                while stream.peek('.'):
                    stream.pull()           # `.`
                    larm = syntax
                    rarm = stream.pull()    # location
                    syntax = ComposeSyntax(larm, rarm)
                    stream.mark(syntax)
        yield syntax

    # Used by binary operator productions of the form:
    #   <larm> ( <symbol> <rarm> )*
    def match_operator(stream):
        syntax = stream.pull()          # (left operand)
        while stream:
            larm = syntax
            operator = stream.pull()    # (operator symbol)
            rarm = stream.pull()        # (right operand)
            syntax = OperatorSyntax(operator.text, larm, rarm)
            stream.mark(syntax)
        yield syntax

    # OR operator.
    disjunction = grammar.add_rule('''
        disjunction:
                conjunction ( `|` conjunction )*
    ''', match=match_operator)

    # AND operator.
    conjunction = grammar.add_rule('''
        conjunction:
                negation ( `&` negation )*
    ''', match=match_operator)

    # NOT operator.
    negation = grammar.add_rule('''
        negation:
                `!` negation | comparison
    ''')

    @negation.set_match
    def match_negation(stream):
        if stream.peek('!'):
            operator = stream.pull()    # `!`
            arm = stream.pull()         # negation
            syntax = PrefixSyntax(operator.text, arm)
            stream.mark(syntax)
        else:
            syntax = stream.pull()      # comparison
        yield syntax

    # Comparison operators.
    comparison = grammar.add_rule('''
        comparison:
                expression
                (
                  (
                    `~`     | `!~`  |
                    `<=`    | `<`   | `>=`  | `>`   |
                    `==`    | `=`   | `!==` | `!=`
                  )
                  expression
                )?
    ''', match=match_operator)

    # Addition/subtraction.
    expression = grammar.add_rule('''
        expression:
                term ( ( `+` | `-` ) term )*
    ''', match=match_operator)

    # Multiplication/division.
    term = grammar.add_rule('''
        term:   factor ( ( `*` | `/` ) factor )*
    ''', match=match_operator)

    # Unary `+` and `-`.
    factor = grammar.add_rule('''
        factor: ( `+` | `-` ) factor | linking
    ''')

    @factor.set_match
    def match_factor(stream):
        if stream.peek('+') or stream.peek('-'):
            prefix = stream.pull()  # `+` | `-`
            arm = stream.pull()     # factor
            syntax = PrefixSyntax(prefix.text, arm)
            stream.mark(syntax)
        else:
            syntax = stream.pull()  # linking
        yield syntax

    # Deprecated link operator (`->`).
    linking = grammar.add_rule('''
        linking:
                composition ( `->` flow )?
    ''')

    @linking.set_match
    def match_linking(stream):
        syntax = stream.pull()      # composition
        if stream.peek('->'):
            larm = syntax
            stream.pull()           # `->`
            rarm = stream.pull()    # flow
            syntax = LinkSyntax(larm, rarm)
            stream.mark(syntax)
        yield syntax

    # Composition expression.
    composition = grammar.add_rule('''
        composition:
                location ( `.` location )*
    ''')

    @composition.set_match
    def match_composition(stream):
        syntax = stream.pull()      # location
        while stream:
            larm = syntax
            stream.pull()           # `.`
            rarm = stream.pull()    # location
            syntax = ComposeSyntax(larm, rarm)
            stream.mark(syntax)
        yield syntax

    # Location expression (`<larm> [...]`)
    location = grammar.add_rule('''
        location:
                attachment identity?
    ''')

    @location.set_match
    def match_location(stream):
        syntax = stream.pull()      # attachment
        if stream:
            larm = syntax
            rarm = stream.pull()    # identity
            syntax = LocateSyntax(larm, rarm)
            stream.mark(syntax)
        yield syntax

    # Attachment operator.
    attachment = grammar.add_rule('''
        attachment:
                atom ( `@` atom )?
    ''')

    @attachment.set_match
    def match_attachment(stream):
        syntax = stream.pull()      # atom
        if stream:
            larm = syntax
            stream.pull()           # `@`
            rarm = stream.pull()    # atom
            syntax = AttachSyntax(larm, rarm)
            stream.mark(syntax)
        yield syntax

    # Atomic expressions.
    atom = grammar.add_rule('''
        atom:   collection  | detachment    | unpacking     |
                reference   | function      | lift          |
                record      | list          | identity      | literal
    ''')

    # Skip indicator or collection operator.
    collection = grammar.add_rule('''
        collection:
                `/` flow_pipe?
    ''')

    @collection.set_match
    def match_collection(stream):
        stream.pull()               # `/`
        if not stream:
            syntax = SkipSyntax()
            stream.mark(syntax)
        else:
            arm = stream.pull()     # flow_pipe
            syntax = CollectSyntax(arm)
            stream.mark(syntax)
        yield syntax

    # Detachment operator.
    detachment = grammar.add_rule('''
        detachment:
                `@` atom
    ''')

    @detachment.set_match
    def match_detachment(stream):
        stream.pull()               # `@`
        arm = stream.pull()         # atom
        syntax = DetachSyntax(arm)
        stream.mark(syntax)
        yield syntax

    # Unpacking expression.
    unpacking = grammar.add_rule('''
        unpacking:
                `*` ( %INTEGER | `(` %INTEGER `)` )?
    ''')

    @unpacking.set_match
    def match_unpacking(stream):
        stream.pull()                   # `*`
        index = None
        is_open = True
        if stream:
            if stream.peek('('):
                is_open = False
                stream.pull()           # `(`
                index = stream.pull()   # %INTEGER
                stream.pull()           # `)`
            else:
                index = stream.pull()   # %INTEGER
            index = int(index.text)
        syntax = UnpackSyntax(index, is_open)
        stream.mark(syntax)
        yield syntax

    # Reference expression.
    reference = grammar.add_rule('''
        reference:
                `$` identifier
    ''')

    @reference.set_match
    def match_reference(stream):
        stream.pull()                   # `$`
        identifier = stream.pull()      # identifier
        syntax = ReferenceSyntax(identifier)
        stream.mark(syntax)
        yield syntax

    # Attribute or function call.
    function = grammar.add_rule('''
        function:
                identifier ( `(` assignment_list? `)` )?
    ''')

    @function.set_match
    def match_function(stream):
        syntax = stream.pull()          # identifier
        if stream:
            identifier = syntax
            stream.pull()               # `(`
            arms = []
            while not stream.peek(')'):
                arm = stream.pull()     # assignment
                arms.append(arm)
            stream.pull()               # `)`
            syntax = FunctionSyntax(identifier, arms)
            stream.mark(syntax)
        yield syntax

    # An identifier.
    identifier = grammar.add_rule('''
        identifier:
                %NAME
    ''')

    @identifier.set_match
    def match_identifier(stream):
        name = stream.pull()        # %NAME
        syntax = IdentifierSyntax(name.text)
        stream.mark(syntax)
        yield syntax

    # Lift indicator (`^`).
    lift = grammar.add_rule('''
        lift:   `^`
    ''')

    @lift.set_match
    def match_lift(stream):
        stream.pull()               # `^`
        syntax = LiftSyntax()
        stream.mark(syntax)
        yield syntax

    # Record constructor.
    record = grammar.add_rule('''
        record: `{` assignment_list? `}`
    ''')

    @record.set_match
    def match_record(stream):
        stream.pull()               # `{`
        arms = []
        while not stream.peek('}'):
            arm = stream.pull()     # assignment
            arms.append(arm)
        stream.pull()               # `}`
        syntax = RecordSyntax(arms)
        stream.mark(syntax)
        yield syntax

    # Grouping or list constructor.
    list_ = grammar.add_rule('''
        list:   `(` ( assignment ( `,` assignment_list? )? )? `)`
    ''')

    @list_.set_match
    def match_list(stream):
        stream.pull()                       # `(`
        if stream.peek(')'):
            stream.pull()                   # `)`
            syntax = ListSyntax([])
            stream.mark(syntax)
        else:
            arm = stream.pull()             # assignment
            if stream.peek(')'):
                stream.pull()               # `)`
                syntax = GroupSyntax(arm)
            else:
                arms = [arm]
                stream.pull()               # `,`
                while not stream.peek(')'):
                    arm = stream.pull()     # assignment
                    arms.append(arm)
                stream.pull()               # `)`
                syntax = ListSyntax(arms)
            stream.mark(syntax)
        yield syntax

    # Identity constructor.
    identity = grammar.add_rule('''
        identity:
                `[` label_list `]`
    ''')

    # A label group.
    label_group = grammar.add_rule('''
        label_group:
                `[` label_list `]` | `(` label_list `)`
    ''')

    def match_identity(stream):
        is_hard = stream.peek('[')
        stream.pull()               # `[` | `(`
        arms = []
        while not (stream.peek(']') or stream.peek(')')):
            arm = stream.pull()     # label
            arms.append(arm)
        stream.pull()               # `]` | `)`
        syntax = IdentitySyntax(arms, is_hard)
        stream.mark(syntax)
        yield syntax

    identity.set_match(match_identity)
    label_group.set_match(match_identity)

    # List of labels separated by `.`.
    label_list = grammar.add_rule('''
        label_list:
                label ( `.` label )*
    ''')

    @label_list.set_match
    def match_label_list(stream):
        yield stream.pull()         # label
        while stream:
            stream.pull()           # `.`
            yield stream.pull()     # label

    # Atomic label expressions.
    label = grammar.add_rule('''
        label:  %LABEL | %STRING | label_group | reference
    ''')

    @label.set_match
    def match_label(stream):
        if stream.peek(LABEL):
            label = stream.pull()   # %LABEL
            syntax = LabelSyntax(label.text)
            stream.mark(syntax)
            yield syntax
        elif stream.peek(STRING):
            literal = stream.pull() # %STRING
            syntax = StringSyntax(literal.text)
            stream.mark(syntax)
            yield syntax
        else:
            yield stream.pull()     # label_group | reference

    # Literal expressions.
    literal = grammar.add_rule('''
        literal:
                %STRING | %INTEGER | %DECIMAL | %FLOAT
    ''')

    @literal.set_match
    def match_literal(stream):
        if stream.peek(STRING):
            literal = stream.pull()     # %STRING
            syntax = StringSyntax(literal.text)
        elif stream.peek(INTEGER):
            literal = stream.pull()     # %INTEGER
            syntax = IntegerSyntax(literal.text)
        elif stream.peek(DECIMAL):
            literal = stream.pull()     # %DECIMAL
            syntax = DecimalSyntax(literal.text)
        elif stream.peek(FLOAT):
            literal = stream.pull()     # %FLOAT
            syntax = FloatSyntax(literal.text)
        stream.mark(syntax)
        yield syntax

    # Generate and return the parser.
    return grammar()


def parse(text, start=None):
    """
    Parses the input query string into a syntax tree.

    `text`: ``str`` or ``unicode``
        A raw query string.

    `start`: ``unicode`` or ``None``
        The initial production rule (by default, the first rule in the grammar).

    *Returns*: :class:`.Syntax`
        The corresponding syntax tree.
    """
    # Tokenize the input.
    tokens = scan(text)
    # Generate a parser for HTSQL grammar.
    parse = prepare_parse()
    # Parse the input to return a syntax tree.
    return parse(tokens, start)


