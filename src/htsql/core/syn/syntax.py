#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ..util import (maybe, listof, oneof, Clonable, Hashable, Printable,
        YAMLable, to_name, to_literal)
import re
import decimal


class Syntax(Clonable, Hashable, Printable, YAMLable):
    """
    A syntax node.
    """

    def __init__(self):
        # Need a dummy constructor for `Clonable`.
        pass


class VoidSyntax(Syntax):
    """
    Represents a syntax node with no meaning.

    Use when a syntax node is required structurally, but no regular nodes are
    available.
    """

    def __basis__(self):
        return ()

    def __str__(self):
        return ""


class SkipSyntax(Syntax):
    """
    A skip symbol.

    ::

        /
    """

    def __basis__(self):
        return ()

    def __str__(self):
        return "/"


class AssignSyntax(Syntax):
    """
    Assignment expression.

    ::

        <larm> := <rarm>

    `larm`: :class:`SpecifySyntax`
        The left-hand side of assignment.

    `rarm`: :class:`Syntax`
        The assigned value.
    """

    def __init__(self, larm, rarm):
        assert isinstance(larm, SpecifySyntax)
        assert isinstance(rarm, Syntax)
        self.larm = larm
        self.rarm = rarm

    def __basis__(self):
        return (self.larm, self.rarm)

    def __str__(self):
        return "%s:=%s" % (self.larm, self.rarm)

    def __yaml__(self):
        yield ('larm', self.larm)
        yield ('rarm', self.rarm)


class SpecifySyntax(Syntax):
    """
    Left-hand side of an assignment expression.

    ::

        <larm>
        <larm> . <larm> ...
        <larm> . <larm> ... (<rarm>, ...)

    `larms`: [:class:`IdentifierSyntax` or :class:`ReferenceSyntax`]
        A chain of identifiers or references separated by ``.`` symbol.

    `rarms`: [:class:`IdentifierSyntax` or :class:`ReferenceSyntax`] or ``None``
        Formal parameters; ``None`` if no ``()``.

    `identifier`: :class:`IdentifierSyntax`
        Set only if the specifier is an identifier.

    `reference`: :class:`ReferenceSyntax`
        Set only if the specifier is a reference.
    """

    def __init__(self, larms, rarms):
        assert isinstance(larms, listof(oneof(IdentifierSyntax,
                                              ReferenceSyntax)))
        assert len(larms) > 0
        assert isinstance(rarms, maybe(listof(oneof(IdentifierSyntax,
                                                    ReferenceSyntax))))
        self.larms = larms
        self.rarms = rarms
        # Unpack the specifier for common cases:
        #   <identifier> := ...
        #   $ <reference> := ...
        self.identifier = None
        self.reference = None
        if rarms is None and len(larms) == 1:
            if isinstance(larms[0], IdentifierSyntax):
                self.identifier = larms[0]
            if isinstance(larms[0], ReferenceSyntax):
                self.reference = larms[0]

    def __basis__(self):
        return (tuple(self.larms), tuple(self.rarms)
                                   if self.rarms is not None else None)

    def __str__(self):
        chunks = []
        chunks.append(".".join(str(arm) for arm in self.larms))
        if self.rarms is not None:
            chunks.append("(")
            chunks.append(",".join(str(arm) for arm in self.rarms))
            chunks.append(")")
        return "".join(chunks)

    def __yaml__(self):
        yield ('larms', self.larms)
        if self.rarms is not None:
            yield ('rarms', self.rarms)


class ApplySyntax(Syntax):
    """
    Function or operator application.

    This is an abstract class; concrete subclasses correspond to different
    syntax forms of functions and operators.

    `name`: ``unicode``
        Normalized function name or operator symbol.

    `arguments`: list of :class:`Syntax`
        Function arguments or operands of the operator.
    """

    def __init__(self, name, arguments):
        assert isinstance(name, str)
        assert isinstance(arguments, listof(Syntax))
        self.name = name
        self.arguments = arguments


class FunctionSyntax(ApplySyntax):
    """
    Function call notation.

    ::

        <identifier> (<arm>, ...)

    `identifier`: :class:`IdentifierSyntax`
        The function name.

    `arms`: list of :class:`Syntax`
        Function arguments.
    """

    def __init__(self, identifier, arms):
        assert isinstance(identifier, IdentifierSyntax)
        assert isinstance(arms, listof(Syntax))
        super(FunctionSyntax, self).__init__(identifier.name, arms)
        self.identifier = identifier
        self.arms = arms

    def __basis__(self):
        return (self.identifier, tuple(self.arms))

    def __str__(self):
        return "%s(%s)" % (self.identifier,
                            ",".join(str(arm) for arm in self.arms))

    def __yaml__(self):
        yield ('identifier', self.identifier)
        yield ('arms', self.arms)


class PipeSyntax(ApplySyntax):
    """
    Pipe notation for function application.

    ::

        <larm> :<identifier>
        <larm> :<identifier> <rarm>
        <larm> :<identifier> (<rarm>, ...)
        <larm> /:<identifier> (<rarm>, ...)

    `identifier`: :class:`IdentifierSyntax`
        The function name.

    `larm`: :class:`Syntax`
        The first argument.

    `rarms`: [:class:`Syntax`]
        The rest of the arguments.

    `is_flow`: ``bool``
        ``True`` for flow (``:``), ``False`` for segment (``/:``) notation.

    `is_open`: ``bool``
        ``True`` if no right-hand arguments or a single right-hand argument not
        enclosed in parentheses.
    """

    def __init__(self, identifier, larm, rarms, is_flow, is_open):
        assert isinstance(identifier, IdentifierSyntax)
        assert isinstance(larm, Syntax)
        assert isinstance(rarms, listof(Syntax))
        assert isinstance(is_flow, bool)
        assert isinstance(is_open, bool)
        assert not is_open or len(rarms) <= 1
        super(PipeSyntax, self).__init__(identifier.name, [larm]+rarms)
        self.identifier = identifier
        self.larm = larm
        self.rarms = rarms
        self.is_flow = is_flow
        self.is_open = is_open

    def __basis__(self):
        return (self.identifier, self.larm, tuple(self.rarms), self.is_open)

    def __str__(self):
        chunks = []
        chunks.append(str(self.larm))
        if self.is_flow:
            chunks.append(" :")
        else:
            chunks.append("/:")
        chunks.append(str(self.identifier))
        if not self.is_open:
            chunks.append("(")
        elif self.rarms:
            chunks.append(" ")
        chunks.append(",".join(str(arm) for arm in self.rarms))
        if not self.is_open:
            chunks.append(")")
        return "".join(chunks)

    def __yaml__(self):
        yield ('is_flow', self.is_flow)
        yield ('is_open', self.is_open)
        yield ('identifier', self.identifier)
        yield ('larm', self.larm)
        if self.rarms or not self.is_open:
            yield ('rarms', self.rarms)


class OperatorSyntax(ApplySyntax):
    """
    Binary operator.

    ::

        <larm> <symbol> <rarm>

    `symbol`: ``unicode``
        The operator symbol.

    `larm`: :class:`Syntax`
        The left-hand operand.

    `rarm`: :class:`Syntax`
        The right-hand operand.
    """

    def __init__(self, symbol, larm, rarm):
        assert isinstance(symbol, str)
        assert isinstance(larm, Syntax)
        assert isinstance(rarm, Syntax)
        super(OperatorSyntax, self).__init__(symbol, [larm, rarm])
        self.symbol = symbol
        self.larm = larm
        self.rarm = rarm

    def __basis__(self):
        return (self.symbol, self.larm, self.rarm)

    def __str__(self):
        return "%s%s%s" % (self.larm, self.symbol, self.rarm)

    def __yaml__(self):
        yield ('symbol', self.symbol)
        yield ('larm', self.larm)
        yield ('rarm', self.rarm)


class PrefixSyntax(ApplySyntax):
    """
    Unary operator.

    ::

        <symbol> <arm>

    `symbol`: ``unicode``
        The operator symbol.

    `arm`: :class:`Syntax`
        The operand.
    """

    def __init__(self, symbol, arm):
        assert isinstance(symbol, str)
        assert isinstance(arm, Syntax)
        super(PrefixSyntax, self).__init__(symbol, [arm])
        self.symbol = symbol
        self.arm = arm

    def __basis__(self):
        return (self.symbol, self.arm)

    def __str__(self):
        return "%s%s" % (self.symbol, self.arm)

    def __yaml__(self):
        yield ('symbol', self.symbol)
        yield ('arm', self.arm)


class FilterSyntax(OperatorSyntax):
    """
    Filtering operator.

    ::

        <larm> ? <rarm>
    """

    def __init__(self, larm, rarm):
        super(FilterSyntax, self).__init__('?', larm, rarm)

    def __yaml__(self):
        yield ('larm', self.larm)
        yield ('rarm', self.rarm)


class ProjectSyntax(OperatorSyntax):
    """
    Projection operator.

    ::

        <larm> ^ <rarm>
    """

    def __init__(self, larm, rarm):
        super(ProjectSyntax, self).__init__('^', larm, rarm)

    def __yaml__(self):
        yield ('larm', self.larm)
        yield ('rarm', self.rarm)


class LinkSyntax(OperatorSyntax):
    """
    Linking operator (deprecated).

    ::

        <larm> -> <rarm>
    """

    def __init__(self, larm, rarm):
        super(LinkSyntax, self).__init__('->', larm, rarm)

    def __yaml__(self):
        yield ('larm', self.larm)
        yield ('rarm', self.rarm)


class AttachSyntax(OperatorSyntax):
    """
    Attachment operator.

    ::

        <larm> @ <rarm>
    """

    def __init__(self, larm, rarm):
        super(AttachSyntax, self).__init__('@', larm, rarm)

    def __yaml__(self):
        yield ('larm', self.larm)
        yield ('rarm', self.rarm)


class DetachSyntax(PrefixSyntax):
    """
    Detachment operator.

    ::

        @ <arm>
    """

    def __init__(self, arm):
        super(DetachSyntax, self).__init__('@', arm)

    def __yaml__(self):
        yield ('arm', self.arm)


class CollectSyntax(PrefixSyntax):
    """
    Collection operator.

    ::

        / <arm>
    """

    def __init__(self, arm):
        super(CollectSyntax, self).__init__('/', arm)

    def __yaml__(self):
        yield ('arm', self.arm)


class DirectSyntax(Syntax):
    """
    Sorting direction indicator.

    ::

        <arm> +
        <arm> -

    `symbol`: ``unicode`` (``'+'`` or ``'-'``)
        The indicator.

    `arm`: :class:`Syntax`
        The operand.
    """

    def __init__(self, symbol, arm):
        assert isinstance(symbol, str) and symbol in ['+', '-']
        assert isinstance(arm, Syntax)
        self.symbol = symbol
        self.arm = arm

    def __basis__(self):
        return (self.symbol, self.arm)

    def __str__(self):
        return "%s%s" % (self.arm, self.symbol)

    def __yaml__(self):
        yield ('symbol', self.symbol)
        yield ('arm', self.arm)


class ComposeSyntax(Syntax):
    """
    Composition expression.

    ::

        <larm> . <rarm>

    `larm`: :class:`Syntax`
        The left-hand operand.

    `rarm`: :class:`Syntax`
        The right-hand operand.
    """

    def __init__(self, larm, rarm):
        assert isinstance(larm, Syntax)
        assert isinstance(rarm, Syntax)
        self.larm = larm
        self.rarm = rarm

    def __basis__(self):
        return (self.larm, self.rarm)

    endswithint_regexp = re.compile(r'(?:\W|\A)\d+\Z', re.U)

    def __str__(self):
        chunks = []
        chunk = str(self.larm)
        chunks.append(chunk)
        # Make sure we do not accidentally make a decimal literal.
        if self.endswithint_regexp.search(chunk):
            chunks.append(" ")
        chunks.append(".")
        chunks.append(str(self.rarm))
        return "".join(chunks)

    def __yaml__(self):
        yield ('larm', self.larm)
        yield ('rarm', self.rarm)


class UnpackSyntax(Syntax):
    """
    Unpacking expression.

    ::

        *
        * <index>
        * (<index>)

    `index`: ``int``, ``long`` or ``None``
        The number of the field to unpack (1-based).

    `is_open`: ``bool``
        ``True`` if no ``()``.
    """

    def __init__(self, index, is_open):
        assert index is None or (isinstance(index, int) and index >= 0)
        assert isinstance(is_open, bool)
        self.index = index
        self.is_open = is_open

    def __basis__(self):
        return (self.index, self.is_open)

    def __str__(self):
        chunks = []
        chunks.append("*")
        if not self.is_open:
            chunks.append("(")
        if self.index is not None:
            chunks.append(str(self.index))
        if not self.is_open:
            chunks.append(")")
        return "".join(chunks)

    def __yaml__(self):
        if self.index is not None:
            yield ('is_open', self.is_open)
            yield ('index', self.index)


class LiftSyntax(Syntax):
    """
    The lift symbol.

    ::

        ^
    """

    def __basis__(self):
        return ()

    def __str__(self):
        return "^"


class GroupSyntax(Syntax):
    """
    An expression in parentheses.

    ::

        (<arm>)

    `arm`: :class:`Syntax`
        The expression.
    """

    def __init__(self, arm):
        assert isinstance(arm, Syntax)
        self.arm = arm

    def __basis__(self):
        return (self.arm,)

    def __str__(self):
        return "(%s)" % self.arm

    def __yaml__(self):
        yield ('arm', self.arm)


class SelectSyntax(Syntax):
    """
    Selection operator.

    ::

        <larm> {...}

    `larm`: :class:`Syntax`
        The operand.

    `rarm`: :class:`RecordSyntax`
        The selection record.
    """

    def __init__(self, larm, rarm):
        assert isinstance(larm, Syntax)
        assert isinstance(rarm, RecordSyntax)
        self.larm = larm
        self.rarm = rarm

    def __basis__(self):
        return (self.larm, self.rarm)

    def __str__(self):
        return "%s%s" % (self.larm, self.rarm)

    def __yaml__(self):
        yield ('larm', self.larm)
        yield ('rarm', self.rarm)


class LocateSyntax(Syntax):
    """
    Location operator.

    ::

        <larm> [...]

    `larm`: :class:`Syntax`
        The operand.

    `rarm`: :class:`IdentitySyntax`
        The identity.
    """

    def __init__(self, larm, rarm):
        assert isinstance(larm, Syntax)
        assert isinstance(rarm, IdentitySyntax)
        self.larm = larm
        self.rarm = rarm

    def __basis__(self):
        return (self.larm, self.rarm)

    def __str__(self):
        return "%s%s" % (self.larm, self.rarm)

    def __yaml__(self):
        yield ('larm', self.larm)
        yield ('rarm', self.rarm)


class RecordSyntax(Syntax):
    """
    Record constructor.

    ::

        {<arm>, ...}

    `arms`: [:class:`Syntax`]
        Record fields.
    """

    def __init__(self, arms):
        assert isinstance(arms, listof(Syntax))
        self.arms = arms

    def __basis__(self):
        return (tuple(self.arms),)

    def __str__(self):
        return "{%s}" % ",".join(str(arm) for arm in self.arms)

    def __yaml__(self):
        yield ('arms', self.arms)


class ListSyntax(Syntax):
    """
    List constructor.

    ::

        (<arm>, ...)

    `arms`: [:class:`Syntax`]
        List elements.
    """

    def __init__(self, arms):
        assert isinstance(arms, listof(Syntax))
        self.arms = arms

    def __basis__(self):
        return (tuple(self.arms),)

    def __str__(self):
        chunks = []
        chunks.append("(")
        chunks.append("".join(str(arm) for arm in self.arms))
        if len(self.arms) == 1:
            chunks.append(",")
        chunks.append(")")
        return "".join(chunks)

    def __yaml__(self):
        yield ('arms', self.arms)


class IdentitySyntax(Syntax):
    """
    Identity constructor.

    ::

        [<arm> . ...]
        (<arm> . ...)

    `arms`: [:class:`Syntax`]
        Identity labels.

    `is_hard`: ``bool``
        ``True`` for square brackets (``[]``), ``False`` for parentheses
        (``()``).
    """

    def __init__(self, arms, is_hard):
        assert isinstance(arms, listof(Syntax)) and len(arms) > 0
        assert isinstance(is_hard, bool)
        self.arms = arms
        self.is_hard = is_hard

    def __basis__(self):
        return (tuple(self.arms), self.is_hard)

    def __str__(self):
        chunks = []
        if self.is_hard:
            chunks.append("[")
        else:
            chunks.append("(")
        chunks.append(".".join(str(arm) for arm in self.arms))
        if self.is_hard:
            chunks.append("]")
        else:
            chunks.append(")")
        return "".join(chunks)

    def __yaml__(self):
        yield ('is_hard', self.is_hard)
        yield ('arms', self.arms)


class ReferenceSyntax(Syntax):
    """
    Reference expression.

    ::

        $ <identifier>

    `identifier`: :class:`IdentifierSyntax`
        The reference name.

    `name`: ``unicode``
        Normalized identifier name.
    """

    def __init__(self, identifier):
        assert isinstance(identifier, IdentifierSyntax)
        self.identifier = identifier
        self.name = identifier.name

    def __basis__(self):
        return (self.identifier,)

    def __str__(self):
        return "$%s" % self.identifier

    def __yaml__(self):
        yield ('identifier', self.identifier)


class IdentifierSyntax(Syntax):
    """
    An identifier.

    ::

        <text>

    `text`: ``unicode``
        The raw identifier name.

    `name`: ``unicode``
        Normalized name.
    """

    def __init__(self, text):
        assert isinstance(text, str)
        self.text = text
        self.name = to_name(text)

    def __basis__(self):
        return (self.text,)

    def __str__(self):
        return self.text

    def __yaml__(self):
        yield ('text', self.text)
        if self.name != self.text:
            yield ('name', self.name)


class LiteralSyntax(Syntax):
    """
    A literal expression.

    This is an abstract class; concrete subclasses for different forms of
    literal expressions.

    `text`: ``unicode``
        The value of the literal.
    """

    def __init__(self, text):
        assert isinstance(text, str)
        self.text = text

    def __basis__(self):
        return (self.text,)

    def __str__(self):
        return self.text

    def __yaml__(self):
        yield ('text', self.text)


class StringSyntax(LiteralSyntax):
    """
    A string literal.

    A string literal is a sequence of characters enclosed in single quotes.
    """

    def __str__(self):
        return to_literal(self.text)


class LabelSyntax(LiteralSyntax):
    """
    A label literal.

    A label literal is a sequence of alphanumeric characters or ``-`` in an
    identity constructor.
    """

    def __str__(self):
        # Should be safe without escaping?
        return self.text


class NumberSyntax(LiteralSyntax):
    """
    A number literal.

    `value`: ``int``, ``long``, :class:`decimal.Decimal`, or ``float``.
        The numeric value.
    """

    is_integer = False
    is_decimal = False
    is_float = False

    def __init__(self, text, value):
        super(NumberSyntax, self).__init__(text)
        self.value = value


class IntegerSyntax(NumberSyntax):
    """
    An integer literal.
    """

    is_integer = True

    def __init__(self, text):
        super(IntegerSyntax, self).__init__(text, int(text))


class DecimalSyntax(NumberSyntax):
    """
    A decimal literal.

    A decimal literal is a number with a decimal point.
    """

    is_decimal = True

    def __init__(self, text):
        super(DecimalSyntax, self).__init__(text, decimal.Decimal(text))


class FloatSyntax(NumberSyntax):
    """
    A float literal.

    A float literal is a number in exponential notation.
    """

    is_float = True

    def __init__(self, text):
        super(FloatSyntax, self).__init__(text, float(text))


