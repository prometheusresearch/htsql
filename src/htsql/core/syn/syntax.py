#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ..error import Mark, EmptyMark
from ..util import (maybe, listof, oneof, Printable, Clonable, Comparable,
        to_name)
import re
import decimal


class Syntax(Clonable, Comparable, Printable):
    """
    A syntax node.

    `mark`: :class:`.Mark`
        Error context.
    """

    def __init__(self, mark):
        assert isinstance(mark, Mark)
        self.mark = mark


class VoidSyntax(Syntax):
    """
    Represents a syntax node with no meaning.

    Use when a syntax node is required structurally, but no regular nodes are
    available.
    """

    def __init__(self):
        super(VoidSyntax, self).__init__(EmptyMark())

    def __basis__(self):
        return ()

    def __unicode__(self):
        return u""


class SkipSyntax(Syntax):
    """
    A skip symbol.

    ::

        /
    """

    def __basis__(self):
        return ()

    def __unicode__(self):
        return u"/"


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

    def __init__(self, larm, rarm, mark):
        assert isinstance(larm, SpecifySyntax)
        assert isinstance(rarm, Syntax)
        super(AssignSyntax, self).__init__(mark)
        self.larm = larm
        self.rarm = rarm

    def __basis__(self):
        return (self.larm, self.rarm)

    def __unicode__(self):
        return u"%s:=%s" % (self.larm, self.rarm)


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
    """

    def __init__(self, larms, rarms, mark):
        assert isinstance(larms, listof(oneof(IdentifierSyntax,
                                              ReferenceSyntax)))
        assert len(larms) > 0
        assert isinstance(rarms, maybe(listof(oneof(IdentifierSyntax,
                                                    ReferenceSyntax))))
        super(SpecifySyntax, self).__init__(mark)
        self.larms = larms
        self.rarms = rarms

    def __basis__(self):
        return (tuple(self.larms), tuple(self.rarms)
                                   if self.rarms is not None else None)

    def __unicode__(self):
        chunks = []
        chunks.append(u".".join(unicode(arm) for arm in self.larms))
        if self.rarms is not None:
            chunks.append(u"(")
            chunks.append(u",".join(unicode(arm) for arm in self.rarms))
            chunks.append(u")")
        return u"".join(chunks)


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

    def __init__(self, name, arguments, mark):
        assert isinstance(name, unicode)
        assert isinstance(arguments, listof(Syntax))
        super(ApplySyntax, self).__init__(mark)
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

    def __init__(self, identifier, arms, mark):
        assert isinstance(identifier, IdentifierSyntax)
        assert isinstance(arms, listof(Syntax))
        super(FunctionSyntax, self).__init__(identifier.name, arms, mark)
        self.identifier = identifier
        self.arms = arms

    def __basis__(self):
        return (self.identifier, tuple(self.arms))

    def __unicode__(self):
        return u"%s(%s)" % (self.identifier,
                            u",".join(unicode(arm) for arm in self.arms))


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

    def __init__(self, identifier, larm, rarms, is_flow, is_open, mark):
        assert isinstance(identifier, IdentifierSyntax)
        assert isinstance(larm, Syntax)
        assert isinstance(rarms, listof(Syntax))
        assert isinstance(is_flow, bool)
        assert isinstance(is_open, bool)
        assert not is_open or len(rarms) <= 1
        super(PipeSyntax, self).__init__(identifier.name, [larm]+rarms, mark)
        self.identifier = identifier
        self.larm = larm
        self.rarms = rarms
        self.is_flow = is_flow
        self.is_open = is_open

    def __basis__(self):
        return (self.identifier, self.larm, tuple(self.rarms), self.is_open)

    def __unicode__(self):
        chunks = []
        chunks.append(unicode(self.larm))
        if self.is_flow:
            chunks.append(u" :")
        else:
            chunks.append(u"/:")
        chunks.append(unicode(self.identifier))
        if not self.is_open:
            chunks.append(u"(")
        elif self.rarms:
            chunks.append(u" ")
        chunks.append(u",".join(unicode(arm) for arm in self.rarms))
        if not self.is_open:
            chunks.append(u")")
        return u"".join(chunks)


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

    def __init__(self, symbol, larm, rarm, mark):
        assert isinstance(symbol, unicode)
        assert isinstance(larm, Syntax)
        assert isinstance(rarm, Syntax)
        super(OperatorSyntax, self).__init__(symbol, [larm, rarm], mark)
        self.symbol = symbol
        self.larm = larm
        self.rarm = rarm

    def __basis__(self):
        return (self.symbol, self.larm, self.rarm)

    def __unicode__(self):
        return u"%s%s%s" % (self.larm, self.symbol, self.rarm)


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

    def __init__(self, symbol, arm, mark):
        assert isinstance(symbol, unicode)
        assert isinstance(arm, Syntax)
        super(PrefixSyntax, self).__init__(symbol, [arm], mark)
        self.symbol = symbol
        self.arm = arm

    def __basis__(self):
        return (self.symbol, self.arm)

    def __unicode__(self):
        return u"%s%s" % (self.symbol, self.arm)


class FilterSyntax(OperatorSyntax):
    """
    Filtering operator.

    ::

        <larm> ? <rarm>
    """

    def __init__(self, larm, rarm, mark):
        super(FilterSyntax, self).__init__(u'?', larm, rarm, mark)


class ProjectSyntax(OperatorSyntax):
    """
    Projection operator.

    ::

        <larm> ^ <rarm>
    """

    def __init__(self, larm, rarm, mark):
        super(ProjectSyntax, self).__init__(u'^', larm, rarm, mark)


class LinkSyntax(OperatorSyntax):
    """
    Linking operator (deprecated).

    ::

        <larm> -> <rarm>
    """

    def __init__(self, larm, rarm, mark):
        super(LinkSyntax, self).__init__(u'->', larm, rarm, mark)


class AttachSyntax(OperatorSyntax):
    """
    Attachment operator.

    ::

        <larm> @ <rarm>
    """

    def __init__(self, larm, rarm, mark):
        super(AttachSyntax, self).__init__(u'@', larm, rarm, mark)


class DetachSyntax(PrefixSyntax):
    """
    Detachment operator.

    ::

        @ <arm>
    """

    def __init__(self, arm, mark):
        super(DetachSyntax, self).__init__(u'@', arm, mark)


class CollectSyntax(PrefixSyntax):
    """
    Collection operator.

    ::

        / <arm>
    """

    def __init__(self, arm, mark):
        super(CollectSyntax, self).__init__(u'/', arm, mark)


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

    def __init__(self, symbol, arm, mark):
        assert isinstance(symbol, unicode) and symbol in [u'+', u'-']
        assert isinstance(arm, Syntax)
        super(DirectSyntax, self).__init__(mark)
        self.symbol = symbol
        self.arm = arm

    def __basis__(self):
        return (self.symbol, self.arm)

    def __unicode__(self):
        return u"%s%s" % (self.arm, self.symbol)


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

    def __init__(self, larm, rarm, mark):
        assert isinstance(larm, Syntax)
        assert isinstance(rarm, Syntax)
        super(ComposeSyntax, self).__init__(mark)
        self.larm = larm
        self.rarm = rarm

    def __basis__(self):
        return (self.larm, self.rarm)

    endswithint_regexp = re.compile(r'(?:\W|\A)\d+$', re.U)

    def __unicode__(self):
        chunks = []
        chunk = unicode(self.larm)
        chunks.append(chunk)
        # Make sure we do not accidentally make a decimal literal.
        if self.endswithint_regexp.search(chunk):
            chunks.append(u" ")
        chunks.append(u".")
        chunks.append(unicode(self.rarm))
        return u"".join(chunks)


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

    def __init__(self, index, is_open, mark):
        super(UnpackSyntax, self).__init__(mark)
        assert index is None or (isinstance(index, (int, long)) and index >= 0)
        assert isinstance(is_open, bool)
        self.index = index
        self.is_open = is_open

    def __basis__(self):
        return (self.index, self.is_open)

    def __unicode__(self):
        chunks = []
        chunks.append(u"*")
        if not self.is_open:
            chunks.append(u"(")
        if self.index is not None:
            chunks.append(unicode(self.index))
        if not self.is_open:
            chunks.append(u")")
        return u"".join(chunks)


class ComplementSyntax(Syntax):
    """
    The complement symbol.

    ::

        ^
    """

    def __basis__(self):
        return ()

    def __unicode__(self):
        return u"^"


class GroupSyntax(Syntax):
    """
    An expression in parentheses.

    ::

        (<arm>)

    `arm`: :class:`Syntax`
        The expression.
    """

    def __init__(self, arm, mark):
        assert isinstance(arm, Syntax)
        super(GroupSyntax, self).__init__(mark)
        self.arm = arm

    def __basis__(self):
        return (self.arm,)

    def __unicode__(self):
        return u"(%s)" % self.arm


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

    def __init__(self, larm, rarm, mark):
        assert isinstance(larm, Syntax)
        assert isinstance(rarm, RecordSyntax)
        super(SelectSyntax, self).__init__(mark)
        self.larm = larm
        self.rarm = rarm

    def __basis__(self):
        return (self.larm, self.rarm)

    def __unicode__(self):
        return u"%s%s" % (self.larm, self.rarm)


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

    def __init__(self, larm, rarm, mark):
        assert isinstance(larm, Syntax)
        assert isinstance(rarm, IdentitySyntax)
        super(LocateSyntax, self).__init__(mark)
        self.larm = larm
        self.rarm = rarm

    def __basis__(self):
        return (self.larm, self.rarm)

    def __unicode__(self):
        return u"%s%s" % (self.larm, self.rarm)


class RecordSyntax(Syntax):
    """
    Record constructor.

    ::

        {<arm>, ...}

    `arms`: [:class:`Syntax`]
        Record fields.
    """

    def __init__(self, arms, mark):
        assert isinstance(arms, listof(Syntax))
        super(RecordSyntax, self).__init__(mark)
        self.arms = arms

    def __basis__(self):
        return (tuple(self.arms),)

    def __unicode__(self):
        return u"{%s}" % u",".join(unicode(arm) for arm in self.arms)


class ListSyntax(Syntax):
    """
    List constructor.

    ::

        (<arm>, ...)

    `arms`: [:class:`Syntax`]
        List elements.
    """

    def __init__(self, arms, mark):
        assert isinstance(arms, listof(Syntax))
        super(ListSyntax, self).__init__(mark)
        self.arms = arms

    def __basis__(self):
        return (tuple(self.arms),)

    def __unicode__(self):
        chunks = []
        chunks.append(u"(")
        chunks.append(u"".join(unicode(arm) for arm in self.arms))
        if len(self.arms) == 1:
            chunks.append(u",")
        chunks.append(u")")
        return u"".join(chunks)


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

    def __init__(self, arms, is_hard, mark):
        assert isinstance(arms, listof(Syntax)) and len(arms) > 0
        assert isinstance(is_hard, bool)
        super(IdentitySyntax, self).__init__(mark)
        self.arms = arms
        self.is_hard = is_hard

    def __basis__(self):
        return (tuple(self.arms), self.is_hard)

    def __unicode__(self):
        chunks = []
        if self.is_hard:
            chunks.append(u"[")
        else:
            chunks.append(u"(")
        chunks.append(u".".join(unicode(arm) for arm in self.arms))
        if self.is_hard:
            chunks.append(u"]")
        else:
            chunks.append(u")")
        return u"".join(chunks)


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

    def __init__(self, identifier, mark):
        assert isinstance(identifier, IdentifierSyntax)
        super(ReferenceSyntax, self).__init__(mark)
        self.identifier = identifier
        self.name = identifier.name

    def __basis__(self):
        return (self.identifier,)

    def __unicode__(self):
        return u"$%s" % self.identifier


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

    def __init__(self, text, mark):
        assert isinstance(text, unicode)
        super(IdentifierSyntax, self).__init__(mark)
        self.text = text
        self.name = to_name(text)

    def __basis__(self):
        return (self.text,)

    def __unicode__(self):
        return self.text


class LiteralSyntax(Syntax):
    """
    A literal expression.

    This is an abstract class; concrete subclasses for different forms of
    literal expressions.

    `text`: ``unicode``
        The value of the literal.
    """

    @staticmethod
    def escape(text,
               regexp=re.compile(r"[\x00-\x1F%\x7F]", re.U),
               replace=(lambda m: u"%%%02X" % ord(m.group()))):
        # %-encode non-printable characters.
        return regexp.sub(replace, text)

    def __init__(self, text, mark):
        assert isinstance(text, unicode)
        super(LiteralSyntax, self).__init__(mark)
        self.text = text

    def __basis__(self):
        return (self.text,)

    def __unicode__(self):
        return self.text


class StringSyntax(LiteralSyntax):
    """
    A string literal.

    A string literal is a sequence of characters enclosed in single quotes.
    """

    def __unicode__(self):
        return u"'%s'" % self.escape(self.text.replace(u"'", u"''"))


class LabelSyntax(LiteralSyntax):
    """
    A label literal.

    A label literal is a sequence of alphanumeric characters or ``-`` in an
    identity constructor.
    """

    def __unicode__(self):
        return self.escape(self.text)


class NumberSyntax(LiteralSyntax):
    """
    A number literal.

    `value`: ``int``, ``long``, :class:`decimal.Decimal`, or ``float``.
        The numeric value.
    """

    is_integer = False
    is_decimal = False
    is_float = False

    def __init__(self, text, value, mark):
        super(NumberSyntax, self).__init__(text, mark)
        self.value = value


class IntegerSyntax(NumberSyntax):
    """
    An integer literal.
    """

    is_integer = True

    def __init__(self, text, mark):
        super(IntegerSyntax, self).__init__(text, int(text), mark)


class DecimalSyntax(NumberSyntax):
    """
    A decimal literal.

    A decimal literal is a number with a decimal point.
    """

    is_decimal = True

    def __init__(self, text, mark):
        super(DecimalSyntax, self).__init__(text, decimal.Decimal(text), mark)


class FloatSyntax(NumberSyntax):
    """
    A float literal.

    A float literal is a number in exponential notation.
    """

    is_float = True

    def __init__(self, text, mark):
        super(FloatSyntax, self).__init__(text, float(text), mark)


