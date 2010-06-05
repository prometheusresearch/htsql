#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
This module defines syntax nodes for the HTSQL grammar.
"""


from ..mark import Mark
from ..util import maybe, listof
import re


class Syntax(object):
    """
    Represents a syntax node.

    `mark` (:class:`htsql.mark.Mark`)
        The location of the node in the original query.
    """

    # The pattern to %-escape unsafe characters.
    escape_pattern = r"[\x00-\x1F%\x7F]"
    escape_regexp = re.compile(escape_pattern)
    escape_replace = (lambda m: "%%%02X" % ord(m.group()))

    def __init__(self, mark):
        assert isinstance(mark, Mark)
        self.mark = mark

    def __str__(self):
        """
        Returns an HTSQL expression that would produce the same syntax node.
        """
        # Override when subclassing.
        raise NotImplementedError()

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self)


class QuerySyntax(Syntax):
    """
    Represents an HTSQL query.

    An HTSQL query consists of a segment expression.

    `segment` (:class:`SegmentSyntax` or ``None``)
        The segment expression.
    """

    def __init__(self, segment, mark):
        assert isinstance(segment, maybe(SegmentSyntax))

        super(QuerySyntax, self).__init__(mark)
        self.segment = segment

    def __str__(self):
        # Generate an HTSQL query corresponding to the node.
        # Note that since the format identifier could split the segment
        # expression between the selector and the filter, we have to
        # serialize parts of the segment expression separately.
        chunks = []
        chunks.append('/')
        if self.segment is not None:
            chunks.append(str(self.segment))
        return ''.join(chunks)


class SegmentSyntax(Syntax):
    """
    Represents a segment expression.

    A segment expression consists of the base expression, the selector,
    and the filter::

        /base{selector}?filter

    `base` (:class:`Syntax` or ``None``)
        The base expression.

    `selector` (:class:`SelectorSyntax` or ``None``)
        The selector.

    `filter` (:class:`Syntax` or ``None``)
        The filter expression.
    """

    def __init__(self, base, selector, filter, mark):
        assert isinstance(base, maybe(Syntax))
        assert isinstance(selector, maybe(SelectorSyntax))
        assert isinstance(filter, maybe(Syntax))

        super(SegmentSyntax, self).__init__(mark)
        self.base = base
        self.selector = selector
        self.filter = filter

    def __str__(self):
        # Generate an HTSQL expression of the form:
        #   base{selector}?filter
        chunks = []
        if self.base is not None:
            chunks.append(str(self.base))
        if self.selector is not None:
            chunks.append(str(self.selector))
        if self.filter is not None:
            chunks.append('?')
            chunks.append(str(self.filter))
        return ''.join(chunks)


class SelectorSyntax(Syntax):
    """
    Represents a selector expression.

    A selector is a sequence of elements::

        {element, ...}

    `elements` (a list of :class:`Syntax`)
        The list of selector elements.
    """

    def __init__(self, elements, mark):
        assert isinstance(elements, listof(Syntax))

        super(SelectorSyntax, self).__init__(mark)
        self.elements = elements

    def __str__(self):
        # Generate an expression of the form:
        #   {element,...}
        return '{%s}' % ','.join(str(element)
                                 for element in self.elements)


class SieveSyntax(Syntax):
    """
    Represents a sieve expression.

    A sieve expression has the same shape as the segment expression.
    It consists of the base, the selector and the filter::

        base{selector}?filter

    `base` (:class:`Syntax`)
        The sieve base expression.

    `selector` (:class:`SelectorSyntax` or ``None``)
        The sieve selector.

    `filter` (:class:`Syntax` or ``None``)
        The sieve filter expression.
    """

    def __init__(self, base, selector, filter, mark):
        assert isinstance(base, Syntax)
        assert isinstance(selector, maybe(SelectorSyntax))
        assert isinstance(filter, maybe(Syntax))
        assert selector is not None or filter is not None

        super(SieveSyntax, self).__init__(mark)
        self.base = base
        self.selector = selector
        self.filter = filter

    def __str__(self):
        # Generate an expression of the form:
        #   base{selector}?filter
        chunks = []
        chunks.append(str(self.base))
        if self.selector is not None:
            chunks.append(str(self.selector))
        if self.filter is not None:
            chunks.append('?')
            chunks.append(str(self.filter))
        return ''.join(chunks)


class OperatorSyntax(Syntax):
    """
    Represents an operator expression.

    An operator expression has one of the following forms::

        left <symbol> right
        left <symbol>
        <symbol> right

    `symbol` (a string)
        The operator.

    `left` (:class:`Syntax` or ``None``)
        The left argument.

    `right` (:class:`Syntax` or ``None``)
        The right argument.
    """

    def __init__(self, symbol, left, right, mark):
        assert isinstance(symbol, str)
        assert isinstance(left, maybe(Syntax))
        assert isinstance(right, maybe(Syntax))
        assert left is not None or right is not None
        super(OperatorSyntax, self).__init__(mark)
        self.symbol = symbol
        self.left = left
        self.right = right

    def __str__(self):
        # Generate an expression of the form:
        #   left<symbol>right
        chunks = []
        if self.left is not None:
            chunks.append(str(self.left))
        chunks.append(self.symbol)
        if self.right is not None:
            chunks.append(str(self.right))
        return ''.join(chunks)


class FunctionOperatorSyntax(Syntax):
    """
    Represents a binary function call in the operator form.

    This expression has the form::

        left identifier right

    and is equivalent to the expression::

        identifier(left, right)

    `identifier` (:class:`IdentifierSyntax`)
        The function name.

    `left` (:class:`Syntax`)
        The first argument.

    `right` (:class:`Syntax`)
        The second argument.
    """

    def __init__(self, identifier, left, right, mark):
        assert isinstance(identifier, IdentifierSyntax)
        assert isinstance(left, Syntax)
        assert isinstance(right, Syntax)

        super(FunctionOperatorSyntax, self).__init__(mark)
        self.identifier = identifier
        self.left = left
        self.right = right

    def __str__(self):
        # Generate an expression of the form:
        #   left identifier right
        chunks = []
        if self.left is not None:
            chunks.append(str(self.left))
            chunks.append(' ')
        chunks.append(str(self.identifier))
        if self.right is not None:
            chunks.append(' ')
            chunks.append(str(self.right))
        return ''.join(chunks)


class FunctionCallSyntax(Syntax):
    """
    Represents a function or a method call.

    This expression has one of the forms::

        identifier(arguments)
        base.identifier(arguments)

    `base` (:class:`Syntax` or ``None``)
        The method base.

    `identifier` (:class:`IdentifierSyntax`)
        The function name.

    `arguments` (a list of :class:`Syntax`)
        The list of arguments.
    """

    def __init__(self, base, identifier, arguments, mark):
        assert isinstance(base, maybe(Syntax))
        assert isinstance(identifier, IdentifierSyntax)
        assert isinstance(arguments, listof(Syntax))

        super(FunctionCallSyntax, self).__init__(mark)
        self.base = base
        self.identifier = identifier
        self.arguments = arguments

    def __str__(self):
        # Generate an expression of the form:
        #   base.identifier(arguments)
        chunks = []
        if self.base is not None:
            chunks.append(str(self.base))
            chunks.append('.')
        chunks.append(str(self.identifier))
        chunks.append('(%s)' % ','.join(str(argument)
                                        for argument in self.arguments))
        return ''.join(chunks)


class GroupSyntax(Syntax):
    """
    Represents an expression in parentheses.

    `expression` (:class:`Syntax`)
        The expression.
    """
    # We keep the parentheses in the syntax tree to ease the reverse
    # translation from the syntax tree to an HTSQL query.

    def __init__(self, expression, mark):
        assert isinstance(expression, Syntax)

        super(GroupSyntax, self).__init__(mark)
        self.expression = expression

    def __str__(self):
        return '(%s)' % self.expression


class SpecifierSyntax(Syntax):
    """
    Represents a specifier expression.

    A specifier expression has one of the forms::

        base.identifier
        base.*

    `base` (:class:`Syntax`)
        The specifier base.

    `identifier` (:class:`IdentifierSyntax` or :class:`WildcardSyntax`)
        The specifier identifier.
    """

    def __init__(self, base, identifier, mark):
        assert isinstance(base, Syntax)
        assert isinstance(identifier, (IdentifierSyntax, WildcardSyntax))

        super(SpecifierSyntax, self).__init__(mark)
        self.base = base
        self.identifier = identifier

    def __str__(self):
        return '%s.%s' % (self.base, self.identifier)


class IdentifierSyntax(Syntax):
    """
    Represents an identifier.

    `value` (a string)
        The identifier.
    """

    def __init__(self, value, mark):
        assert isinstance(value, str)
        super(IdentifierSyntax, self).__init__(mark)

        self.value = value

    def __str__(self):
        return self.value


class WildcardSyntax(Syntax):
    """
    Represents a wildcard.
    """

    def __str__(self):
        return '*'


class LiteralSyntax(Syntax):
    """
    Represents a literal expression.

    This is an abstract class with subclasses :class:`StringSyntax` and
    :class:`NumberSyntax`.

    `value` (a string)
        The value.
    """

    def __init__(self, value, mark):
        assert isinstance(value, str)
        super(LiteralSyntax, self).__init__(mark)
        self.value = value


class StringSyntax(LiteralSyntax):
    """
    Represents a string literal.
    """

    def __str__(self):
        # Quote and %-encode the value.
        value = '\'%s\'' % self.value.replace('\'', '\'\'')
        value = self.escape_regexp.sub(self.escape_replace, value)
        return value


class NumberSyntax(LiteralSyntax):
    """
    Represents a number literal.
    """

    def __str__(self):
        return self.value


