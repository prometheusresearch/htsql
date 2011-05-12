#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.syntax`
======================

This module defines syntax nodes for the HTSQL grammar.
"""


from ..mark import Mark
from ..util import maybe, listof, Printable, Clonable
import re


class Syntax(Printable, Clonable):
    """
    Represents a syntax node.

    `mark` (:class:`htsql.mark.Mark`)
        The location of the node in the original query.
    """

    # The pattern to %-escape unsafe characters.
    escape_pattern = r"[\x00-\x1F%\x7F]"
    escape_regexp = re.compile(escape_pattern)
    escape_replace = (lambda s, m: "%%%02X" % ord(m.group()))

    def __init__(self, mark):
        assert isinstance(mark, Mark)
        self.mark = mark

    def __str__(self):
        """
        Returns an HTSQL expression that would produce the same syntax node.
        """
        # Override when subclassing.
        raise NotImplementedError()


class QuerySyntax(Syntax):
    """
    Represents an HTSQL query.

    An HTSQL query consists of a segment expression.

    `segment` (:class:`SegmentSyntax` or ``None``)
        The segment expression.

    `format` (:class:`FormatSyntax` or ``None``)
        The format indicator.
    """

    def __init__(self, segment, format, mark):
        assert isinstance(segment, maybe(SegmentSyntax))
        assert isinstance(format, maybe(FormatSyntax))

        super(QuerySyntax, self).__init__(mark)
        self.segment = segment
        self.format = format

    def __str__(self):
        # Generate an HTSQL query corresponding to the node.
        chunks = []
        if self.segment is not None:
            chunks.append('/')
            chunks.append(str(self.segment))
        if self.format is not None:
            chunks.append('/')
            chunks.append(str(self.format))
        if not chunks:
            chunks.append('/')
        return ''.join(chunks)


class SegmentSyntax(Syntax):
    """
    Represents a segment expression.
    """

    def __init__(self, branch, mark):
        assert isinstance(branch, Syntax)

        super(SegmentSyntax, self).__init__(mark)
        self.branch = branch

    def __str__(self):
        return str(self.branch)


class FormatSyntax(Syntax):

    def __init__(self, identifier, mark):
        assert isinstance(identifier, IdentifierSyntax)

        super(FormatSyntax, self).__init__(mark)
        self.identifier = identifier

    def __str__(self):
        return ':%s' % self.identifier


class SelectorSyntax(Syntax):
    """
    Represents a selector expression.

    A selector is a sequence of elements::

        {rbranch, ...}
        lbranch{rbranch, ...}

    `lbranch` (:class:`Syntax` or ``None``)
        The selector base.

    `rbranches` (a list of :class:`Syntax`)
        The list of selector elements.
    """

    def __init__(self, lbranch, rbranches, mark):
        assert isinstance(lbranch, maybe(Syntax))
        assert isinstance(rbranches, listof(Syntax))

        super(SelectorSyntax, self).__init__(mark)
        self.lbranch = lbranch
        self.rbranches = rbranches

    def __str__(self):
        # Generate an expression of the form:
        #   {rbranch,...}
        # or
        #   lbranch{rbranch,...}
        chunks = []
        if self.lbranch is not None:
            chunks.append(str(self.lbranch))
        chunks.append('{')
        chunks.append(','.join(str(rbranch) for rbranch in self.rbranches))
        chunks.append('}')
        return ''.join(chunks)


class ApplicationSyntax(Syntax):
    """
    Represents a function or an operator call.

    This is an abstract class with three concrete subclasses
    corresponding to operators, functional operators, and
    function or method calls.

    `name` (a string)
        The name of the function or the operator.

    `arguments` (a list of :class:`Syntax`)
        The list of argument.
    """

    def __init__(self, name, arguments, mark):
        assert isinstance(arguments, listof(Syntax))
        super(ApplicationSyntax, self).__init__(mark)
        self.name = name
        self.arguments = arguments


class OperatorSyntax(ApplicationSyntax):
    """
    Represents an operator expression.

    An operator expression has one of the following forms::

        lbranch <symbol> rbranch
        lbranch <symbol>
        <symbol> rbranch

    Note that for a binary operator, the name coincides with the `<symbol>`,
    for a prefix operator, the name has the form `<symbol>_`, for a postfix
    operator, the name has the form `_<symbol>`.

    `symbol` (a string)
        The operator.

    `lbranch` (:class:`Syntax` or ``None``)
        The left operand.

    `rbranch` (:class:`Syntax` or ``None``)
        The right operand.
    """

    def __init__(self, symbol, lbranch, rbranch, mark):
        assert isinstance(symbol, str)
        assert isinstance(lbranch, maybe(Syntax))
        assert isinstance(rbranch, maybe(Syntax))
        assert lbranch is not None or rbranch is not None
        # For a binary operator, the name is equal to `<symbol>`, for a prefix
        # operator, the name is equal to `<symbol>_`, for a postfix operator,
        # the name is equal to `_<symbol>`.  Thus `a+b`, `+b`, and `a+` are
        # operators with the names `+`, `+_` and `_+` respectively.
        name = symbol
        if lbranch is None:
            name = name+'_'
        if rbranch is None:
            name = '_'+name
        # Gather the arguments.  Both prefix and postfix operators have
        # one argument; since they have different names even when the
        # operator symbol is the same, we don't have to mark the argument
        # as left one or right one.
        arguments = []
        if lbranch is not None:
            arguments.append(lbranch)
        if rbranch is not None:
            arguments.append(rbranch)
        super(OperatorSyntax, self).__init__(name, arguments, mark)
        self.symbol = symbol
        self.lbranch = lbranch
        self.rbranch = rbranch

    def __str__(self):
        # Generate an expression of the form:
        #   lbranch<symbol>rbranch
        chunks = []
        if self.lbranch is not None:
            chunks.append(str(self.lbranch))
        chunks.append(self.symbol)
        if self.rbranch is not None:
            chunks.append(str(self.rbranch))
        return ''.join(chunks)


class SpecifierSyntax(OperatorSyntax):

    def __init__(self, symbol, lbranch, rbranch, mark):
        assert symbol == '.'
        super(SpecifierSyntax, self).__init__(symbol, lbranch, rbranch, mark)


class TransformSyntax(ApplicationSyntax):
    """
    Represents a function call in the infix or postfix form.

    This expression has one of the forms::

        lbranch :identifier
        lbranch :identifier rbranch
        lbranch :identifier (rbranch, ...)

    and is equivalent to the expression::

        identifier(lbranch, rbranch, ...)

    `identifier` (:class:`IdentifierSyntax`)
        The function name.

    `lbranch` (:class:`Syntax`)
        The left operand.

    `rbranches` (a list of :class:`Syntax`)
        The right operands.
    """

    def __init__(self, identifier, lbranch, rbranches, mark):
        assert isinstance(identifier, IdentifierSyntax)
        assert isinstance(lbranch, Syntax)
        assert isinstance(rbranches, listof(Syntax))

        name = identifier.value
        arguments = [lbranch] + rbranches
        super(TransformSyntax, self).__init__(name, arguments, mark)
        self.identifier = identifier
        self.lbranch = lbranch
        self.rbranches = rbranches

    def __str__(self):
        # Generate an expression of the form:
        #   lbranch:identifier(rbranch,...)
        chunks = []
        chunks.append(str(self.lbranch))
        chunks.append(':%s' % self.identifier)
        if self.rbranches:
            chunks.append('(%s)' % ','.join(str(rbranch)
                                            for rbranch in self.rbranches))
        return ''.join(chunks)


class FunctionSyntax(ApplicationSyntax):
    """
    Represents a function or a method call.

    This expression has the form::

        identifier(branch,...)

    `identifier` (:class:`IdentifierSyntax`)
        The function name.

    `branches` (a list of :class:`Syntax`)
        The list of arguments.
    """

    def __init__(self, identifier, branches, mark):
        assert isinstance(identifier, IdentifierSyntax)
        assert isinstance(branches, listof(Syntax))

        name = identifier.value
        arguments = branches
        super(FunctionSyntax, self).__init__(name, arguments, mark)
        self.identifier = identifier
        self.branches = branches

    def __str__(self):
        # Generate an expression of the form:
        #   identifier(arguments)
        chunks = []
        chunks.append(str(self.identifier))
        chunks.append('(%s)' % ','.join(str(branch)
                                        for branch in self.branches))
        return ''.join(chunks)


class GroupSyntax(Syntax):
    """
    Represents an expression in parentheses.

    `branch` (:class:`Syntax`)
        The branch.
    """
    # We keep the parentheses in the syntax tree to ease the reverse
    # translation from the syntax tree to an HTSQL query.

    def __init__(self, branch, mark):
        assert isinstance(branch, Syntax)

        super(GroupSyntax, self).__init__(mark)
        self.branch = branch

    def __str__(self):
        return '(%s)' % self.branch


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

    `index` (:class:`NumberSyntax` or ``None``)
        The index in a wildcard expression.
    """

    def __init__(self, index, mark):
        assert isinstance(index, maybe(NumberSyntax))

        super(WildcardSyntax, self).__init__(mark)
        self.index = index

    def __str__(self):
        # FIXME: wrap the index with parentheses to avoid
        # interpreting a trailing `.` as a decimal point?
        if self.index is not None:
            return '*%s' % self.index
        return '*'


class ComplementSyntax(Syntax):
    """
    Represents a complement.
    """

    def __str__(self):
        return '^'


class ReferenceSyntax(Syntax):
    """
    Represents a reference.

    `identifier` (:class:`IdentifierSyntax`)
        The name of the reference.
    """

    def __init__(self, identifier, mark):
        assert isinstance(identifier, IdentifierSyntax)
        super(ReferenceSyntax, self).__init__(mark)
        self.identifier = identifier

    def __str__(self):
        return "$%s" % self.identifier


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
        # FIXME: what is the number is a base of a specifier?
        return self.value


