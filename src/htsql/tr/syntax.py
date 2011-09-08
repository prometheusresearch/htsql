#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
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

    The syntax tree expresses the structure of the input HTSQL query, with each
    node corresponding to some rule in the HTSQL grammar.

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
        Returns a fragment of an HTSQL query that, when parsed back, produces
        the same syntax node.
        """
        # Override when subclassing.
        raise NotImplementedError()


class QuerySyntax(Syntax):
    """
    Represents an HTSQL query.

    `segment` (:class:`SegmentSyntax`)
        The top-level segment.
    """

    def __init__(self, segment, mark):
        assert isinstance(segment, SegmentSyntax)

        super(QuerySyntax, self).__init__(mark)
        self.segment = segment

    def __str__(self):
        # Generate an HTSQL query:
        #   /<segment>
        return str(self.segment)


class SegmentSyntax(Syntax):
    """
    Represents a segment expression.

    `branch` (:class:`Syntax` or ``None``)
        An expression.
    """

    def __init__(self, branch, mark):
        assert isinstance(branch, maybe(Syntax))

        super(SegmentSyntax, self).__init__(mark)
        self.branch = branch

    def __str__(self):
        # Display:
        #   /<branch>
        if self.branch is None:
            return '/'
        elif isinstance(self.branch, CommandSyntax):
            return str(self.branch)
        else:
            return '/%s' % self.branch


class SelectorSyntax(Syntax):
    """
    Represents a selector expression.

    A selector is a comma-separated list of expression enclosed in
    curly brakets, with an optional selector base::

        {<rbranch>, ...}
        <lbranch>{<rbranch>, ...}

    `lbranch` (:class:`Syntax` or ``None``)
        The selector base.

    `rbranches` (a list of :class:`Syntax`)
        Selector elements.
    """

    def __init__(self, lbranch, rbranches, mark):
        assert isinstance(lbranch, maybe(Syntax))
        assert isinstance(rbranches, listof(Syntax))

        super(SelectorSyntax, self).__init__(mark)
        self.lbranch = lbranch
        self.rbranches = rbranches

    def __str__(self):
        # Generate an HTSQL fragment:
        #   {<rbranch>,...}
        # or
        #   <lbranch>{<rbranch>,...}
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
    corresponding to function calls, function calls in infix form
    and operators.

    `name` (a string)
        The name of the function or the operator.

    `arguments` (a list of :class:`Syntax`)
        The list of arguments.
    """

    def __init__(self, name, arguments, mark):
        assert isinstance(name, str)
        assert isinstance(arguments, listof(Syntax))
        super(ApplicationSyntax, self).__init__(mark)
        self.name = name
        self.arguments = arguments


class FunctionSyntax(ApplicationSyntax):
    """
    Represents a function call.

    A function call starts with the function name followed by
    the list of arguments enclosed in parentheses::

        <identifier>(<branch>, ...)

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
        # Generate a fragment:
        #   <identifier>(<branch>,...)
        chunks = []
        chunks.append(str(self.identifier))
        chunks.append('(%s)' % ','.join(str(branch)
                                        for branch in self.branches))
        return ''.join(chunks)


class MappingSyntax(ApplicationSyntax):
    """
    Represents a function call in infix or postfix form.

    This expression has one of the forms::

        <lbranch> :<identifier>
        <lbranch> :<identifier> <rbranch>
        <lbranch> :<identifier> (<rbranch>, ...)

    and is equivalent to a regular function call::

        <identifier>(<lbranch>, <rbranch>, ...)

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
        super(MappingSyntax, self).__init__(name, arguments, mark)
        self.identifier = identifier
        self.lbranch = lbranch
        self.rbranches = rbranches

    def __str__(self):
        # Generate an HTSQL fragment:
        #   <lbranch>:<identifier>(<rbranch>,...)
        chunks = []
        chunks.append(str(self.lbranch))
        chunks.append(':%s' % self.identifier)
        if self.rbranches:
            chunks.append('(%s)' % ','.join(str(rbranch)
                                            for rbranch in self.rbranches))
        return ''.join(chunks)


class CommandSyntax(MappingSyntax):

    def __init__(self, identifier, lbranch, rbranches, mark):
        assert isinstance(lbranch, SegmentSyntax)
        super(CommandSyntax, self).__init__(identifier, lbranch, rbranches,
                                            mark)

    def __str__(self):
        chunks = []
        chunks.append(str(self.lbranch))
        chunks.append('/:%s' % self.identifier)
        if self.rbranches:
            chunks.append('(%s)' % ','.join(str(rbranch)
                                            for rbranch in self.rbranches))
        return ''.join(chunks)


class OperatorSyntax(ApplicationSyntax):
    """
    Represents an operator expression.

    An operator expression has one of the forms::

        <lbranch> <symbol> <rbranch>
                  <symbol> <rbranch>
        <lbranch> <symbol>

    The operator name is composed from the symbol:

    * `<symbol>` for infix binary operators;
    * `<symbol>_` for prefix unary operators;
    * `_<symbol>` for postfix unary operators.

    Some operators (those with non-standard precedence) are separated
    into subclasses of :class:`OperatorSyntax`.

    `symbol` (a string)
        The operator symbol.

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
        # The operator name is derived from the symbol by the rule:
        # * `<symbol>` for infix operators;
        # * `<symbol>_` for prefix operators;
        # * `_<symbol>` for postfix operators.
        # Thus, `a+b`, `+b` and `a+` are operators with the names `+`, `+_`
        # and `_+` respectively.
        name = symbol
        if lbranch is None:
            name = name+'_'
        if rbranch is None:
            name = '_'+name
        # Gather the arguments.  We distinguish prefix and postfix operators
        # by name, so we don't need to specifically note the original position
        # of the arguments.
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
        # Generate a fragment:
        #   <lbranch><symbol><rbranch>
        chunks = []
        if self.lbranch is not None:
            chunks.append(str(self.lbranch))
        chunks.append(self.symbol)
        if self.rbranch is not None:
            chunks.append(str(self.rbranch))
        return ''.join(chunks)


class QuotientSyntax(OperatorSyntax):
    """
    Represents a quotient operator.

    ::

        <lbranch> ^ <rbranch>
    """

    def __init__(self, lbranch, rbranch, mark):
        super(QuotientSyntax, self).__init__('^', lbranch, rbranch, mark)


class SieveSyntax(OperatorSyntax):
    """
    Represents a sieve operator.

    ::

        <lbranch> ? <rbranch>
    """

    def __init__(self, lbranch, rbranch, mark):
        super(SieveSyntax, self).__init__('?', lbranch, rbranch, mark)


class LinkSyntax(OperatorSyntax):
    """
    Represents a linking operator.

    ::

        <lbranch> -> <rbranch>
    """

    def __init__(self, lbranch, rbranch, mark):
        super(LinkSyntax, self).__init__('->', lbranch, rbranch, mark)


class AssignmentSyntax(OperatorSyntax):
    """
    Represents an assignment operator.

    ::

        <lbranch> := <rbranch>
    """

    def __init__(self, lbranch, rbranch, mark):
        super(AssignmentSyntax, self).__init__(':=', lbranch, rbranch, mark)


class SpecifierSyntax(OperatorSyntax):
    """
    Represents a specifier expression.

    ::

        <lbranch> . <rbranch>
    """

    def __init__(self, lbranch, rbranch, mark):
        super(SpecifierSyntax, self).__init__('.', lbranch, rbranch, mark)


class GroupSyntax(Syntax):
    """
    Represents an expression in parentheses.

    The parentheses are kept in the syntax tree to make sure the
    serialization from the syntax tree to HTSQL obeys the grammar.

    `branch` (:class:`Syntax`)
        The branch.
    """

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
        The identifier value.
    """

    def __init__(self, value, mark):
        assert isinstance(value, str)
        # FIXME: check for a valid identifier.

        super(IdentifierSyntax, self).__init__(mark)
        self.value = value

    def __str__(self):
        return self.value


class WildcardSyntax(Syntax):
    """
    Represents a wildcard expression.

    ::

        *
        * <index>

    `index` (:class:`NumberSyntax` or ``None``)
        The index in a wildcard expression.
    """

    def __init__(self, index, mark):
        assert isinstance(index, maybe(NumberSyntax))

        super(WildcardSyntax, self).__init__(mark)
        self.index = index

    def __str__(self):
        # Generate a fragment:
        #   *<index>
        # FIXME: may break if followed by a specifier (`.`) symbol.
        if self.index is not None:
            return '*%s' % self.index
        return '*'


class ComplementSyntax(Syntax):
    """
    Represents a complement expression.

    ::

        ^
    """

    def __str__(self):
        return '^'


class ReferenceSyntax(Syntax):
    """
    Represents a reference.

    A reference is an identifier preceded by symbol ``$``::

        $ <identifier>

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

    Attributes:

    `is_integer` (Boolean)
        Set if the number is in integer notation.

    `is_decimal` (Boolean)
        Set if the number is in decimal notation.

    `is_exponential` (Boolean)
        Set if the number is in exponential notation.
    """

    def __init__(self, value, mark):
        super(NumberSyntax, self).__init__(value, mark)

        # Determine the notation of the number.
        self.is_integer = False
        self.is_decimal = False
        self.is_exponential = False
        if 'e' in value or 'E' in value:
            self.is_exponential = True
        elif '.' in value:
            self.is_decimal = True
        else:
            self.is_integer = True

    def __str__(self):
        # FIXME: may break when followed by a specifier (`.`) symbol.
        return self.value


