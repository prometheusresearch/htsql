#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql.core.tr.binding`
============================

This module declares binding nodes and recipe objects.
"""


from ..util import maybe, listof, tupleof, Clonable, Printable, Comparable
from ..entity import TableEntity, ColumnEntity, Join
from ..domain import (Domain, VoidDomain, BooleanDomain, ListDomain,
                      RecordDomain, EntityDomain, IdentityDomain, Profile)
from .syntax import Syntax, VoidSyntax
from .signature import Signature, Bag, Formula
from ..cmd.command import Command


class Binding(Clonable, Printable):
    """
    Represents a binding node.

    This is an abstract class; see subclasses for concrete binding nodes.

    A binding graph is an intermediate phase of the HTSQL translator between
    the syntax tree and the flow graph.  It is converted from the syntax tree
    by the *binding* process and further translated to the flow graph by the
    *encoding* process.

    The structure of the binding graph reflects the form of naming *scopes*
    in the query; each binding node keeps a reference to the scope where
    it was instantiated.

    The constructor arguments:

    `base` (:class:`Binding` or ``None``)
        The scope in which the node is created.

        The value of ``None`` is only valid for an instance of
        :class:`RootBinding`, which represents the origin node in the graph.

    `domain` (:class:`htsql.core.domain.Domain`)
        The type of the binding node; use :class:`htsql.core.domain.VoidDomain`
        instance when not applicable.

    `syntax` (:class:`htsql.core.tr.syntax.Syntax`)
        The syntax node that generated the binding node; should be used
        for presentation or error reporting only, there is no guarantee
        that that the syntax node is semantically, or even syntaxically
        valid.

    Other attributes:

    `mark` (:class:`htsql.core.mark.Mark`)
        The location of the node in the original query (for error reporting).
    """

    def __init__(self, base, domain, syntax):
        assert isinstance(base, maybe(Binding))
        assert base is None or not isinstance(self, RootBinding)
        assert isinstance(domain, Domain)
        assert isinstance(syntax, Syntax)

        self.base = base
        self.domain = domain
        self.syntax = syntax
        self.mark = syntax.mark

    def __str__(self):
        # Display an HTSQL fragment that (approximately) corresponds
        # to the binding node.
        return str(self.syntax)


class Recipe(Comparable, Printable):
    """
    Represents a recipe object.

    A recipe is a generator of binding nodes.  Recipes are produced by lookup
    requests and used to construct the binding graph.
    """


class VoidBinding(Binding):

    def __init__(self):
        base = RootBinding(VoidSyntax())
        super(VoidBinding, self).__init__(base, VoidDomain(), VoidSyntax())


class QueryBinding(Binding):
    """
    Represents the whole HTSQL query.

    `segment` (:class:`SegmentBinding` or ``None``)
        The top segment.
    """

    def __init__(self, base, segment, profile, syntax):
        assert isinstance(base, RootBinding)
        assert isinstance(segment, maybe(SegmentBinding))
        assert isinstance(profile, Profile)
        super(QueryBinding, self).__init__(base, VoidDomain(), syntax)
        self.segment = segment
        self.profile = profile


class SegmentBinding(Binding):
    """
    Represents a segment of an HTSQL query.

    `seed` (:class:`Binding` or ``None``)
        The output flow.  If not set explicitly, should be inferred from
        `elements`.

    `elements` (a list of :class:`Binding`)
        The output columns.
    """

    def __init__(self, base, seed, domain, syntax):
        assert isinstance(base, Binding)
        assert isinstance(seed, Binding)
        super(SegmentBinding, self).__init__(base, domain, syntax)
        self.seed = seed


class CommandBinding(Binding):

    def __init__(self, base, command, syntax):
        assert isinstance(command, Command)
        super(CommandBinding, self).__init__(base, VoidDomain(), syntax)
        self.command = command


class ScopingBinding(Binding):
    """
    Represents a binding node that introduces a new naming scope.

    This is an abstract class; see subclasses for concrete node types.
    """


class ChainingBinding(Binding):
    """
    Represents a binding node that augments the parent naming scope.

    This is an abstract class; see subclasses for concrete node types.
    """


class WrappingBinding(ChainingBinding):
    """
    Represents a binding node ignored by the encoder.

    This class has subclasses for concrete node types, but could also
    be used directly to change a syntax node of the parent binding.
    """

    def __init__(self, base, syntax):
        super(WrappingBinding, self).__init__(base, base.domain, syntax)


class HomeBinding(ScopingBinding):
    """
    Represents the *home* naming scope.

    The home scope contains links to all tables in the database.
    """

    def __init__(self, base, syntax):
        super(HomeBinding, self).__init__(base, EntityDomain(), syntax)


class RootBinding(HomeBinding):
    """
    Represents the root scope.

    The root scope is the origin of the binding graph.
    """

    def __init__(self, syntax):
        super(RootBinding, self).__init__(None, syntax)


class TableBinding(ScopingBinding):
    """
    Represents a table scope.

    This is an abstract class; see :class:`FreeTableBinding` and
    :class:`AttachedTableBinding` for concrete subclasses.

    A table scope contains all attributes of the tables as well
    as the links to other tables related via foreign key constraints.

    `table` (:class:`htsql.core.entity.TableEntity`)
        The table with which the binding is associated.
    """

    def __init__(self, base, table, syntax):
        assert isinstance(table, TableEntity)
        super(TableBinding, self).__init__(base, EntityDomain(), syntax)
        self.table = table


class FreeTableBinding(TableBinding):
    """
    Represents a free table scope.

    A free table binding is generated by a link from the home class.
    """


class AttachedTableBinding(TableBinding):
    """
    Represents an attached table scope.

    An attached table binding is generated by a link from another table.

    `join` (:class:`htsql.core.entity.Join`)
        The join attaching the table to its base.
    """

    def __init__(self, base, join, syntax):
        assert isinstance(join, Join)
        super(AttachedTableBinding, self).__init__(base, join.target, syntax)
        self.join = join


class ColumnBinding(ScopingBinding):
    """
    Represents a table column scope.

    `column` (:class:`htsql.core.entity.ColumnEntity`)
        The column entity.

    `link` (:class:`Binding` or ``None``)
        If set, indicates that the binding also represents a link
        to another table.  Any lookup requests applied to the column
        binding are delegated to `link`.
    """

    def __init__(self, base, column, link, syntax):
        assert isinstance(column, ColumnEntity)
        assert isinstance(link, maybe(Binding))
        super(ColumnBinding, self).__init__(base, column.domain, syntax)
        self.column = column
        self.link = link


class QuotientBinding(ScopingBinding):
    """
    Represents a quotient scope.

    A quotient expression generates a flow of all unique values of
    the given kernel as it ranges over the `seed` flow.

    `seed` (:class:`Binding`)
        The seed of the quotient.

    `kernels` (a list of :class:`Binding`)
        The kernel expressions of the quotient.
    """

    def __init__(self, base, seed, kernels, syntax):
        assert isinstance(seed, Binding)
        assert isinstance(kernels, listof(Binding))
        super(QuotientBinding, self).__init__(base, EntityDomain(), syntax)
        self.seed = seed
        self.kernels = kernels


class KernelBinding(ScopingBinding):
    """
    Represents a kernel in a quotient scope.

    `quotient` (:class:`QuotientBinding`)
        The quotient binding (typically coincides with `base`).

    `index` (an integer)
        The position of the selected kernel expression.
    """

    def __init__(self, base, quotient, index, syntax):
        assert isinstance(quotient, QuotientBinding)
        assert isinstance(index, int)
        assert 0 <= index < len(quotient.kernels)
        domain = quotient.kernels[index].domain
        super(KernelBinding, self).__init__(base, domain, syntax)
        self.quotient = quotient
        self.index = index


class ComplementBinding(ScopingBinding):
    """
    Represents a complement link in a quotient scope.

    `quotient` (:class:`QuotientBinding`)
        The quotient binding (typically coincides with `base`)
    """

    def __init__(self, base, quotient, syntax):
        assert isinstance(quotient, QuotientBinding)
        domain = quotient.seed.domain
        super(ComplementBinding, self).__init__(base, domain, syntax)
        self.quotient = quotient


class CoverBinding(ScopingBinding):
    """
    Represents an opaque alias for a scope expression.

    `seed` (:class:`Binding`)
        The covered expression.
    """

    def __init__(self, base, seed, syntax):
        assert isinstance(seed, Binding)
        super(CoverBinding, self).__init__(base, seed.domain, syntax)
        self.seed = seed


class ForkBinding(ScopingBinding):
    """
    Represents a forking expression.

    `kernels` (a list of :class:`Binding`)
        The kernel expressions of the fork.
    """

    def __init__(self, base, kernels, syntax):
        assert isinstance(kernels, listof(Binding))
        super(ForkBinding, self).__init__(base, base.domain, syntax)
        self.kernels = kernels


class LinkBinding(ScopingBinding):
    """
    Represents a linking expression.

    `seed` (:class:`Binding`)
        The target of the link.

    `images` (a list of pairs of :class:`Binding`)
        Pairs of expressions connecting `seed` to `base`.
    """

    def __init__(self, base, seed, images, syntax):
        assert isinstance(seed, Binding)
        assert isinstance(images, listof(tupleof(Binding, Binding)))
        super(LinkBinding, self).__init__(base, EntityDomain(), syntax)
        self.seed = seed
        self.images = images


class ClipBinding(ScopingBinding):

    def __init__(self, base, seed, limit, offset, syntax):
        assert isinstance(seed, Binding)
        assert isinstance(limit, maybe(int))
        assert isinstance(offset, maybe(int))
        super(ClipBinding, self).__init__(base, seed.domain, syntax)
        self.seed = seed
        self.limit = limit
        self.offset = offset


class LocatorBinding(ScopingBinding):

    def __init__(self, base, seed, identity, value, syntax):
        assert isinstance(seed, Binding)
        assert isinstance(identity, IdentityBinding)
        assert (isinstance(value, tuple) and
                len(value) == len(identity.elements))
        super(LocatorBinding, self).__init__(base, seed.domain, syntax)
        self.seed = seed
        self.identity = identity
        self.value = value


class SieveBinding(ChainingBinding):
    """
    Represents a sieve expression.

    A sieve applies a filter to the base binding.

    `filter` (:class:`Binding`)
        A conditional expression that filters the base scope.
    """


    def __init__(self, base, filter, syntax):
        assert isinstance(filter, Binding)
        assert isinstance(filter.domain, BooleanDomain)
        super(SieveBinding, self).__init__(base, base.domain, syntax)
        self.filter = filter


class SortBinding(ChainingBinding):
    """
    Represents a sorting expression.

    A sort binding specifies the row order for the flow generated by the
    `base` binding.  It may also apply a slice to the flow.

    `order` (a list of :class:`Binding`)
        The expressions by which the base rows are sorted.

    `limit` (an integer or ``None``)
        If set, indicates that only the first `limit` rows are produced
        (``None`` means no limit).

    `offset` (an integer or ``None``)
        If set, indicates that only the rows starting from `offset`
        are produced (``None`` means ``0``).
    """

    def __init__(self, base, order, limit, offset, syntax):
        assert isinstance(order, listof(Binding))
        assert isinstance(limit, maybe(int))
        assert isinstance(offset, maybe(int))
        super(SortBinding, self).__init__(base, base.domain, syntax)
        self.order = order
        self.limit = limit
        self.offset = offset


class RescopingBinding(ChainingBinding):
    """
    Represents a rescoping operation.

    `scope` (:class:`Binding`)
        The target scope.
    """

    def __init__(self, base, scope, syntax):
        assert isinstance(scope, Binding)
        super(RescopingBinding, self).__init__(base, base.domain, syntax)
        self.scope = scope


class SelectionBinding(ChainingBinding):
    """
    Represents a selector expression (``{...}`` operator).

    A selector specifies output columns of a flow.

    `elements` (a list of :class:`Binding`)
        The output columns.
    """

    def __init__(self, base, elements, domain, syntax):
        assert isinstance(elements, listof(Binding))
        super(SelectionBinding, self).__init__(base, domain, syntax)
        self.elements = elements


class WildSelectionBinding(SelectionBinding):
    """
    Represents a selector generated by a wildcard (``*``).
    """


class IdentityBinding(Binding):

    def __init__(self, base, elements, syntax):
        assert isinstance(elements, listof(Binding))
        domain = IdentityDomain([element.domain for element in elements])
        super(IdentityBinding, self).__init__(base, domain, syntax)
        self.elements = elements


class AssignmentBinding(Binding):
    """
    Represents an assignment expression.

    `terms` (a list of pairs `(Unicode string, Boolean)`)
        The terms of the assignment.

        Each term is represented by a pair of the term name and a flag
        indicating whether the name is a reference or not.

    `parameters` (a list of pairs `(Unicode string, Boolean)` or ``None``)
        The parameters; if not set, indicates the defined attribute
        does not accept any parameters.

        Each parameter is represented by a pair of the parameter name
        and a flag indicating whether the name is a reference.

    `body` (:class:`htsql.core.tr.syntax.Syntax`)
        The body of the assignment.
    """

    def __init__(self, base, terms, parameters, body, syntax):
        assert isinstance(terms, listof(tupleof(unicode, bool)))
        assert len(terms) > 0
        assert isinstance(parameters, maybe(listof(tupleof(unicode, bool))))
        assert isinstance(body, Syntax)
        super(AssignmentBinding, self).__init__(base, VoidDomain(), syntax)
        self.terms = terms
        self.parameters = parameters
        self.body = body


class DefinitionBinding(WrappingBinding):
    """
    Represents a definition of a calculated attribute or a reference.

    `name` (a Unicode string)
        The name of the attribute.

    `is_reference` (Boolean)
        If set, indicates a definition of a reference.

    `arity` (an integer or ``None``)
        The number of arguments for an parameterized attribute;
        ``None`` for an attribute without parameters.

    `recipe` (:class:`Recipe`)
        The value of the attribute.
    """

    def __init__(self, base, name, is_reference, arity, recipe, syntax):
        assert isinstance(name, unicode)
        assert isinstance(is_reference, bool)
        assert isinstance(arity, maybe(int))
        # A reference cannot have parameters.
        assert arity is None or not is_reference
        assert isinstance(recipe, Recipe)
        super(DefinitionBinding, self).__init__(base, syntax)
        self.name = name
        self.is_reference = is_reference
        self.arity = arity
        self.recipe = recipe


class DirectionBinding(WrappingBinding):
    """
    Represents a direction decorator (postfix ``+`` and ``-`` operators).

    `direction` (``+1`` or ``-1``).
        Indicates the direction; ``+1`` for ascending, ``-1`` for descending.
    """

    def __init__(self, base, direction, syntax):
        assert direction in [+1, -1]
        super(DirectionBinding, self).__init__(base, syntax)
        self.direction = direction


class RerouteBinding(WrappingBinding):
    """
    Represents a rerouting binding node.

    A rerouting node redirects all lookup requests to a designated target.

    `target` (:class:`Binding`)
        The route destination.
    """

    def __init__(self, base, target, syntax):
        assert isinstance(target, Binding)
        super(RerouteBinding, self).__init__(base, syntax)
        self.target = target


class ReferenceRerouteBinding(WrappingBinding):
    """
    Represents a reference rerouting node.

    A reference rerouting node redirects reference lookup requests to a
    designated target.

    `target` (:class:`Binding`)
        The route destination.
    """

    def __init__(self, base, target, syntax):
        assert isinstance(target, Binding)
        super(ReferenceRerouteBinding, self).__init__(base, syntax)
        self.target = target


class TitleBinding(WrappingBinding):
    """
    Represents a title decorator (the ``as`` operator).

    The title decorator is used to specify the column title explicitly
    (by default, a serialized syntax node is used as the title).

    `title` (a Unicode string)
        The title.
    """

    def __init__(self, base, title, syntax):
        assert isinstance(title, unicode)
        super(TitleBinding, self).__init__(base, syntax)
        self.title = title


class AliasBinding(WrappingBinding):
    """
    Represents a syntax decorator.

    The syntax decorator changes the syntax node associated with the base
    binding node.
    """

    def __init__(self, base, syntax):
        super(AliasBinding, self).__init__(base, syntax)


class FormatBinding(WrappingBinding):
    """
    Represents a format decorator (the ``format`` operator).

    The format decorator is used to provide hints to the renderer
    as to how display column values.  How the format is interpreted
    by the renderer depends on the renderer and the type of the column.

    `format` (a Unicode string)
        The formatting hint.
    """

    # FIXME: currently unused.

    def __init__(self, base, format, syntax):
        assert isinstance(format, unicode)
        super(FormatBinding, self).__init__(base, syntax)
        self.format = format


class LiteralBinding(Binding):
    """
    Represents a literal value.

    `value` (valid type depends on the domain)
        The value.

    `domain` (:class:`htsql.core.domain.Domain`)
        The value type.
    """

    def __init__(self, base, value, domain, syntax):
        super(LiteralBinding, self).__init__(base, domain, syntax)
        self.value = value


class CastBinding(Binding):
    """
    Represents a type conversion operation.

    `domain` (:class:`htsql.core.domain.Domain`)
        The target domain.
    """

    def __init__(self, base, domain, syntax):
        super(CastBinding, self).__init__(base, domain, syntax)


class ImplicitCastBinding(CastBinding):
    pass


class FormulaBinding(Formula, Binding):
    """
    Represents a formula binding.

    A formula binding represents a function or an operator call as
    as a binding node.

    `signature` (:class:`htsql.core.tr.signature.Signature`)
        The signature of the formula.

    `domain` (:class:`Domain`)
        The co-domain of the formula.

    `arguments` (a dictionary)
        The arguments of the formula.

        Note that all the arguments become attributes of the node object.
    """

    def __init__(self, base, signature, domain, syntax, **arguments):
        assert isinstance(signature, Signature)
        # Check that the arguments match the formula signature.
        arguments = Bag(**arguments)
        assert arguments.admits(Binding, signature)
        # This will impress the arguments to the node.
        super(FormulaBinding, self).__init__(signature, arguments,
                                             base, domain, syntax)


class LiteralRecipe(Recipe):

    def __init__(self, value, domain):
        assert isinstance(domain, Domain)
        self.value = value
        self.domain = domain

    def __basis__(self):
        return (self.value, self.domain)

    def __str__(self):
        return "%s: %s" % (self.value, self.domain)


class SelectionRecipe(Recipe):

    def __init__(self, recipes):
        assert isinstance(recipes, listof(Recipe))
        self.recipes = recipes

    def __basis__(self):
        return (tuple(self.recipes),)

    def __str__(self):
        return "{%s}" % ",".join(str(recipe) for recipe in self.recipes)


class FreeTableRecipe(Recipe):
    """
    Generates a :class:`FreeTableBinding` node.

    `table` (:class:`htsql.core.entity.TableEntity`)
        The table associated with the binding.
    """

    def __init__(self, table):
        assert isinstance(table, TableEntity)
        self.table = table

    def __basis__(self):
        return (self.table,)

    def __str__(self):
        return str(self.table)


class AttachedTableRecipe(Recipe):
    """
    Generates a chain of :class:`AttachedTableBinding` nodes.

    `joins` (a list of :class:`htsql.core.entity.Join`)
        The joins to attach the nodes.

    `origin_table`
        table entity at the head of this link

    `target_table`
        table entity at the tail of this link

    `is_singular` 
        boolean value if this link is singular

    `is_direct``
        this is single join direct link created by a foreign key
 
    `is_reverse`
        this is a single join created by reversal of a foreign key 
    """

    def __init__(self, joins):
        assert isinstance(joins, listof(Join)) and len(joins) > 0
        self.joins = joins
        self.origin_table = joins[0].target
        self.target_table = joins[-1].target
        self.is_singular = all(join.is_contracting for join in joins)
        self.is_direct  = len(joins) == 1 and joins[0].is_direct
        self.is_reverse = len(joins) == 1 and joins[0].is_reverse

    def __basis__(self):
        return (tuple(self.joins),)

    def __str__(self):
        return " => ".join(str(join) for join in self.joins)


class ColumnRecipe(Recipe):
    """
    Generates a :class:`ColumnBinding` node.

    `column` (:class:`htsql.core.entity.ColumnEntity`)
        The column entity.

    `link` (:class:`Recipe` or ``None``)
        If set, indicates that the column also represents a link
        to another binding node.
    """

    def __init__(self, column, link=None):
        assert isinstance(column, ColumnEntity)
        assert isinstance(link, maybe(Recipe))
        self.column = column
        self.link = link

    def __basis__(self):
        return (self.column,)

    def __str__(self):
        return str(self.column)


class KernelRecipe(Recipe):
    """
    Generates a :class:`KernelBinding` node.

    `quotient` (:class:`QuotientBinding`)
        The quotient binding.

    `index` (an integer)
        The position of the selected kernel expression.
    """

    def __init__(self, quotient, index):
        assert isinstance(quotient, QuotientBinding)
        assert isinstance(index, int)
        assert 0 <= index < len(quotient.kernels)
        self.quotient = quotient
        self.index = index

    def __basis__(self):
        return (self.quotient, self.index)

    def __str__(self):
        return "%s.*%s" % (self.quotient, self.index+1)


class IdentityRecipe(Recipe):

    def __init__(self, elements):
        assert isinstance(elements, listof(Recipe))
        self.elements = elements

    def __basis__(self):
        return (self.elements,)


class ComplementRecipe(Recipe):
    """
    Generates a :class:`ComplementBinding` node.

    `quotient` (:class:`QuotientBinding`)
        The quotient binding.
    """

    def __init__(self, quotient):
        assert isinstance(quotient, QuotientBinding)
        self.quotient = quotient

    def __basis__(self):
        return (self.quotient,)

    def __str__(self):
        return "%s.^" % self.quotient


class SubstitutionRecipe(Recipe):
    """
    Evaluates a calculated attribute or a reference.

    `base` (:class:`Binding`)
        The scope in which the calculation is defined.

    `terms` (a list of pairs `(Unicode string, Boolean)`)
        The tail of a qualified definition.  Each term is represented by a pair
        of the term name and a flag indicating whether the term is a reference
        or not.

    `parameters` (a list of pairs `(Unicode string, Boolean)` or ``None``)
        The parameters of the calculation.  Each parameter is a pair of the
        parameter name and a flag indicating whether the parameter is a
        reference.

    `body` (:class:`htsql.core.tr.syntax.Syntax`)
        The body of the calculation.
    """

    def __init__(self, base, terms, parameters, body):
        assert isinstance(base, Binding)
        assert isinstance(terms, listof(tupleof(unicode, bool)))
        assert isinstance(parameters, maybe(listof(tupleof(unicode, bool))))
        assert isinstance(body, Syntax)
        self.base = base
        self.terms = terms
        self.parameters = parameters
        self.body = body

    def __basis__(self):
        return (self.base, tuple(self.terms), self.body,
                None if self.parameters is None else tuple(self.parameters))

    def __str__(self):
        # Display:
        #   <term>....(<parameter>,...) := <body>
        chunks = []
        for index, (name, is_reference) in enumerate(self.terms):
            if index > 0:
                chunks.append(".")
            if is_reference:
                chunks.append("$")
            chunks.append(name.encode('utf-8'))
        if self.parameters is not None:
            chunks.append("(")
            for index, (name, is_reference) in enumerate(self.parameters):
                if index > 0:
                    chunks.append(",")
                if is_reference:
                    chunks.append("$")
                chunks.append(name.encode('utf-8'))
            chunks.append(")")
        if chunks:
            chunks.append(" := ")
        chunks.append(str(self.body))
        return "".join(chunks)


class BindingRecipe(Recipe):
    """
    Generates the given node.

    `binding` (:class:`Binding`)
        The node to generate.
    """

    def __init__(self, binding):
        assert isinstance(binding, Binding)
        self.binding = binding

    def __basis__(self):
        return (self.binding,)

    def __str__(self):
        return str(self.binding)


class ClosedRecipe(Recipe):
    """
    Hides the syntax node of the generated node.
    """

    def __init__(self, recipe):
        assert isinstance(recipe, Recipe)
        self.recipe = recipe

    def __basis__(self):
        return (self.recipe,)

    def __str__(self):
        return "(%s)" % self.recipe


class PinnedRecipe(Recipe):
    """
    Evaluates a recipe in the given scope.

    `base` (:class:`Binding`)
        The scope to apply the recipe to.

    `recipe` (:class:`Recipe`)
        The recipe to apply.
    """

    def __init__(self, scope, recipe):
        assert isinstance(scope, Binding)
        assert isinstance(recipe, Recipe)
        self.scope = scope
        self.recipe = recipe

    def __basis__(self):
        return (self.scope, self.recipe)

    def __str__(self):
        return "%s -> %s" % (self.scope, self.recipe)


class ChainRecipe(Recipe):

    def __init__(self, recipes):
        assert isinstance(recipes, listof(Recipe))
        self.recipes = recipes

    def __basis__(self):
        return (tuple(self.recipes),)


class InvalidRecipe(Recipe):
    """
    Generates an error when applied.
    """

    def __basis__(self):
        return ()

    def __str__(self):
        return "!"


class AmbiguousRecipe(InvalidRecipe):
    """
    Generates an "ambiguous name" error when applied.
    """

    def __init__(self, alternatives=None):
        assert isinstance(alternatives, maybe(listof(unicode)))
        super(AmbiguousRecipe, self).__init__()
        self.alternatives = alternatives

    def __basis__(self):
        return (tuple(self.alternatives) if self.alternatives is not None
                else (),)


