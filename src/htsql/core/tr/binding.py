#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ..util import maybe, listof, tupleof, dictof, Clonable, Printable, Hashable
from ..entity import TableEntity, ColumnEntity, Join
from ..domain import (Domain, VoidDomain, BooleanDomain, ListDomain,
        RecordDomain, EntityDomain, IdentityDomain, Profile)
from ..error import point
from ..syn.syntax import Syntax, VoidSyntax, IdentifierSyntax, StringSyntax
from .signature import Signature, Bag, Formula


class Binding(Clonable, Printable):
    """
    A binding node.

    A binding graph is an intermediate representation of an HTSQL query.
    It is constructed from the syntax tree by the *binding* process and
    further translated to the space graph by the *encoding* process.

    A binding node represents an HTSQL expression or a naming scope (or both).
    Each binding node keeps a reference to the scope in which it was created;
    this scope chain forms a lookup context.

    `base`: :class:`Binding` or ``None``
        The scope in which the node was created; used for chaining lookup
        requests.

    `domain`: :class:`.Domain`
        The data type of the expression.

    `syntax`: :class:`.Syntax`
        The syntax node from which the binding node was generated; use
        only for presentation and error reporting.
    """

    def __init__(self, base, domain, syntax):
        #assert isinstance(base, maybe(Binding))
        #assert base is not None or isinstance(self, (RootBinding, VoidBinding))
        #assert isinstance(domain, Domain)
        #assert isinstance(syntax, Syntax)

        self.base = base
        self.domain = domain
        self.syntax = syntax
        # Inherit the error context from the syntax node.
        point(self, syntax)

    def __str__(self):
        return str(self.syntax)


class Recipe(Hashable):
    """
    A recipe object.

    A recipe is a generator of binding nodes.  Recipes are produced by lookup
    requests and used to construct the binding graph.
    """


class VoidBinding(Binding):
    """
    A dummy binding node.
    """

    def __init__(self):
        super(VoidBinding, self).__init__(None, VoidDomain(), VoidSyntax())


class ScopeBinding(Binding):
    """
    Represents a binding node that introduces a new naming scope.
    """


class HomeBinding(ScopeBinding):
    """
    The *home* scope.

    The home scope contains all database tables.
    """

    def __init__(self, base, syntax):
        super(HomeBinding, self).__init__(base, EntityDomain(), syntax)


class RootBinding(HomeBinding):
    """
    The root scope.

    The root scope is the origin of the binding graph.
    """

    def __init__(self, syntax):
        super(RootBinding, self).__init__(None, syntax)


class TableBinding(ScopeBinding):
    """
    A table scope.

    A table scope contains all table attributes and the links to other tables
    related via foreign key constraints.

    `table`: :class:`.TableEntity`
        The database table.
    """

    def __init__(self, base, table, syntax):
        assert isinstance(table, TableEntity)
        super(TableBinding, self).__init__(base, EntityDomain(), syntax)
        self.table = table


class ChainBinding(TableBinding):
    """
    An attached table scope.

    An attached table is produced by a link from another table via
    a chain of foreign key constraints.

    `joins`: [:class:`.Join`]
        Constraints attaching the table to its base.
    """

    def __init__(self, base, joins, syntax):
        assert isinstance(joins, listof(Join)) and len(joins) > 0
        super(ChainBinding, self).__init__(base, joins[-1].target, syntax)
        self.joins = joins


class ColumnBinding(ScopeBinding):
    """
    A table column scope.

    `column`: :class:`.ColumnEntity`
        The column.

    `link`: :class:`Binding` or ``None``
        If set, intercepts all lookup requests to the column scope.  Used
        when the same name represents both a column and a link to another
        table.
    """

    def __init__(self, base, column, link, syntax):
        assert isinstance(column, ColumnEntity)
        assert isinstance(link, maybe(Binding))
        super(ColumnBinding, self).__init__(base, column.domain, syntax)
        self.column = column
        self.link = link


class QuotientBinding(ScopeBinding):
    """
    A quotient scope.

    A quotient of the `seed` space by the given `kernels` is a space of
    all unique values of ``kernels`` as it ranges over ``seed``.

    `seed`: :class:`Binding`
        The seed of the quotient.

    `kernels`: [:class:`Binding`]
        The kernel expressions.
    """

    def __init__(self, base, seed, kernels, syntax):
        assert isinstance(seed, Binding)
        assert isinstance(kernels, listof(Binding))
        super(QuotientBinding, self).__init__(base, EntityDomain(), syntax)
        self.seed = seed
        self.kernels = kernels


class CoverBinding(ScopeBinding):
    """
    Represents a scope that borrows its content from another scope.

    `seed`: :class:`Binding`
        The wrapped scope.
    """

    def __init__(self, base, seed, syntax):
        assert isinstance(seed, Binding)
        super(ScopeBinding, self).__init__(base, seed.domain, syntax)
        self.seed = seed


class KernelBinding(CoverBinding):
    """
    A kernel expression in a quotient scope.

    `quotient`: :class:`QuotientBinding`
        The quotient scope.

    `index`: ``int``
        The position of the selected kernel expression.
    """

    def __init__(self, base, quotient, index, syntax):
        assert isinstance(quotient, QuotientBinding)
        assert isinstance(index, int)
        assert 0 <= index < len(quotient.kernels)
        seed = quotient.kernels[index]
        super(KernelBinding, self).__init__(base, seed, syntax)
        self.quotient = quotient
        self.index = index


class ComplementBinding(CoverBinding):
    """
    A complement link in a quotient scope.

    `quotient`: :class:`QuotientBinding`
        The quotient scope.
    """

    def __init__(self, base, quotient, syntax):
        assert isinstance(quotient, QuotientBinding)
        super(ComplementBinding, self).__init__(base, quotient.seed, syntax)
        self.quotient = quotient


class ForkBinding(CoverBinding):
    """
    A fork of the current scope.

    `kernels` [:class:`Binding`]
        The kernel expressions attaching the fork to its base.
    """

    def __init__(self, base, kernels, syntax):
        assert isinstance(kernels, listof(Binding))
        super(ForkBinding, self).__init__(base, base, syntax)
        self.kernels = kernels


class AttachBinding(CoverBinding):
    """
    An attachment expression.

    `images`: [(:class:`Binding`, :class:`Binding`)]
        Pairs of expressions attaching the binding to its base.

    `condition`: :class:`Binding`
        A condition attaching the binding to its base.
    """

    def __init__(self, base, seed, images, condition, syntax):
        assert isinstance(images, listof(tupleof(Binding, Binding)))
        assert isinstance(condition, maybe(Binding))
        if condition is not None:
            assert isinstance(condition.domain, BooleanDomain)
        super(AttachBinding, self).__init__(base, seed, syntax)
        self.images = images
        self.condition = condition


class LocateBinding(AttachBinding):
    """
    A locator expression.

    A locator is an attachment expression for which we know that
    it produces a singular value.
    """


class ClipBinding(CoverBinding):
    """
    A slice of a space.

    `order`: [(:class:`Binding`, ``+1`` or ``-1``)]
        Expressions to sort by.

    `limit`: ``int`` or ``None``
        If set, indicates to take the top ``limit`` rows.
        (``None`` means ``1``).

    `offset`: ``int`` or ``None``
        If set, indicates to drop the top ``offset`` rows.
        (``None`` means ``0``).
    """

    def __init__(self, base, seed, order, limit, offset, syntax):
        assert isinstance(seed, Binding)
        assert isinstance(order, listof(tupleof(Binding, int)))
        assert isinstance(limit, maybe(int))
        assert isinstance(offset, maybe(int))
        super(ClipBinding, self).__init__(base, seed, syntax)
        self.order = order
        self.limit = limit
        self.offset = offset


class ValueBinding(Binding):
    """
    A literal value.

    `data`
        The data value.
    """

    def __init__(self, base, data, domain, syntax):
        super(ValueBinding, self).__init__(base, domain, syntax)
        self.data = data


class WrapBinding(Binding):
    """
    Represents a binding node that augments a naming scope.
    """


class DecorateBinding(WrapBinding):
    """
    Represents a binding node ignored by the encoder.
    """

    def __init__(self, base, syntax):
        super(DecorateBinding, self).__init__(base, base.domain, syntax)


class DefineBinding(DecorateBinding):
    """
    Defines a calculated attribute.

    `name`: ``unicode``
        The name of the attribute.

    `arity`: ``int`` or ``None``
        The number of arguments for an parameterized attribute;
        ``None`` for an attribute without parameters.

    `recipe`: :class:`Recipe`
        The value generator.
    """

    def __init__(self, base, name, arity, recipe, syntax):
        assert isinstance(name, str)
        assert isinstance(arity, maybe(int))
        assert isinstance(recipe, Recipe)
        super(DefineBinding, self).__init__(base, syntax)
        self.name = name
        self.arity = arity
        self.recipe = recipe


class DefineReferenceBinding(DecorateBinding):
    """
    Defines a reference.

    `name`: ``unicode``
        The reference name.

    `recipe`: :class:`Recipe`
        The value generator.
    """

    def __init__(self, base, name, recipe, syntax):
        assert isinstance(name, str)
        assert isinstance(recipe, Recipe)
        super(DefineReferenceBinding, self).__init__(base, syntax)
        self.name = name
        self.recipe = recipe


class DefineCollectionBinding(DecorateBinding):
    """
    Defines a collection of attributes or references.
    """

    def __init__(self, base, collection, is_reference, syntax):
        assert isinstance(collection, dictof(str, Recipe))
        assert isinstance(is_reference, bool)
        super(DefineCollectionBinding, self).__init__(base, syntax)
        self.collection = collection
        self.is_reference = is_reference


class DefineLiftBinding(DecorateBinding):
    """
    Defines the value of the lift symbol.

    `recipe`: :class:`Recipe`
        The value generator.
    """

    def __init__(self, base, recipe, syntax):
        assert isinstance(recipe, Recipe)
        super(DefineLiftBinding, self).__init__(base, syntax)
        self.recipe = recipe


class CollectBinding(Binding):
    """
    Represents a segment of an HTSQL query.

    `seed` (:class:`Binding` or ``None``)
        The output space.  If not set explicitly, should be inferred from
        `elements`.

    `elements` (a list of :class:`Binding`)
        The output columns.
    """

    def __init__(self, base, seed, domain, syntax):
        assert isinstance(base, Binding)
        assert isinstance(seed, Binding)
        super(CollectBinding, self).__init__(base, domain, syntax)
        self.seed = seed


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

    A sort binding specifies the row order for the space generated by the
    `base` binding.  It may also apply a slice to the space.

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

    A selector specifies output columns of a space.

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
        self.width = domain.width


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
        assert isinstance(terms, listof(tupleof(str, bool)))
        assert len(terms) > 0
        assert isinstance(parameters, maybe(listof(tupleof(str, bool))))
        assert isinstance(body, Syntax)
        super(AssignmentBinding, self).__init__(base, VoidDomain(), syntax)
        self.terms = terms
        self.parameters = parameters
        self.body = body


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
        assert isinstance(title, (IdentifierSyntax, StringSyntax))
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
        assert isinstance(format, str)
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
        assert isinstance(terms, listof(tupleof(str, bool)))
        assert isinstance(parameters, maybe(listof(tupleof(str, bool))))
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
            chunks.append(name)
        if self.parameters is not None:
            chunks.append("(")
            for index, (name, is_reference) in enumerate(self.parameters):
                if index > 0:
                    chunks.append(",")
                if is_reference:
                    chunks.append("$")
                chunks.append(name)
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
        assert isinstance(alternatives, maybe(listof(str)))
        super(AmbiguousRecipe, self).__init__()
        self.alternatives = alternatives

    def __basis__(self):
        return (tuple(self.alternatives) if self.alternatives is not None
                else (),)


