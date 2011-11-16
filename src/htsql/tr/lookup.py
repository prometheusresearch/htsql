#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql.tr.lookup`
======================

This module implements name resolution adapters.
"""


from ..util import Clonable, Printable, maybe
from ..adapter import Adapter, adapts, adapts_many
from ..model import (HomeNode, TableNode, Arc, TableArc, ChainArc, ColumnArc,
                     SyntaxArc, AmbiguousArc)
from ..classify import classify, normalize
from .syntax import IdentifierSyntax
from .binding import (Binding, ScopingBinding, ChainingBinding, WrappingBinding,
                      SegmentBinding, HomeBinding, RootBinding, TableBinding,
                      ColumnBinding, QuotientBinding, ComplementBinding,
                      CoverBinding, ForkBinding, LinkBinding, RescopingBinding,
                      DefinitionBinding, SelectionBinding, DirectionBinding,
                      RerouteBinding, ReferenceRerouteBinding,
                      TitleBinding, AliasBinding, CommandBinding,
                      FreeTableRecipe, AttachedTableRecipe, ColumnRecipe,
                      ComplementRecipe, KernelRecipe, BindingRecipe,
                      SubstitutionRecipe, ClosedRecipe, InvalidRecipe,
                      AmbiguousRecipe)


class Probe(Clonable, Printable):
    """
    Represents a lookup request.
    """

    def __init__(self):
        # Need a constructor to satisfy `Clonable` interface.
        pass

    def __str__(self):
        return "?"


class AttributeProbe(Probe):
    """
    Represents a request for an attribute.

    The result of this probe is a :class:`htsql.tr.binding.Recipe`
    instance.

    `name` (a Unicode string)
        The attribute name.

    `arity` (an integer or ``None``)
        The number of parameters.

    Other attributes:

    `key` (a string)
        The normal form of the name.
    """

    def __init__(self, name, arity):
        assert isinstance(name, unicode)
        assert isinstance(arity, maybe(int))
        assert arity is None or arity >= 0
        self.name = name
        self.arity = arity
        self.key = normalize(name)

    def __str__(self):
        # Display:
        #   ?<key>
        # or:
        #   ?<key>(_,...)
        if self.arity is None:
            return "?%s" % self.key.encode('utf-8')
        else:
            return "?%s(%s)" % (self.key.encode('utf-8'),
                                ",".join(["_"]*self.arity))


class ReferenceProbe(Probe):
    """
    Represents a request for a reference.

    The result of this probe is a :class:`htsql.tr.binding.Recipe`
    instance.

    `name` (a Unicode string)
        The reference name.

    Other attributes:

    `key` (a string)
        The normal form of the name.
    """

    def __init__(self, name):
        assert isinstance(name, unicode)
        self.name = name
        self.key = normalize(name)

    def __str__(self):
        # Display:
        #   ?$<key>
        return "?$%s" % self.key.encode('utf-8')


class ComplementProbe(Probe):
    """
    Represents a request for a complement link.

    The result of this probe is a :class:`htsql.tr.binding.Recipe`
    instance.
    """

    def __str__(self):
        return "?<^>"


class ExpansionProbe(Probe):
    """
    Represents expansion requests.

    The result of this probe is a list of pairs `(recipe, syntax)`,
    where `recipe` is an instance of :class:`htsql.tr.binding.Recipe` and
    `syntax` is an instance of :class:`htsql.tr.syntax.Syntax`.

    `is_soft` (Boolean)
        If set, expand selections.

    `is_hard` (Boolean)
        If set, expand classes.
    """

    def __init__(self, is_soft=True, is_hard=True):
        self.is_soft = is_soft
        self.is_hard = is_hard

    def __str__(self):
        # Display:
        #   ?<*|**>
        symbols = []
        if self.is_soft:
            symbols.append("*")
        if self.is_hard:
            symbols.append("**")
        return "?<%s>" % "|".join(symbols)


class GuessNameProbe(Probe):
    """
    Represents a request for an attribute name.

    The result of the probe is a string value -- an appropriate
    name of a binding.
    """

    def __str__(self):
        return "?<:=>"


class GuessTitleProbe(Probe):
    """
    Represents a request for a title.

    The result of this probe is a list of headings.
    """

    def __str__(self):
        return "?<:as>"


class DirectionProbe(Probe):
    """
    Represents a request for a direction modifier.

    The result of this probe is ``+1`` or ``-1`` --- the direction
    indicator.
    """

    def __str__(self):
        return "?<+|->"


class CommandProbe(Probe):
    pass


class Prescribe(Adapter):

    adapts(Arc)

    def __init__(self, arc, binding):
        assert isinstance(arc, Arc)
        assert isinstance(binding, Binding)
        self.arc = arc
        self.binding = binding

    def __call__(self):
        return InvalidRecipe()


class PrescribeTable(Prescribe):

    adapts(TableArc)

    def __call__(self):
        return FreeTableRecipe(self.arc.table)


class PrescribeColumn(Prescribe):

    adapts(ColumnArc)

    def __call__(self):
        link = (prescribe(self.arc.link, self.binding)
                if self.arc.link is not None else None)
        return ColumnRecipe(self.arc.column, link)


class PrescribeChain(Prescribe):

    adapts(ChainArc)

    def __call__(self):
        return AttachedTableRecipe(self.arc.joins)


class PrescribeSyntax(Prescribe):

    adapts(SyntaxArc)

    def __call__(self):
        recipe = SubstitutionRecipe(self.binding, [], None, self.arc.syntax)
        return ClosedRecipe(recipe)


class PrescribeAmbiguous(Prescribe):

    adapts(AmbiguousArc)

    def __call__(self):
        return AmbiguousRecipe()


class Lookup(Adapter):
    """
    Extracts information from a binding node.

    This is an interface adapter, see subclasses for implementations.

    The :class:`Lookup` adapter has the following signature::

        Lookup: (Binding, Probe) -> ...

    The adapter is polymorphic on both arguments.  The type of the output
    value depends on the type of the `Probe` argument.  ``None`` is returned
    when the request cannot be satisfied.

    `binding` (:class:`htsql.tr.binding.Binding`)
        The binding node.

    `probe` (:class:`Probe`)
        The lookup request.
    """

    adapts(Binding, Probe)

    def __init__(self, binding, probe):
        assert isinstance(binding, Binding)
        assert isinstance(probe, Probe)
        self.binding = binding
        self.probe = probe

    def __call__(self):
        # `None` means the lookup request failed.
        return None


class GuessName(Lookup):
    # Generate an attribute name from a binging node.

    adapts(Binding, GuessNameProbe)

    def __call__(self):
        # If the binding was induced by an identifier node,
        # use the identifier name.
        if isinstance(self.binding.syntax, IdentifierSyntax):
            return self.binding.syntax.value
        # Otherwise, fail to produce a name.
        return None


class GuessTitle(Lookup):
    # Generate a sequence of headings for a binding node.

    adapts(Binding, GuessTitleProbe)

    def __call__(self):
        # Generate a header from the associated syntax node.
        return [unicode(self.binding.syntax)]


class LookupReferenceInScoping(Lookup):
    # Find a reference in a scoping node.

    adapts(ScopingBinding, ReferenceProbe)

    def __call__(self):
        # Delegate reference lookups to the parent binding.
        if self.binding.base is not None:
            return lookup(self.binding.base, self.probe)
        # Stop at the root binding.
        return None


class LookupInChaining(Lookup):
    # Pass all lookup requests (except name guesses)
    # through chaining nodes.

    # Everything but `GuessNameProbe`.  Can't use `Probe`
    # since it causes ambiguous ordering of components.
    adapts_many((ChainingBinding, AttributeProbe),
                (ChainingBinding, ReferenceProbe),
                (ChainingBinding, ComplementProbe),
                (ChainingBinding, ExpansionProbe),
                (ChainingBinding, GuessTitleProbe),
                (ChainingBinding, DirectionProbe))

    def __call__(self):
        # Delegate all lookup requests to the parent binding.
        return lookup(self.binding.base, self.probe)


class GuessNameFromChaining(Lookup):
    # Generate an attribute name from a chaining binding.

    adapts(ChainingBinding, GuessNameProbe)

    def __call__(self):
        # If the binding was induced by an identifier node,
        # use the identifier name.
        if isinstance(self.binding.syntax, IdentifierSyntax):
            return self.binding.syntax.value
        # Otherwise, pass the probe to the parent binding.
        return lookup(self.binding.base, self.probe)


class LookupCommandInWrapping(Lookup):

    adapts(WrappingBinding, CommandProbe)

    def __call__(self):
        return lookup(self.binding.base, self.probe)


class GuessTitleFromSegment(Lookup):
    # Generate a heading from a segment binding.

    adapts(SegmentBinding, GuessTitleProbe)

    def __call__(self):
        # If available, use the heading of the segment seed.
        if self.binding.seed is not None:
            return lookup(self.binding.seed, self.probe)
        # Otherwise, produce an empty heading.
        return []


class LookupCommandInCommand(Lookup):

    adapts(CommandBinding, CommandProbe)

    def __call__(self):
        return self.binding.command


class LookupAttributeInHome(Lookup):
    # Attributes of the *home* scope represent database tables.

    adapts(HomeBinding, AttributeProbe)

    def __call__(self):
        # Ignore probes for parameterized attributes.
        if self.probe.arity is not None:
            return None
        labels = classify(HomeNode())
        label_by_name = dict((label.name, label) for label in labels)
        if self.probe.key not in label_by_name:
            return None
        label = label_by_name[self.probe.key]
        recipe = prescribe(label.arc, self.binding)
        return recipe


class ExpandHome(Lookup):
    # The home class contains no public attributes.

    adapts(HomeBinding, ExpansionProbe)

    def __call__(self):
        # Expand the home class: there should be no public attributes, but try
        # it anyway.
        if self.probe.is_hard:
            labels = classify(HomeNode())
            recipes = []
            for label in labels:
                if not label.is_public:
                    continue
                identifier = IdentifierSyntax(label.name, self.binding.mark)
                recipe = prescribe(label.arc, self.binding)
                recipes.append((identifier, recipe))
            return recipes
        # Otherwise, fail to expand.
        return None


class GuessTitleFromHome(Lookup):
    # Generate a title for a home scope.

    adapts(HomeBinding, GuessTitleProbe)

    def __call__(self):
        # Produce no headers.
        return []


class LookupAttributeInTable(Lookup):
    # A table context contains three types of members:
    # - table columns;
    # - referenced tables, i.e., tables for which there exists a foreign
    #   key from the scope table;
    # - referencing tables, i.e., tables with a foreign key to the
    #   context table.

    adapts(TableBinding, AttributeProbe)

    def __call__(self):
        # Ignore probes for parameterized attributes.
        if self.probe.arity is not None:
            return None
        labels = classify(TableNode(self.binding.table))
        label_by_name = dict((label.name, label) for label in labels)
        if self.probe.key not in label_by_name:
            return None
        label = label_by_name[self.probe.key]
        recipe = prescribe(label.arc, self.binding)
        return recipe


class ExpandTable(Lookup):
    # Extract all the columns of the table.

    adapts(TableBinding, ExpansionProbe)

    def __call__(self):
        # Only expand on a class probe.
        if not self.probe.is_hard:
            return super(ExpandTable, self).__call__()
        return self.itemize_columns()

    def itemize_columns(self):
        labels = classify(TableNode(self.binding.table))
        for label in labels:
            if not label.is_public:
                continue
            # Create a "virtual" syntax node for each column
            identifier = IdentifierSyntax(label.name, self.binding.mark)
            recipe = prescribe(label.arc, self.binding)
            yield (identifier, recipe)


class LookupAttributeInColumn(Lookup):
    # Find a member with the given name in a column scope.
    #
    # All lookup requests are redirected to the link associated
    # with the column; if there is no associated link, the request
    # fails.

    adapts(ColumnBinding, AttributeProbe)

    def __call__(self):
        # Ignore probes for parameterized attributes.
        if self.probe.arity is not None:
            return None
        # If there is an associated link node, delegate the request to it.
        if self.binding.link is not None:
            return lookup(self.binding.link, self.probe)
        # Otherwise, no luck.
        return None


class ExpandColumn(Lookup):
    # Produce all public members in a column scope.
    #
    # The request is delegated to the link associated with the
    # column; if there is no associated link, the request fails.

    adapts(ColumnBinding, ExpansionProbe)

    def __call__(self):
        # If there is an associated link node, delegate the request to it.
        if self.binding.link is not None:
            return lookup(self.binding.link, self.probe)
        # Otherwise, no luck.
        return None


class LookupComplementInQuotient(Lookup):
    # Extract a complement from a quotient scope.

    adapts(QuotientBinding, ComplementProbe)

    def __call__(self):
        return ComplementRecipe(self.binding)


class ExpandQuotient(Lookup):
    # Extract public "columns" from a quotient scope.

    adapts(QuotientBinding, ExpansionProbe)

    def __call__(self):
        # Ignore non-class expansion requests.
        if not self.probe.is_hard:
            return super(ExpandQuotient, self).__call__()
        # Expand the kernel.
        return self.itemize_kernels()

    def itemize_kernels(self):
        # Produce a recipe for each expression in the kernel.
        for index, binding in enumerate(self.binding.kernels):
            syntax = binding.syntax
            recipe = KernelRecipe(self.binding, index)
            yield (syntax, recipe)


class LookupAttributeInComplement(Lookup):
    # Find an attribute or a complement link in a complement scope.

    adapts_many((ComplementBinding, AttributeProbe),
                (ComplementBinding, ComplementProbe))

    def __call__(self):
        # Delegate all lookup probes to the seed scope.
        return lookup(self.binding.quotient.seed, self.probe)


class ExpandComplement(Lookup):
    # Extract public columns from a complement scope.

    adapts(ComplementBinding, ExpansionProbe)

    def __call__(self):
        # Ignore selection expand probes.
        if not self.probe.is_hard:
            return super(ExpandComplement, self).__call__()
        # Delegate class expansion probe to the seed flow;
        # turn off selection expansion to avoid expanding the
        # selector in `distinct(table{kernel})`.
        probe = self.probe.clone(is_soft=False)
        return lookup(self.binding.quotient.seed, probe)


class LookupAttributeInCover(Lookup):
    # Find an attribute in a cover scope.

    adapts(CoverBinding, AttributeProbe)

    def __call__(self):
        # Delegate all lookup requests to the seed flow.
        return lookup(self.binding.seed, self.probe)


class ExpandCover(Lookup):
    # Expand public columns from a cover scope.

    adapts(CoverBinding, ExpansionProbe)

    def __call__(self):
        # Ignore pure selector expansion probes.
        if not self.probe.is_hard:
            return super(ExpandCover, self).__call__()
        # FIXME: selector expansion does not work between scopes.
        ## Turn on selector expansion.
        #probe = self.probe.clone(is_soft=True)
        # Delegate the probe to the seed class;
        #return lookup(self.binding.seed, probe)
        probe = self.probe.clone(is_soft=False)
        return lookup(self.binding.seed, probe)


class LookupAttributeInFork(Lookup):
    # Find an attribute or a complement link in a fork scope.

    adapts_many((ForkBinding, AttributeProbe),
                (ForkBinding, ComplementProbe))

    def __call__(self):
        # Delegate all lookup probes to the parent binding.
        return lookup(self.binding.base, self.probe)


class ExpandFork(Lookup):
    # Extract public columns from a fork scope.

    adapts(ForkBinding, ExpansionProbe)

    def __call__(self):
        # Delegate the expansion probe to the parent binding.
        # FIXME: selector expansion does not work between scopes.
        #return lookup(self.binding.base, self.probe)
        probe = self.probe.clone(is_soft=False)
        return lookup(self.binding.base, probe)


class LookupAttributeInLink(Lookup):
    # Find an attribute in a link scope.

    adapts(LinkBinding, AttributeProbe)

    def __call__(self):
        # Delegate all lookup probes to the seed scope.
        return lookup(self.binding.seed, self.probe)


class ExpandLink(Lookup):
    # Expand public columns in a link scope.

    adapts(LinkBinding, ExpansionProbe)

    def __call__(self):
        # Ignore selection expand probes.
        if not self.probe.is_hard:
            return super(ExpandLink, self).__call__()
        # Delegate class expansion probe to the seed flow;
        # turn off selection expansion to avoid expanding the
        # selector in `image -> table{image}`.
        probe = self.probe.clone(is_soft=False)
        return lookup(self.binding.seed, probe)


class GuessTitleFromLink(Lookup):
    # Extract a heading from a link scope.

    adapts(LinkBinding, GuessTitleProbe)

    def __call__(self):
        # Generate a heading from the link target.
        return lookup(self.binding.seed, self.probe)


class GuessTitleFromRescoping(Lookup):
    # Generate a title for a rescoping binding.

    adapts(RescopingBinding, GuessTitleProbe)

    def __call__(self):
        # For a fragment:
        #   scope{expression}
        # generate a combined heading from `scope` and `expression`.
        child_titles = lookup(self.binding.base, self.probe)
        parent_titles = lookup(self.binding.scope, self.probe)
        # Collapse repeating headers.
        if parent_titles and child_titles:
            if parent_titles[-1] == child_titles[0]:
                parent_titles = parent_titles[:-1]
        return parent_titles + child_titles


class DirectRescoping(Lookup):
    # Extract a direction decorator from a rescoping binding.

    adapts(RescopingBinding, DirectionProbe)

    def __call__(self):
        # Extract directions from both parts of the fragment:
        #   scope{expression}
        base_direction = lookup(self.binding.base, self.probe)
        scope_direction = lookup(self.binding.scope, self.probe)
        # Combine the scope and the expression directions.
        if scope_direction is None:
            return base_direction
        if base_direction is None:
            return scope_direction
        return base_direction * scope_direction


class LookupAttributeInDefinition(Lookup):
    # Find an attribute in a definition binding.

    adapts(DefinitionBinding, AttributeProbe)

    def __call__(self):
        # Check if the definition matches the probe.
        if not self.binding.is_reference:
            if self.binding.arity == self.probe.arity:
                binding_key = normalize(self.binding.name)
                if binding_key == self.probe.key:
                    # If it matches, produce the associated recipe.
                    return self.binding.recipe
        # Otherwise, delegate the probe to the parent binding.
        return super(LookupAttributeInDefinition, self).__call__()


class LookupReferenceInDefinition(Lookup):
    # Find a reference in a definition binding.

    adapts(DefinitionBinding, ReferenceProbe)

    def __call__(self):
        # Check if the definition matches the probe.
        if self.binding.is_reference:
            binding_key = normalize(self.binding.name)
            if binding_key == self.probe.key:
                # If it matches, produce the associated recipe.
                return self.binding.recipe
        # Otherwise, delegate the probe to the parent binding.
        return super(LookupReferenceInDefinition, self).__call__()


class ExpandSelection(Lookup):
    # Expand a selector operation.

    adapts(SelectionBinding, ExpansionProbe)

    def __call__(self):
        # Skip class expansion probes.
        if not self.probe.is_soft:
            return super(ExpandSelection, self).__call__()
        # Emit elements of the selector.
        return self.itemize()

    def itemize(self):
        # Emit elements of the selector.
        for element in self.binding.elements:
            syntax = element.syntax
            recipe = BindingRecipe(element)
            yield (syntax, recipe)


class DirectDirection(Lookup):
    # Extract a direction indicator from a direction binding.

    adapts(DirectionBinding, DirectionProbe)

    def __call__(self):
        return self.binding.direction


class LookupInReroute(Lookup):
    # Probe a reroute binding.

    # All recipe-generating lookup requests.
    adapts_many((RerouteBinding, AttributeProbe),
                (RerouteBinding, ReferenceProbe),
                (RerouteBinding, ComplementProbe),
                (RerouteBinding, ExpansionProbe))

    def __call__(self):
        # Reroute all probes to the target binding.
        return lookup(self.binding.target, self.probe)


class LookupReferenceInReferenceReroute(Lookup):
    # Probe for a reference in a reference reroute binding.

    adapts(ReferenceRerouteBinding, ReferenceProbe)

    def __call__(self):
        # Reroute the probe to the target binding.
        return lookup(self.binding.target, self.probe)


class GuessTitleFromTitle(Lookup):
    # Extract the title from a title decorator.

    adapts(TitleBinding, GuessTitleProbe)

    def __call__(self):
        return [self.binding.title]


class GuessNameFromAlias(Lookup):
    # Extract an attribute name from a syntax decorator.

    adapts(AliasBinding, GuessNameProbe)

    def __call__(self):
        # If the binding is associated with an identifier node,
        # use the identifier name.
        if isinstance(self.binding.syntax, IdentifierSyntax):
            return self.binding.syntax.value
        # Otherwise, fail to produce a name.
        return None


class GuessTitleFromAlias(Lookup):
    # Extract a title from a syntax decorator.

    adapts(AliasBinding, GuessTitleProbe)

    def __call__(self):
        return [str(self.binding.syntax)]


def prescribe(arc, binding):
    prescribe = Prescribe(arc, binding)
    return prescribe()


def lookup(binding, probe):
    """
    Applies a lookup probe to a binding node.

    The type of a returned value depends on the type of `probe`.
    The value of ``None`` indicates the lookup request failed.

    `binding` (:class:`htsql.tr.binding.Binding`)
        A binding node.

    `probe` (:class:`Probe`)
        A lookup probe.
    """
    # Realize and apply a `Lookup` adapter.
    lookup = Lookup(binding, probe)
    return lookup()


def lookup_attribute(binding, name, arity=None):
    """
    Finds an attribute in the scope of the given binding.

    Returns an instance of :class:`htsql.tr.binding.Recipe`
    or ``None`` if an attribute is not found.

    `binding` (:class:`htsql.tr.binding.Binding`)
        A binding node.

    `name` (a Unicode string)
        An attribute name.

    `arity` (an integer or ``None``)
        The number of arguments for a parameterized attribute;
        ``None`` for an attribute without parameters.
    """
    probe = AttributeProbe(name, arity)
    return lookup(binding, probe)


def lookup_reference(binding, name):
    """
    Finds a reference in the scope of the given binding.

    Returns an instance of :class:`htsql.tr.binding.Recipe`
    or ``None`` if a reference is not found.

    `binding` (:class:`htsql.tr.binding.Binding`)
        A binding node.

    `name` (a Unicode string)
        A reference name.
    """
    probe = ReferenceProbe(name)
    return lookup(binding, probe)


def lookup_complement(binding):
    """
    Extracts a complement link from the scope of the given binding.

    Returns an instance of :class:`htsql.tr.binding.Recipe`
    or ``None`` if a complement link is not found.

    `binding` (:class:`htsql.tr.binding.Binding`)
        A binding node.
    """
    probe = ComplementProbe()
    return lookup(binding, probe)


def expand(binding, is_soft=True, is_hard=True):
    """
    Extracts public attributes from the given binding.

    Returns a list of pairs `(syntax, recipe)`, where
    `recipe` is an instance of :class:`htsql.tr.binding.Recipe` and
    `syntax` is an instance of :class:`htsql.tr.syntax.Syntax`.
    The function returns ``None`` if the scope does not support
    public attributes.

    `is_soft` (Boolean)
        If set, the function expands selector expressions.

    `is_hard` (Boolean)
        If set, the function expands classes.
    """
    probe = ExpansionProbe(is_soft=is_soft, is_hard=is_hard)
    recipies = lookup(binding, probe)
    if recipies is not None:
        recipies = list(recipies)
    return recipies


def guess_name(binding):
    """
    Extracts an attribute name from the given binding.

    Returns a string value; ``None`` if the node is not associated
    with any attribute.
    """
    probe = GuessNameProbe()
    return lookup(binding, probe)


def guess_title(binding):
    """
    Extracts a heading from the given binding.

    Returns a list of string values: the header associated with
    the binding node.
    """
    probe = GuessTitleProbe()
    return lookup(binding, probe)


def direct(binding):
    """
    Extracts a direction indicator.

    Returns ``+1`` or ``-1`` to indicate ascending or descending order
    respectively; ``None`` if the node does not have a direction
    indicator.
    """
    probe = DirectionProbe()
    return lookup(binding, probe)


def lookup_command(binding):
    probe = CommandProbe()
    return lookup(binding, probe)


