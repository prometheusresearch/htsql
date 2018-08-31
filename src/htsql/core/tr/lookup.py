#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


"""
:mod:`htsql.core.tr.lookup`
===========================

This module implements name resolution adapters.
"""


from ..util import Clonable, Printable, maybe
from ..adapter import Adapter, adapt, adapt_many
from ..entity import DirectJoin
from ..model import (HomeNode, TableNode, Arc, TableArc, ChainArc, ColumnArc,
        SyntaxArc, InvalidArc, AmbiguousArc)
from ..classify import classify, relabel, localize, normalize
from ..syn.syntax import IdentifierSyntax
from ..error import point
from .binding import (Binding, ScopeBinding, ChainingBinding, WrappingBinding,
        DecorateBinding, CollectBinding, HomeBinding, RootBinding,
        TableBinding, ChainBinding, ColumnBinding, QuotientBinding,
        ComplementBinding, CoverBinding, ForkBinding, AttachBinding,
        ClipBinding, LocateBinding, RescopingBinding, DefineBinding,
        DefineReferenceBinding, DefineCollectionBinding, DefineLiftBinding,
        SelectionBinding, WildSelectionBinding, DirectionBinding,
        RerouteBinding, ReferenceRerouteBinding, TitleBinding, AliasBinding,
        ImplicitCastBinding, FreeTableRecipe, AttachedTableRecipe,
        ColumnRecipe, ComplementRecipe, KernelRecipe, BindingRecipe,
        IdentityRecipe, ChainRecipe, SubstitutionRecipe, ClosedRecipe,
        InvalidRecipe, AmbiguousRecipe)


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

    The result of this probe is a :class:`htsql.core.tr.binding.Recipe`
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
        assert isinstance(name, str)
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
            return "?%s" % self.key
        else:
            return "?%s(%s)" % (self.key, ",".join(["_"]*self.arity))


class AttributeSetProbe(Probe):
    """
    Represents a request for a set of all attributes with their arities.
    """


class ReferenceProbe(Probe):
    """
    Represents a request for a reference.

    The result of this probe is a :class:`htsql.core.tr.binding.Recipe`
    instance.

    `name` (a Unicode string)
        The reference name.

    Other attributes:

    `key` (a string)
        The normal form of the name.
    """

    def __init__(self, name):
        assert isinstance(name, str)
        self.name = name
        self.key = normalize(name)

    def __str__(self):
        # Display:
        #   ?$<key>
        return "?$%s" % self.key


class ReferenceSetProbe(Probe):
    """
    Represents a request for a set of all references.
    """


class ComplementProbe(Probe):
    """
    Represents a request for a complement link.

    The result of this probe is a :class:`htsql.core.tr.binding.Recipe`
    instance.
    """

    def __str__(self):
        return "?<^>"


class ExpansionProbe(Probe):
    """
    Represents expansion requests.

    The result of this probe is a list of pairs `(recipe, syntax)`,
    where `recipe` is an instance of :class:`htsql.core.tr.binding.Recipe` and
    `syntax` is an instance of :class:`htsql.core.tr.syntax.Syntax`.
    """

    def __init__(self, with_syntax=False, with_wild=False,
                 with_class=False, with_link=False):
        self.with_syntax = with_syntax
        self.with_wild = with_wild
        self.with_class = with_class
        self.with_link = with_link

    def __str__(self):
        ## Display:
        ##   ?<*|**>
        #symbols = []
        #if self.is_soft:
        #    symbols.append("*")
        #if self.is_hard:
        #    symbols.append("**")
        #return "?<%s>" % "|".join(symbols)
        return "?<*>"


class IdentityProbe(Probe):
    pass


class GuessTagProbe(Probe):
    pass


class GuessHeaderProbe(Probe):
    pass


class GuessPathProbe(Probe):
    pass


class DirectionProbe(Probe):
    """
    Represents a request for a direction modifier.

    The result of this probe is ``+1`` or ``-1`` --- the direction
    indicator.
    """

    def __str__(self):
        return "?<+|->"


class UnwrapProbe(Probe):

    def __init__(self, binding_class, is_deep=True):
        assert (isinstance(binding_class, type) and
                issubclass(binding_class, Binding))
        assert isinstance(is_deep, bool)
        self.binding_class = binding_class
        self.is_deep = is_deep


class Prescribe(Adapter):

    adapt(Arc)

    def __init__(self, arc, binding):
        assert isinstance(arc, Arc)
        assert isinstance(binding, Binding)
        self.arc = arc
        self.binding = binding

    def __call__(self):
        return InvalidRecipe()


class PrescribeTable(Prescribe):

    adapt(TableArc)

    def __call__(self):
        return FreeTableRecipe(self.arc.table)


class PrescribeColumn(Prescribe):

    adapt(ColumnArc)

    def __call__(self):
        link = (prescribe(self.arc.link, self.binding)
                if self.arc.link is not None else None)
        return ColumnRecipe(self.arc.column, link)


class PrescribeChain(Prescribe):

    adapt(ChainArc)

    def __call__(self):
        return AttachedTableRecipe(self.arc.joins)


class PrescribeSyntax(Prescribe):

    adapt(SyntaxArc)

    def __call__(self):
        recipe = SubstitutionRecipe(self.binding, [], self.arc.parameters,
                                    self.arc.syntax)
        return ClosedRecipe(recipe)


class PrescribeAmbiguous(Prescribe):

    adapt(AmbiguousArc)

    def __call__(self):
        alternatives = []
        for arc in self.arc.alternatives:
            labels = relabel(arc)
            if labels:
                alternatives.append(labels[0].name)
        return AmbiguousRecipe(alternatives)


class Lookup(Adapter):
    """
    Extracts information from a binding node.

    This is an interface adapter, see subclasses for implementations.

    The :class:`Lookup` adapter has the following signature::

        Lookup: (Binding, Probe) -> ...

    The adapter is polymorphic on both arguments.  The type of the output
    value depends on the type of the `Probe` argument.  ``None`` is returned
    when the request cannot be satisfied.

    `binding` (:class:`htsql.core.tr.binding.Binding`)
        The binding node.

    `probe` (:class:`Probe`)
        The lookup request.
    """

    adapt(Binding, Probe)

    def __init__(self, binding, probe):
        #assert isinstance(binding, Binding)
        #assert isinstance(probe, Probe)
        self.binding = binding
        self.probe = probe

    def __call__(self):
        # `None` means the lookup request failed.
        return None


class LookupAttributeSet(Lookup):
    # Generate a set of all available attributes.

    adapt(Binding, AttributeSetProbe)

    def __call__(self):
        # No attributes by default.
        return set()


class LookupReferenceSet(Lookup):
    # Generate a set of all available references.

    adapt(Binding, ReferenceSetProbe)

    def __call__(self):
        # No references by default.
        return set()


class GuessTag(Lookup):

    adapt(Binding, GuessTagProbe)

    def __call__(self):
        if isinstance(self.binding.syntax, IdentifierSyntax):
            return self.binding.syntax.text
        return None


class GuessHeader(Lookup):

    adapt(Binding, GuessHeaderProbe)

    def __call__(self):
        value = str(self.binding.syntax)
        if value:
            return value
        return None


class Unwrap(Lookup):

    adapt(Binding, UnwrapProbe)

    def __call__(self):
        if isinstance(self.binding, self.probe.binding_class):
            return self.binding
        return None


class LookupReferenceInScoping(Lookup):
    # Find a reference in a scoping node.

    adapt(ScopeBinding, ReferenceProbe)

    def __call__(self):
        # Delegate reference lookups to the parent binding.
        if self.binding.base is not None:
            return lookup(self.binding.base, self.probe)
        # Stop at the root binding.
        return None


class LookupReferenceSetInScoping(Lookup):
    # Find all available references in a scoping node.

    adapt(ScopeBinding, ReferenceSetProbe)

    def __call__(self):
        # Delegate reference lookups to the parent binding.
        if self.binding.base is not None:
            return lookup(self.binding.base, self.probe)
        # Stop at the root binding.
        return set()


class LookupInChaining(Lookup):
    # Pass all lookup requests (except name guesses)
    # through chaining nodes.

    # Everything but `GuessNameProbe`.  Can't use `Probe`
    # since it causes ambiguous ordering of components.
    adapt_many((ChainingBinding, AttributeProbe),
               (ChainingBinding, AttributeSetProbe),
               (ChainingBinding, ReferenceProbe),
               (ChainingBinding, ReferenceSetProbe),
               (ChainingBinding, ComplementProbe),
               (ChainingBinding, ExpansionProbe),
               (ChainingBinding, IdentityProbe),
               (ChainingBinding, DirectionProbe),
               (DecorateBinding, AttributeProbe),
               (DecorateBinding, AttributeSetProbe),
               (DecorateBinding, ReferenceProbe),
               (DecorateBinding, ReferenceSetProbe),
               (DecorateBinding, ComplementProbe),
               (DecorateBinding, ExpansionProbe),
               (DecorateBinding, IdentityProbe),
               (DecorateBinding, DirectionProbe))

    def __call__(self):
        # Delegate all lookup requests to the parent binding.
        return lookup(self.binding.base, self.probe)


class GuessTagFromChaining(Lookup):

    adapt_many((ChainingBinding, GuessTagProbe),
               (DecorateBinding, GuessTagProbe))

    def __call__(self):
        if isinstance(self.binding.syntax, IdentifierSyntax):
            return self.binding.syntax.text
        return lookup(self.binding.base, self.probe)


class GuessHeaderInChaining(Lookup):

    adapt_many((ChainingBinding, GuessHeaderProbe),
               (DecorateBinding, GuessHeaderProbe))

    def __call__(self):
        return lookup(self.binding.base, self.probe)


class GuessPathFromChaining(Lookup):

    adapt_many((ChainingBinding, GuessPathProbe),
               (DecorateBinding, GuessPathProbe))

    def __call__(self):
        return lookup(self.binding.base, self.probe)


class UnwrapChaining(Lookup):

    adapt(ChainingBinding, UnwrapProbe)

    def __call__(self):
        if isinstance(self.binding, self.probe.binding_class):
            return self.binding
        if self.probe.is_deep:
            return lookup(self.binding.base, self.probe)
        return None


class LookupInImplicitCast(Lookup):

    adapt_many((ImplicitCastBinding, GuessTagProbe),
               (ImplicitCastBinding, GuessHeaderProbe),
               (ImplicitCastBinding, GuessPathProbe),
               (ImplicitCastBinding, DirectionProbe))

    def __call__(self):
        return lookup(self.binding.base, self.probe)


class UnwrapImplicitCast(Lookup):

    adapt(ImplicitCastBinding, UnwrapProbe)

    def __call__(self):
        if isinstance(self.binding, self.probe.binding_class):
            return self.binding
        if self.probe.is_deep:
            return lookup(self.binding.base, self.probe)
        return None


class UnwrapWrapping(Lookup):

    adapt_many((WrappingBinding, UnwrapProbe),
               (DecorateBinding, UnwrapProbe))

    def __call__(self):
        if isinstance(self.binding, self.probe.binding_class):
            return self.binding
        return lookup(self.binding.base, self.probe)


class GuessFromCollect(Lookup):

    adapt_many((CollectBinding, GuessTagProbe),
               (CollectBinding, GuessHeaderProbe),
               (CollectBinding, GuessPathProbe))

    def __call__(self):
        return lookup(self.binding.seed, self.probe)


class UnwrapCollect(Lookup):

    adapt(CollectBinding, UnwrapProbe)

    def __call__(self):
        if isinstance(self.binding, self.probe.binding_class):
            return self.binding
        if self.probe.is_deep:
            return lookup(self.binding.seed, self.probe)
        return None


class LookupAttributeInHome(Lookup):
    # Attributes of the *home* scope represent database tables.

    adapt(HomeBinding, AttributeProbe)

    def __call__(self):
        labels = classify(HomeNode())
        label_by_signature = dict(((label.name, label.arity), label)
                                  for label in labels)
        if (self.probe.key, self.probe.arity) not in label_by_signature:
            return None
        label = label_by_signature[self.probe.key, self.probe.arity]
        recipe = prescribe(label.arc, self.binding)
        return recipe


class LookupAttributeSetInHome(Lookup):
    # Attributes of the *home* scope represent database tables.

    adapt(HomeBinding, AttributeSetProbe)

    def __call__(self):
        attributes = set()
        labels = classify(HomeNode())
        for label in labels:
            # Skip invalid labels.
            if isinstance(label.arc, InvalidArc):
                continue
            attributes.add((label.name, label.arity))
        return attributes


class ExpandHome(Lookup):
    # The home class contains no public attributes.

    adapt(HomeBinding, ExpansionProbe)

    def __call__(self):
        # Expand the home class: there should be no public attributes, but try
        # it anyway.
        if self.probe.with_class:
            labels = classify(HomeNode())
            recipes = []
            for label in labels:
                if not label.is_public:
                    continue
                identifier = IdentifierSyntax(label.name)
                point(identifier, self.binding)
                recipe = prescribe(label.arc, self.binding)
                recipes.append((identifier, recipe))
            return recipes
        # Otherwise, fail to expand.
        return None


class GuessHeaderFromHome(Lookup):

    adapt(HomeBinding, GuessHeaderProbe)

    def __call__(self):
        return None


class GuessPathFromRoot(Lookup):

    adapt(RootBinding, GuessPathProbe)

    def __call__(self):
        return []


class LookupAttributeInTable(Lookup):
    # A table context contains three types of members:
    # - table columns;
    # - referenced tables, i.e., tables for which there exists a foreign
    #   key from the scope table;
    # - referencing tables, i.e., tables with a foreign key to the
    #   context table.

    adapt(TableBinding, AttributeProbe)

    def __call__(self):
        labels = classify(TableNode(self.binding.table))
        label_by_signature = dict(((label.name, label.arity), label)
                                  for label in labels)
        if (self.probe.key, self.probe.arity) not in label_by_signature:
            return None
        label = label_by_signature[self.probe.key, self.probe.arity]
        recipe = prescribe(label.arc, self.binding)
        return recipe


class LookupAttributeSetInTable(Lookup):
    # Produce a set of all labels available in a table scope.

    adapt(TableBinding, AttributeSetProbe)

    def __call__(self):
        attributes = set()
        labels = classify(TableNode(self.binding.table))
        for label in labels:
            # Skip invalid labels.
            if isinstance(label.arc, InvalidArc):
                continue
            attributes.add((label.name, label.arity))
        return attributes


class ExpandTable(Lookup):
    # Extract all the columns of the table.

    adapt(TableBinding, ExpansionProbe)

    def __call__(self):
        # Only expand on a class probe.
        if not self.probe.with_class:
            return super(ExpandTable, self).__call__()
        return self.itemize_columns()

    def itemize_columns(self):
        labels = classify(TableNode(self.binding.table))
        for label in labels:
            if not label.is_public:
                continue
            # Create a "virtual" syntax node for each column
            identifier = IdentifierSyntax(label.name)
            point(identifier, self.binding)
            recipe = prescribe(label.arc, self.binding)
            yield (identifier, recipe)


class IdentifyTable(Lookup):

    adapt(TableBinding, IdentityProbe)

    def __call__(self):
        def chain(node):
            arcs = localize(node)
            if arcs is None:
                return None
            recipes = []
            for arc in arcs:
                recipe = prescribe(arc, self.binding)
                target_chain = chain(arc.target)
                if target_chain is not None:
                    recipe = ChainRecipe([recipe, target_chain])
                recipes.append(recipe)
            return IdentityRecipe(recipes)
        return chain(TableNode(self.binding.table))


class GuessPathForTable(Lookup):

    adapt(TableBinding, GuessPathProbe)

    def __call__(self):
        path = lookup(self.binding.base, self.probe)
        if path is None:
            return None
        if path:
            node = path[-1].target
        else:
            node = HomeNode()
        arc = None
        for label in classify(node):
            if (isinstance(label.arc, TableArc) and
                    (label.arc.table == self.binding.table)):
                arc = label.arc
                break
        if arc is None:
            return None
        return path+[arc]


class GuessPathForChain(Lookup):

    adapt(ChainBinding, GuessPathProbe)

    def __call__(self):
        path = lookup(self.binding.base, self.probe)
        if not path:
            return None
        arc = None
        parent_arc = path[-1]
        if isinstance(parent_arc, ColumnArc) and parent_arc.link is not None:
            parent_arc = parent_arc.link
        for label in classify(parent_arc.target):
            if (isinstance(label.arc, ChainArc) and
                    (label.arc.joins == self.binding.joins)):
                arc = label.arc
                break
        if arc is None:
            return None
        return path+[arc]


class LookupAttributeInColumn(Lookup):
    # Find a member with the given name in a column scope.
    #
    # All lookup requests are redirected to the link associated
    # with the column; if there is no associated link, the request
    # fails.

    adapt_many((ColumnBinding, AttributeProbe),
               (ColumnBinding, IdentityProbe))

    def __call__(self):
        # If there is an associated link node, delegate the request to it.
        if self.binding.link is not None:
            return lookup(self.binding.link, self.probe)
        # Otherwise, no luck.
        return None


class LookupAttributeSetInColumn(Lookup):
    # Find all available attributes in a column scope.

    adapt(ColumnBinding, AttributeSetProbe)

    def __call__(self):
        if self.binding.link is not None:
            return lookup(self.binding.link, self.probe)
        return set()


class ExpandColumn(Lookup):
    # Produce all public members in a column scope.
    #
    # The request is delegated to the link associated with the
    # column; if there is no associated link, the request fails.

    adapt(ColumnBinding, ExpansionProbe)

    def __call__(self):
        # If there is an associated link node, delegate the request to it.
        if self.probe.with_link and self.binding.link is not None:
            return lookup(self.binding.link, self.probe)
        # Otherwise, no luck.
        return None


class GuessPathForColumn(Lookup):

    adapt(ColumnBinding, GuessPathProbe)

    def __call__(self):
        path = lookup(self.binding.base, self.probe)
        if not path:
            return None
        arc = None
        parent_arc = path[-1]
        if isinstance(parent_arc, ColumnArc) and parent_arc.link is not None:
            parent_arc = parent_arc.link
        for label in classify(parent_arc.target):
            if (isinstance(label.arc, ColumnArc) and
                    (label.arc.column == self.binding.column)):
                arc = label.arc
                break
        if arc is None:
            return None
        return path+[arc]


class LookupComplementInQuotient(Lookup):
    # Extract a complement from a quotient scope.

    adapt(QuotientBinding, ComplementProbe)

    def __call__(self):
        return ComplementRecipe(self.binding)


class ExpandQuotient(Lookup):
    # Extract public "columns" from a quotient scope.

    adapt(QuotientBinding, ExpansionProbe)

    def __call__(self):
        # Ignore non-class expansion requests.
        if not self.probe.with_class:
            return super(ExpandQuotient, self).__call__()
        # Expand the kernel.
        return self.itemize_kernels()

    def itemize_kernels(self):
        # Produce a recipe for each expression in the kernel.
        for index, binding in enumerate(self.binding.kernels):
            syntax = binding.syntax
            recipe = KernelRecipe(self.binding, index)
            yield (syntax, recipe)


class GuessHeaderFromQuotient(Lookup):

    adapt(QuotientBinding, GuessHeaderProbe)

    def __call__(self):
        seed_header = lookup(self.binding.seed, self.probe)
        kernel_headers = [lookup(kernel, self.probe)
                          for kernel in self.binding.kernels]
        if seed_header is None or any(header is None
                                      for header in kernel_headers):
            return super(GuessHeaderFromQuotient, self).__call__()
        if len(kernel_headers) == 1:
            [kernel_header] = kernel_headers
            return "%s^%s" % (seed_header, kernel_header)
        else:
            return "%s^{%s}" % (seed_header, ",".join(kernel_headers))


class LookupAttributeInComplement(Lookup):
    # Find an attribute or a complement link in a complement scope.

    adapt_many((ComplementBinding, AttributeProbe),
               (ComplementBinding, AttributeSetProbe),
               (ComplementBinding, ComplementProbe),
               (ComplementBinding, IdentityProbe))

    def __call__(self):
        # Delegate all lookup probes to the seed scope.
        return lookup(self.binding.quotient.seed, self.probe)


class ExpandComplement(Lookup):
    # Extract public columns from a complement scope.

    adapt(ComplementBinding, ExpansionProbe)

    def __call__(self):
        # Ignore selection expand probes.
        if not self.probe.with_class:
            return super(ExpandComplement, self).__call__()
        # Delegate class expansion probe to the seed flow;
        # turn off selection expansion to avoid expanding the
        # selector in `distinct(table{kernel})`.
        probe = self.probe.clone(with_syntax=False, with_wild=False)
        return lookup(self.binding.quotient.seed, probe)


class LookupAttributeInCover(Lookup):
    # Find an attribute in a cover scope.

    adapt_many((CoverBinding, AttributeProbe),
               (CoverBinding, AttributeSetProbe),
               (CoverBinding, ComplementProbe),
               (CoverBinding, IdentityProbe),
               (ClipBinding, AttributeProbe),
               (ClipBinding, AttributeSetProbe),
               (ClipBinding, ComplementProbe),
               (ClipBinding, IdentityProbe),
               (AttachBinding, AttributeProbe),
               (AttachBinding, AttributeSetProbe),
               (AttachBinding, ComplementProbe),
               (AttachBinding, IdentityProbe),
               (LocateBinding, AttributeProbe),
               (LocateBinding, AttributeSetProbe),
               (LocateBinding, ComplementProbe),
               (LocateBinding, IdentityProbe))

    def __call__(self):
        # Delegate all lookup requests to the seed flow.
        return lookup(self.binding.seed, self.probe)


class ExpandCover(Lookup):
    # Expand public columns from a cover scope.

    adapt_many((CoverBinding, ExpansionProbe),
               (ClipBinding, ExpansionProbe),
               (LocateBinding, ExpansionProbe))

    def __call__(self):
        # Ignore pure selector expansion probes.
        if not self.probe.with_class:
            return super(ExpandCover, self).__call__()
        # FIXME: selector expansion does not work between scopes.
        ## Turn on selector expansion.
        #probe = self.probe.clone(is_soft=True)
        # Delegate the probe to the seed class;
        #return lookup(self.binding.seed, probe)
        probe = self.probe.clone(with_syntax=False, with_wild=False)
        return lookup(self.binding.seed, probe)


class GuessTagHeaderFromLocatorClipPath(Lookup):

    adapt_many((LocateBinding, GuessTagProbe),
               (LocateBinding, GuessHeaderProbe),
               (LocateBinding, GuessPathProbe),
               (ClipBinding, GuessTagProbe),
               (ClipBinding, GuessHeaderProbe),
               (ClipBinding, GuessPathProbe))

    def __call__(self):
        return lookup(self.binding.seed, self.probe)


class LookupAttributeInFork(Lookup):
    # Find an attribute or a complement link in a fork scope.

    adapt_many((ForkBinding, AttributeProbe),
               (ForkBinding, AttributeSetProbe),
               (ForkBinding, ComplementProbe),
               (ForkBinding, IdentityProbe))

    def __call__(self):
        # Delegate all lookup probes to the parent binding.
        return lookup(self.binding.base, self.probe)


class ExpandFork(Lookup):
    # Extract public columns from a fork scope.

    adapt(ForkBinding, ExpansionProbe)

    def __call__(self):
        # Delegate the expansion probe to the parent binding.
        # FIXME: selector expansion does not work between scopes.
        #return lookup(self.binding.base, self.probe)
        probe = self.probe.clone(with_syntax=False, with_wild=False)
        return lookup(self.binding.base, probe)


class GuessHeaderFromLink(Lookup):

    adapt(AttachBinding, GuessHeaderProbe)

    def __call__(self):
        return lookup(self.binding.seed, self.probe)


class DirectRescoping(Lookup):
    # Extract a direction decorator from a rescoping binding.

    adapt(RescopingBinding, DirectionProbe)

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


class LookupAttributeInDefine(Lookup):
    # Find an attribute in a definition binding.

    adapt(DefineBinding, AttributeProbe)

    def __call__(self):
        # Check if the definition matches the probe.
        if self.binding.arity == self.probe.arity:
            binding_key = normalize(self.binding.name)
            if binding_key == self.probe.key:
                # If it matches, produce the associated recipe.
                return self.binding.recipe
        # Otherwise, delegate the probe to the parent binding.
        return super(LookupAttributeInDefine, self).__call__()


class LookupAttributeInDefineCollection(Lookup):

    adapt(DefineCollectionBinding, AttributeProbe)

    def __call__(self):
        if (not self.binding.is_reference and
                self.probe.arity is None and
                self.probe.key in self.binding.collection):
            return self.binding.collection[self.probe.key]
        return super(LookupAttributeInDefineCollection, self).__call__()


class LookupAttributeSetInDefine(Lookup):
    # Find all attributes in a definition binding.

    adapt(DefineBinding, AttributeSetProbe)

    def __call__(self):
        attributes = super(LookupAttributeSetInDefine, self).__call__()
        attributes.add((normalize(self.binding.name), self.binding.arity))
        return attributes


class LookupAttributeSetInDefineCollection(Lookup):

    adapt(DefineCollectionBinding, AttributeSetProbe)

    def __call__(self):
        attributes = super(LookupAttributeSetInDefineCollection, self).__call__()
        if not self.binding.is_reference:
            for name in self.binding.collection:
                attributes.add((name, None))
        return attributes


class LookupReferenceInDefineReference(Lookup):
    # Find a reference in a definition binding.

    adapt(DefineReferenceBinding, ReferenceProbe)

    def __call__(self):
        # Check if the definition matches the probe.
        binding_key = normalize(self.binding.name)
        if binding_key == self.probe.key:
            # If it matches, produce the associated recipe.
            return self.binding.recipe
        # Otherwise, delegate the probe to the parent binding.
        return super(LookupReferenceInDefineReference, self).__call__()


class LookupReferenceInDefineCollection(Lookup):

    adapt(DefineCollectionBinding, ReferenceProbe)

    def __call__(self):
        if (self.binding.is_reference and
                self.probe.key in self.binding.collection):
            return self.binding.collection[self.probe.key]
        return super(LookupReferenceInDefineCollection, self).__call__()


class LookupReferenceSetInDefineReference(Lookup):
    # Find all references in a definition binding.

    adapt(DefineReferenceBinding, ReferenceSetProbe)

    def __call__(self):
        references = super(LookupReferenceSetInDefineReference, self).__call__()
        references.add(normalize(self.binding.name))
        return references


class LookupReferenceSetInDefineCollection(Lookup):

    adapt(DefineCollectionBinding, ReferenceSetProbe)

    def __call__(self):
        references = super(LookupReferenceSetInDefineCollection, self).__call__()
        if self.binding.is_reference:
            for name in self.binding.collection:
                references.add(name)
        return references


class LookupComplementInDefineLift(Lookup):

    adapt(DefineLiftBinding, ComplementProbe)

    def __call__(self):
        return self.binding.recipe


class ExpandSelection(Lookup):
    # Expand a selector operation.

    adapt(SelectionBinding, ExpansionProbe)

    def __call__(self):
        # Skip class expansion probes.
        if not self.probe.with_syntax:
            probe = self.probe.clone(with_wild=False)
            return lookup(self.binding.base, probe)
        # Emit elements of the selector.
        return self.itemize()

    def itemize(self):
        # Emit elements of the selector.
        for element in self.binding.elements:
            syntax = element.syntax
            recipe = BindingRecipe(element)
            yield (syntax, recipe)


class ExpandWildSelection(Lookup):
    # Expand a *-selector.

    adapt(WildSelectionBinding, ExpansionProbe)

    def __call__(self):
        # Skip class expansion probes.
        if not self.probe.with_wild:
            probe = self.probe.clone(with_syntax=False)
            return lookup(self.binding.base, probe)
        # Emit elements of the selector.
        return self.itemize()


class DirectDirection(Lookup):
    # Extract a direction indicator from a direction binding.

    adapt(DirectionBinding, DirectionProbe)

    def __call__(self):
        return self.binding.direction


class LookupInReroute(Lookup):
    # Probe a reroute binding.

    # All recipe-generating lookup requests.
    adapt_many((RerouteBinding, AttributeProbe),
               (RerouteBinding, AttributeSetProbe),
               (RerouteBinding, ReferenceProbe),
               (RerouteBinding, ReferenceSetProbe),
               (RerouteBinding, ComplementProbe),
               (RerouteBinding, ExpansionProbe))

    def __call__(self):
        # Reroute all probes to the target binding.
        return lookup(self.binding.target, self.probe)


class LookupReferenceInReferenceReroute(Lookup):
    # Probe for a reference in a reference reroute binding.

    adapt_many((ReferenceRerouteBinding, ReferenceProbe),
               (ReferenceRerouteBinding, ReferenceSetProbe))

    def __call__(self):
        # Reroute the probe to the target binding.
        return lookup(self.binding.target, self.probe)


class GuessHeaderFromTitle(Lookup):

    adapt(TitleBinding, GuessHeaderProbe)

    def __call__(self):
        if isinstance(self.binding.title, IdentifierSyntax):
            return self.binding.title.text
        return self.binding.title.text


class GuessTagFromTitle(Lookup):

    adapt(TitleBinding, GuessTagProbe)

    def __call__(self):
        if isinstance(self.binding.title, IdentifierSyntax):
            return self.binding.title.text
        return super(GuessTagFromTitle, self).__call__()


class GuessTagFromAlias(Lookup):

    adapt(AliasBinding, GuessTagProbe)

    def __call__(self):
        if isinstance(self.binding.syntax, IdentifierSyntax):
            return self.binding.syntax.text
        return None


class GuessHeaderFromAlias(Lookup):

    adapt(AliasBinding, GuessHeaderProbe)

    def __call__(self):
        return str(self.binding.syntax)


def prescribe(arc, binding):
    return Prescribe.__invoke__(arc, binding)


def lookup(binding, probe):
    """
    Applies a lookup probe to a binding node.

    The type of a returned value depends on the type of `probe`.
    The value of ``None`` indicates the lookup request failed.

    `binding` (:class:`htsql.core.tr.binding.Binding`)
        A binding node.

    `probe` (:class:`Probe`)
        A lookup probe.
    """
    # Realize and apply a `Lookup` adapter.
    return Lookup.__invoke__(binding, probe)


def lookup_attribute(binding, name, arity=None):
    """
    Finds an attribute in the scope of the given binding.

    Returns an instance of :class:`htsql.core.tr.binding.Recipe`
    or ``None`` if an attribute is not found.

    `binding` (:class:`htsql.core.tr.binding.Binding`)
        A binding node.

    `name` (a Unicode string)
        An attribute name.

    `arity` (an integer or ``None``)
        The number of arguments for a parameterized attribute;
        ``None`` for an attribute without parameters.
    """
    probe = AttributeProbe(name, arity)
    return lookup(binding, probe)


def lookup_attribute_set(binding):
    """
    Produces a set of all available attributes and their arities.
    """
    probe = AttributeSetProbe()
    return lookup(binding, probe)


def lookup_reference(binding, name):
    """
    Finds a reference in the scope of the given binding.

    Returns an instance of :class:`htsql.core.tr.binding.Recipe`
    or ``None`` if a reference is not found.

    `binding` (:class:`htsql.core.tr.binding.Binding`)
        A binding node.

    `name` (a Unicode string)
        A reference name.
    """
    probe = ReferenceProbe(name)
    return lookup(binding, probe)


def lookup_reference_set(binding):
    """
    Produces a set of all available references.
    """
    probe = ReferenceSetProbe()
    return lookup(binding, probe)


def lookup_complement(binding):
    """
    Extracts a complement link from the scope of the given binding.

    Returns an instance of :class:`htsql.core.tr.binding.Recipe`
    or ``None`` if a complement link is not found.

    `binding` (:class:`htsql.core.tr.binding.Binding`)
        A binding node.
    """
    probe = ComplementProbe()
    return lookup(binding, probe)


def expand(binding, with_syntax=False, with_wild=False,
           with_class=False, with_link=False):
    """
    Extracts public attributes from the given binding.

    Returns a list of pairs `(syntax, recipe)`, where
    `recipe` is an instance of :class:`htsql.core.tr.binding.Recipe` and
    `syntax` is an instance of :class:`htsql.core.tr.syntax.Syntax`.
    The function returns ``None`` if the scope does not support
    public attributes.
    """
    probe = ExpansionProbe(with_syntax=with_syntax, with_wild=with_wild,
                           with_class=with_class, with_link=with_link)
    recipes = lookup(binding, probe)
    if recipes is not None:
        recipes = list(recipes)
    return recipes


def identify(binding):
    probe = IdentityProbe()
    return lookup(binding, probe)


def guess_tag(binding):
    probe = GuessTagProbe()
    return lookup(binding, probe)


def guess_header(binding):
    probe = GuessHeaderProbe()
    return lookup(binding, probe)


def guess_path(binding):
    probe = GuessPathProbe()
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


def unwrap(binding, binding_class, is_deep=True):
    probe = UnwrapProbe(binding_class, is_deep)
    return lookup(binding, probe)


