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
from ..adapter import Adapter, adapts, adapts_many, Utility
from ..context import context
from ..introspect import Introspect
from ..entity import DirectJoin, ReverseJoin, TableEntity
from .syntax import Syntax, IdentifierSyntax
from .binding import (Binding, ScopingBinding, ChainingBinding, WrappingBinding,
                      SegmentBinding, HomeBinding, RootBinding, TableBinding,
                      ColumnBinding, QuotientBinding, ComplementBinding,
                      CoverBinding, ForkBinding, LinkBinding, RescopingBinding,
                      DefinitionBinding, SelectionBinding, DirectionBinding,
                      RerouteBinding, ReferenceRerouteBinding,
                      TitleBinding, AliasBinding, CommandBinding,
                      FreeTableRecipe, AttachedTableRecipe, ColumnRecipe,
                      ComplementRecipe, KernelRecipe, SubstitutionRecipe,
                      BindingRecipe, PinnedRecipe, AmbiguousRecipe)
import re
import unicodedata


def get_catalog():
    """
    Returns the catalog object; generates it if necessary.
    """
    # FIXME: use locking; move to a more appropriate place.
    # Get the database metadata.  Check if it was loaded before;
    # if not, load it from the database and cache it as an application
    # attribute.
    app = context.app
    if app.htsql.cached_catalog is None:
        introspect = Introspect()
        catalog = introspect()
        app.htsql.cached_catalog = catalog
    return app.htsql.cached_catalog


def normalize(name):
    """
    Normalizes a name to provide a valid HTSQL identifier.

    We assume `name` is a valid UTF-8 string.  Then it is:

    - translated to Unicode normal form C;
    - converted to lowercase;
    - has non-alphanumeric characters replaced with underscores;
    - preceded with an underscore if it starts with a digit.

    The result is a valid HTSQL identifier.
    """
    assert isinstance(name, str) and len(name) > 0
    name = name.decode('utf-8')
    name = unicodedata.normalize('NFC', name).lower()
    name = re.sub(ur"(?u)^(?=\d)|\W", u"_", name)
    name = name.encode('utf-8')
    return name


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

    `name` (a string)
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


class ReferenceProbe(Probe):
    """
    Represents a request for a reference.

    The result of this probe is a :class:`htsql.tr.binding.Recipe`
    instance.

    `name` (a string)
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


class ItemizeHome(Utility):
    """ 
    Returns top-level tables in the root context.

    The intent of this enumeration is to provide a single list
    of non-redundant tables by canonical name.  We don't expect
    the lookup list returned to contain the same table twice.
    """

    # TODO: This requires pathalogical test schemas
    #       in order to test for full coverage.
    def __init__(self):
        self.catalog = get_catalog()

    def __call__(self):
        # FIXME: keep the original case of the tables.
        buckets = {}
        for schema in self.catalog.schemas:
            for table in schema.tables:
                buckets.setdefault(normalize(table.name), []).append(table)
       
        itemization = {} 
        collisions = []
        for (name, candidates) in buckets.items():
            if len(candidates) > 1:
                rankings = [self.catalog.schemas[table.schema_name].priority
                            for table in candidates]
                max_rank = max(rankings)
                if rankings.count(max_rank) == 1:
                    chosen = candidates[rankings.index(max_rank)]
                    collisions.extend(table for table in candidates
                                      if table != chosen)
                    candidates = [chosen]
                else:
                    # schema ranking did not resolve ambiguity 
                    pass
            if len(candidates) > 1:
                collisions.extend(candidates)
                itemization[name] = AmbiguousRecipe()
            else:
                table = candidates[0]
                itemization[name] = FreeTableRecipe(table)

        for table in collisions:
            fq_name = "%s_%s" % (normalize(table.schema_name),
                                 normalize(table.name))
            if fq_name in itemization:
                # TODO: find some way to report when this
                # secondary naming scheme creates collisions
                continue
            itemization[fq_name] = FreeTableRecipe(table)

        return itemization


class ItemizeTable(Utility):
    """ 
    Returns columns and links recipies for a given table.

    Column attributes trump direct links which trump reverse links.  
    However, if two objects of the same name exist, then the 
    associated value is an ``AmbiguousRecipe``.  This could happen
    if two columns happen to have the same normalized name, or when
    there are two foreign keys to the same table.
    """

    def __init__(self, table):
        assert isinstance(table, TableEntity)
        self.table = table
        self.catalog = get_catalog()

    def __call__(self):
        # FIXME: keep the original case of the object names.
        # Builds enumeration such that columns override 
        # direct joins, which override reverse joins.

        itemization = self.itemize_reverse_joins()
        itemization.update(self.itemize_direct_joins())
        itemization.update(self.itemize_columns())
        return itemization

    def find_link(self, column):
        # Determines if the column may represents a link to another table.
        # This is the case when the column is associated with a foreign key.

        # Get a list of foreign keys associated with the given column.
        candidates = []
        for fk in self.table.foreign_keys:
            if fk.origin_column_names != [column.name]:
                continue
            candidates.append(fk)

        # Return immediately if there are no candidate keys.
        if not candidates:
            return None

        # We got an unambiguous link if there's only one foreign key
        # associated with the column,
        if len(candidates) == 1:
            # Generate the link join.
            fk = candidates[0]
            origin_schema = self.catalog.schemas[fk.origin_schema_name]
            origin = origin_schema.tables[fk.origin_name]
            target_schema = self.catalog.schemas[fk.target_schema_name]
            target = target_schema.tables[fk.target_name]
            join = DirectJoin(origin, target, fk)
            # Build and return the link binding.
            return AttachedTableRecipe([join])

        if len(candidates) > 1:
            return AmbiguousRecipe()

    def itemize_columns(self):
        # Builds mapping of column names into column recipes.
        # If two columns have the same normalized name, then 
        # the result is ambiguous.

        itemization = {}
        for column in self.table.columns:
            name = normalize(column.name)
            if name in itemization:
                itemization[name] = AmbiguousRecipe()
                continue
            link = self.find_link(column)
            itemization[name] = ColumnRecipe(column, link)
        return itemization

    def itemize_direct_joins(self):
        # Builds mapping of table names into link recipies using
        # foreign keys originating from the current table.

        itemization = {}
        for foreign_key in self.table.foreign_keys:
            name = normalize(foreign_key.target_name)
            if name in itemization:
                itemization[name] = AmbiguousRecipe()
                continue
            target_schema = self.catalog.schemas[foreign_key.target_schema_name]
            target = target_schema.tables[foreign_key.target_name]
            join = DirectJoin(self.table, target, foreign_key)
            itemization[name] = AttachedTableRecipe([join])
        return itemization

    def itemize_reverse_joins(self):
        # Builds mapping of referencing tables that possess a foreign 
        # key to current context table.

        target_pair = (self.table.schema_name, self.table.name)
        target_schema = self.catalog.schemas[self.table.schema_name]
        itemization = {}
        for schema in self.catalog.schemas:
            for table in schema.tables:
                for foreign_key in table.foreign_keys:
                    if target_pair == (foreign_key.target_schema_name,
                                       foreign_key.target_name):
                        name = normalize(foreign_key.origin_name)
                        if name in itemization:
                            itemization[name] = AmbiguousRecipe()
                            continue
                        join = ReverseJoin(self.table, table, foreign_key)
                        itemization[name] = AttachedTableRecipe([join])
        return itemization


class EnumerateTable(Utility):
    """ 
    Returns a list of public identifiers for a table.  If there
    are two columns that have same normalized name, then both
    columns are omitted from the enumeration.
    """

    def __init__(self, table):
        assert isinstance(table, TableEntity)
        self.table = table

    def __call__(self):
        names = []
        collisions = []
        for column in self.table.columns:
            name = normalize(column.name)
            if name in names:
                if name not in collisions:
                    collisions.append(name)
            else:
                names.append(name)
        for name in collisions:
            names.remove(name)
        return names 


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

    Other attributes:

    `catalog` (:class:`htsql.entity.Catalog`)
        The database metadata.
    """

    adapts(Binding, Probe)

    def __init__(self, binding, probe):
        assert isinstance(binding, Binding)
        assert isinstance(probe, Probe)
        self.binding = binding
        self.probe = probe
        self.catalog = get_catalog()

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
        return [str(self.binding.syntax)]


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
        recipe_by_name = itemize()
        return recipe_by_name.get(self.probe.key)

class ExpandHome(Lookup):
    # The home class contains no public attributes.

    adapts(HomeBinding, ExpansionProbe)

    def __call__(self):
        # Expand the home class: there are no public attributes.
        if self.probe.is_hard:
            return []
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
        recipe_by_name = itemize(self.binding.table)
        return recipe_by_name.get(self.probe.key)


class ExpandTable(Lookup):
    # Extract all the columns of the table.

    adapts(TableBinding, ExpansionProbe)

    def __call__(self):
        # Only expand on a class probe.
        if not self.probe.is_hard:
            return super(ExpandTable, self).__call__()
        return self.itemize_columns()

    def itemize_columns(self):
        recipe_by_name = itemize(self.binding.table)
        for name in enumerate_table(self.binding.table):
            # Create a "virtual" syntax node for each column
            identifier = IdentifierSyntax(name, self.binding.mark)
            recipe = recipe_by_name[name]
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
                if normalize(self.binding.name) == self.probe.key:
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
            if normalize(self.binding.name) == self.probe.key:
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

def itemize(table=None):
    """
    Returns a dictionary of names in the given context.
    If table is provided, columns and links are returned;
    otherwise this returns tables accessable from the root.
    """
    # FIXME: This needs to be cached such that an
    #        update of the catalog will cause the
    #        lookup table to be re-generated.
    if table:
	itemize = ItemizeTable(table)
    else:
        itemize = ItemizeHome()
    return itemize()
    
def enumerate_table(table):
    """
    Returns a list of public names for a table.
    """
    enumerate_table = EnumerateTable(table)
    return enumerate_table()

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

    `name` (a string)
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

    `name` (a string)
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


