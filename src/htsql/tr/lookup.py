#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.lookup`
======================

This module implements name resolution adapters.
"""


from ..util import Clonable
from ..mark import EmptyMark
from ..adapter import Adapter, adapts, adapts_many
from ..context import context
from ..introspect import Introspect
from ..entity import DirectJoin, ReverseJoin
from .syntax import Syntax, IdentifierSyntax
from .binding import (Binding, HomeBinding, RootBinding, ChainBinding,
                      TableBinding, FreeTableBinding, AttachedTableBinding,
                      ColumnBinding, SieveBinding, WrapperBinding, SortBinding,
                      QuotientBinding, ComplementBinding, KernelBinding,
                      DefinitionBinding, RedirectBinding,
                      ReverseRedirectBinding, AliasBinding,
                      SelectionBinding, DirectionBinding, FlatBinding,
                      MonikerBinding, ForkBinding, LinkBinding)
from .recipe import (FreeTableRecipe, AttachedTableRecipe, ColumnRecipe,
                     ComplementRecipe, KernelRecipe, SubstitutionRecipe,
                     BindingRecipe, PinnedRecipe, AmbiguousRecipe)
import re
import unicodedata


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


class Probe(Clonable):

    def __init__(self):
        pass


class AttributeProbe(Probe):

    def __init__(self, name):
        self.name = name
        self.key = normalize(name)


class FunctionProbe(Probe):

    def __init__(self, name, arity):
        self.name = name
        self.arity = arity
        self.key = normalize(name)


class ReferenceProbe(Probe):

    def __init__(self, name):
        self.name = name
        self.key = normalize(name)


class ComplementProbe(Probe):
    pass


class DeepAttributeProbe(AttributeProbe):
    pass


class DeepFunctionProbe(FunctionProbe):
    pass


class ExpansionProbe(Probe):

    def __init__(self, is_soft=True, is_hard=True):
        self.is_soft = is_soft
        self.is_hard = is_hard


class GuessNameProbe(Probe):
    pass


class DirectionProbe(Probe):
    pass


class Lookup(Adapter):

    adapts(Binding, Probe)

    def __init__(self, binding, probe):
        assert isinstance(binding, Binding)
        assert isinstance(probe, Probe)
        self.binding = binding
        self.probe = probe
        # Get the database metadata.  Check if it was loaded before;
        # if not, load it from the database and save it as an application
        # attribute.  FIXME: this is definitely not the place where
        # it should be done.
        app = context.app
        if app.cached_catalog is None:
            introspect = Introspect()
            catalog = introspect()
            app.cached_catalog = catalog
        self.catalog = app.cached_catalog

    def __call__(self):
        return None


class LookupDeep(Lookup):

    adapts_many((HomeBinding, DeepAttributeProbe),
                (HomeBinding, DeepFunctionProbe),
                (TableBinding, DeepAttributeProbe),
                (TableBinding, DeepFunctionProbe),
                (ColumnBinding, DeepAttributeProbe),
                (ColumnBinding, DeepFunctionProbe),
                (QuotientBinding, DeepAttributeProbe),
                (QuotientBinding, DeepFunctionProbe),
                (ComplementBinding, DeepAttributeProbe),
                (ComplementBinding, DeepFunctionProbe),
                (MonikerBinding, DeepAttributeProbe),
                (MonikerBinding, DeepFunctionProbe),
                (ForkBinding, DeepAttributeProbe),
                (ForkBinding, DeepFunctionProbe),
                (LinkBinding, DeepAttributeProbe),
                (LinkBinding, DeepFunctionProbe))

    def __call__(self):
        recipe = super(LookupDeep, self).__call__()
        if recipe is not None:
            return recipe
        if self.binding is not self.binding.base:
            recipe = lookup(self.binding.base, self.probe)
            if recipe is not None:
                return PinnedRecipe(self.binding.base, recipe)
        return None


class LookupReferenceInReverseRedirect(Lookup):

    adapts(ReverseRedirectBinding, ReferenceProbe)

    def __call__(self):
        return lookup(self.binding.pointer, self.probe)


class LookupDeepAttributeInReverseRedirect(Lookup):

    adapts(ReverseRedirectBinding, DeepAttributeProbe)

    def __call__(self):
        probe = self.probe.clone_to(AttributeProbe)
        recipe = lookup(self.binding.base, probe)
        if recipe is not None:
            return recipe
        return lookup(self.binding.pointer, self.probe)


class LookupDeepFunctionInReverseRedirect(Lookup):

    adapts(ReverseRedirectBinding, DeepFunctionProbe)

    def __call__(self):
        probe = self.probe.clone_to(FunctionProbe)
        recipe = lookup(self.binding.base, probe)
        if recipe is not None:
            return recipe
        return lookup(self.binding.pointer, self.probe)


class LookupAttributeInFlat(Lookup):

    adapts(FlatBinding, AttributeProbe)

    def __call__(self):
        probe = self.probe.clone_to(DeepAttributeProbe)
        return lookup(self.binding.base, probe)


class LookupFunctionInFlat(Lookup):

    adapts(FlatBinding, FunctionProbe)

    def __call__(self):
        probe = self.probe.clone_to(DeepFunctionProbe)
        return lookup(self.binding.base, probe)


class LookupAttributeInHome(Lookup):
    """
    Finds an attribute with the given name in the root context.

    Members of the root context are free tables (that is,
    they give rise to :class:`htsql.tr.binding.FreeTableBinding` instances).
    """

    adapts(HomeBinding, AttributeProbe)

    def __call__(self):
        # Check if we could find a table with the given name.
        recipe = self.lookup_table()
        if recipe is not None:
            return recipe
        # No luck, report that we cannot find a member with the given name.
        return None

    def lookup_table(self):
        # Find all tables which normalized name coincides with the normalized
        # identifier.
        candidates = []
        # FIXME: very inefficient.  We could either build and cache
        # a mapping: normalized name -> list of matching tables, or cache
        # the result of the lookup operation (the parameters of the binding
        # constructor).
        for schema in self.catalog.schemas:
            for table in schema.tables:
                if normalize(table.name) == self.probe.key:
                    candidates.append(table)
        # Keep only the schemas with the highest priority.
        if candidates:
            priority = max(self.catalog.schemas[table.schema_name].priority
                           for table in candidates)
            candidates = [table
                          for table in candidates
                          if self.catalog.schemas[table.schema_name].priority
                                                                == priority]
        # If we find one and only one matching table, generate a binding
        # node for it.
        if len(candidates) == 1:
            table = candidates[0]
            return FreeTableRecipe(table)
        if len(candidates) > 1:
            return AmbiguousRecipe()


class ExpandHome(Lookup):

    adapts(HomeBinding, ExpansionProbe)

    def __call__(self):
        if self.probe.is_hard:
            return []
        return None


class LookupInTable(Lookup):

    adapts(TableBinding, Probe)

    def find_link(self, column):
        # Determines if the column may represents a link to another table.
        # This is the case when the column is associated with a foreign key.

        # Get a list of foreign keys associated with the given column.
        candidates = []
        schema = self.catalog.schemas[column.schema_name]
        table = schema.tables[column.table_name]
        for fk in table.foreign_keys:
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


class LookupAttributeInTable(Lookup):
    """
    Finds a member with the given name in a table context.

    A table context contains three types of members:

    - table columns;

    - referenced tables, i.e., tables for which there exists a foreign
      key from the context table;

    - referencing tables, i.e., tables with a foreign key to the
      context table.

    Column members give rise to :class:`ColumnBinding` instances
    while table members give rise to :class:`AttachedTableBinding` instances.
    """

    adapts(TableBinding, AttributeProbe)

    def __call__(self):
        # Check if we could find a column with the given name.
        recipe = self.lookup_column()
        if recipe is not None:
            return recipe
        # If not, check for a referenced table with the given name.
        recipe = self.lookup_direct_join()
        if recipe is not None:
            return recipe
        # Finally, check for a referencing table with the given name.
        recipe = self.lookup_reverse_join()
        if recipe is not None:
            return recipe
        # We are out of luck.
        return None

    def lookup_column(self):
        # Finds a column with given name in the context table.

        table = self.binding.table
        # Columns matching the given identifier.  Since we are comparing
        # normalized names, there is a (very small) possibility there are
        # more than one column matching the same name.
        candidates = []
        # FIXME: not very efficient.
        for column in table.columns:
            if normalize(column.name) == self.probe.key:
                candidates.append(column)
        # FIXME: if there are more than one candidate, we should stop
        # the lookup process instead of passing to the next step.
        if len(candidates) == 1:
            # We found a matching column, generate the corresponding
            # binding node.
            column = candidates[0]
            link = self.find_link(column)
            return ColumnRecipe(column, link)
        if len(candidates) > 1:
            return AmbiguousRecipe()

    def lookup_direct_join(self):
        # Finds a table referenced from the context table that matches
        # the given identifier.

        origin = self.binding.table
        # Candidates are foreign keys with the context table as the origin
        # and a table matching the given name as the target.
        candidates = []
        for foreign_key in origin.foreign_keys:
            if normalize(foreign_key.target_name) == self.probe.key:
                candidates.append(foreign_key)
        # FIXME: if there are more than one candidate, we should stop
        # the lookup process instead of passing to the next step.
        if len(candidates) == 1:
            # We found exactly one matching foreign key, generate the
            # corresponding binding node.
            foreign_key = candidates[0]
            target_schema = self.catalog.schemas[foreign_key.target_schema_name]
            target = target_schema.tables[foreign_key.target_name]
            join = DirectJoin(origin, target, foreign_key)
            return AttachedTableRecipe([join])
        if len(candidates) > 1:
            return AmbiguousRecipe()

    def lookup_reverse_join(self):
        # Finds a table with the given name that possesses a foreign key
        # referencing the context table.

        # The origin of the reverse join (but the target of the foreign key).
        origin = self.binding.table
        # List of foreign keys targeting the context table which referencing
        # table matches the given identifier.
        candidates = []
        # Go through all tables matching the identifier.
        # FIXME: very inefficient.
        for target_schema in self.catalog.schemas:
            for target in target_schema.tables:
                if normalize(target.name) != self.probe.key:
                    continue
                # Add all foreign keys referencing the context table to the
                # list of candidates.
                for foreign_key in target.foreign_keys:
                    if (foreign_key.target_schema_name == origin.schema_name
                            and foreign_key.target_name == origin.name):
                        candidates.append(foreign_key)
        if len(candidates) == 1:
            # We found exactly one matching foreign key, generate the
            # corresponding binding node.
            foreign_key = candidates[0]
            target_schema = self.catalog.schemas[foreign_key.origin_schema_name]
            target = target_schema.tables[foreign_key.origin_name]
            join = ReverseJoin(origin, target, foreign_key)
            return AttachedTableRecipe([join])
        if len(candidates) > 1:
            return AmbiguousRecipe()


class ExpandTable(Lookup):
    """
    Produces all public members of a table context.

    Public members of a table binding are the columns of the table.
    """

    adapts(TableBinding, ExpansionProbe)

    def __call__(self):
        # Only expand on a hard probe.
        if not self.probe.is_hard:
            return super(ExpandTable, self).__call__()
        # Produce a list of column bindings.
        return self.itemize_columns()

    def itemize_columns(self):
        # Produce a binding for each column of the table.
        for column in self.binding.table.columns:
            # Note that we create a "virtual" syntax node for each column,
            # and only use the `mark` attribute from the original syntax node.
            identifier = IdentifierSyntax(column.name, EmptyMark())
            link = self.find_link(column)
            recipe = ColumnRecipe(column, link)
            yield (identifier, recipe)


class LookupAttributeInColumn(Lookup):
    """
    Finds a member with the given name in a column context.

    A column binding delegates all `Lookup` and `Itemize` requests to
    the corresponding link node.  If there is no associated link node,
    the requests fail.
    """

    adapts(ColumnBinding, AttributeProbe)

    def __call__(self):
        # If there is an associated link node, delegate the request to it.
        if self.binding.link is not None:
            return lookup(self.binding.link, self.probe)
        # Otherwise, no luck.
        return None


class ExpandColumn(Lookup):
    """
    Produces all public members of a column context.

    A column binding delegates all `Lookup` and `Itemize` requests to
    the corresponding link node.  If there is no associated link node,
    the requests fail.
    """

    adapts(ColumnBinding, ExpansionProbe)

    def __call__(self):
        # If there is an associated link node, delegate the request to it.
        if self.binding.link is not None:
            return lookup(self.binding.link, self.probe)
        # Otherwise, no luck.
        return None


class LookupReferenceInChain(Lookup):

    adapts_many((HomeBinding, ReferenceProbe),
                (TableBinding, ReferenceProbe),
                (ColumnBinding, ReferenceProbe),
                (QuotientBinding, ReferenceProbe),
                (ComplementBinding, ReferenceProbe),
                (MonikerBinding, ReferenceProbe),
                (ForkBinding, ReferenceProbe),
                (LinkBinding, ReferenceProbe),
                (KernelBinding, ReferenceProbe))

    def __call__(self):
        if self.binding is not self.binding.base:
            return lookup(self.binding.base, self.probe)


class GuessNameInChain(Lookup):

    adapts_many((HomeBinding, GuessNameProbe),
                (TableBinding, GuessNameProbe),
                (ColumnBinding, GuessNameProbe),
                (QuotientBinding, GuessNameProbe),
                (ComplementBinding, GuessNameProbe),
                (MonikerBinding, GuessNameProbe),
                (ForkBinding, GuessNameProbe),
                (LinkBinding, GuessNameProbe),
                (KernelBinding, GuessNameProbe))

    def __call__(self):
        if isinstance(self.binding.syntax, IdentifierSyntax):
            return self.binding.syntax.value


class LookupInWrapper(Lookup):
    """
    Finds a member with the given name in a wrapper node.

    All requests are delegated to the base node.
    """

    adapts_many((WrapperBinding, Probe),
                (SieveBinding, Probe),
                (SortBinding, Probe),
                (AliasBinding, Probe),
                (DefinitionBinding, Probe),
                (SelectionBinding, Probe))

    def __call__(self):
        # Delegate the request to the base node.
        return lookup(self.binding.base, self.probe)


class GuessNameInWrapper(Lookup):

    adapts_many((WrapperBinding, GuessNameProbe),
                (SieveBinding, GuessNameProbe),
                (SortBinding, GuessNameProbe),
                (AliasBinding, GuessNameProbe),
                (DefinitionBinding, GuessNameProbe),
                (SelectionBinding, GuessNameProbe))

    def __call__(self):
        if isinstance(self.binding.syntax, IdentifierSyntax):
            return self.binding.syntax.value
        return lookup(self.binding.base, self.probe)


class ExpandSelection(Lookup):

    adapts(SelectionBinding, ExpansionProbe)

    def __call__(self):
        if not self.probe.is_soft:
            return lookup(self.binding.base, self.probe)
        return self.itemize()

    def itemize(self):
        for element in self.binding.elements:
            syntax = element.syntax
            recipe = BindingRecipe(element)
            yield (syntax, recipe)


class LookupComplementInQuotient(Lookup):

    adapts(QuotientBinding, ComplementProbe)

    def __call__(self):
        return ComplementRecipe(self.binding.seed)


class ExpandQuotient(Lookup):

    adapts(QuotientBinding, ExpansionProbe)

    def __call__(self):
        if not self.probe.is_hard:
            return
        recipies = []
        for index, binding in enumerate(self.binding.kernel):
            syntax = binding.syntax
            recipe = KernelRecipe(self.binding.kernel, index)
            recipies.append((syntax, recipe))
        return recipies


class LookupInComplement(Lookup):

    adapts(ComplementBinding, Probe)

    def __call__(self):
        return lookup(self.binding.seed, self.probe)


class LookupInMoniker(Lookup):

    adapts(MonikerBinding, Probe)

    def __call__(self):
        return lookup(self.binding.seed, self.probe)


class LookupInFork(Lookup):

    adapts(ForkBinding, Probe)

    def __call__(self):
        return lookup(self.binding.base, self.probe)


class LookupInLink(Lookup):

    adapts(LinkBinding, Probe)

    def __call__(self):
        return lookup(self.binding.seed, self.probe)


class ExpandComplement(Lookup):

    adapts(ComplementBinding, ExpansionProbe)

    def __call__(self):
        if not self.probe.is_hard:
            return None
        probe = self.probe.clone(is_soft=False)
        return lookup(self.binding.seed, probe)


class ExpandLink(Lookup):

    adapts(LinkBinding, ExpansionProbe)

    def __call__(self):
        if not self.probe.is_hard:
            return None
        probe = self.probe.clone(is_soft=False)
        return lookup(self.binding.seed, probe)


class LookupAttributeInAlias(Lookup):

    adapts(AliasBinding, AttributeProbe)

    def __call__(self):
        if (not self.binding.is_reference and
                self.probe.key == normalize(self.binding.name)):
            return self.binding.recipe
        return super(LookupAttributeInAlias, self).__call__()


class LookupReferenceInAlias(Lookup):

    adapts(AliasBinding, ReferenceProbe)

    def __call__(self):
        if (self.binding.is_reference and
                self.probe.key == normalize(self.binding.name)):
            return self.binding.recipe
        return super(LookupReferenceInAlias, self).__call__()


class LookupAttributeInDefinition(Lookup):

    adapts(DefinitionBinding, AttributeProbe)

    def __call__(self):
        if self.probe.key == normalize(self.binding.name):
            if (self.binding.arguments is None or
                    len(self.binding.subnames) > 0):
                return SubstitutionRecipe(self.binding.base,
                                          self.binding.subnames,
                                          self.binding.arguments,
                                          self.binding.body)
        return super(LookupAttributeInDefinition, self).__call__()


class LookupFunctionInDefinition(Lookup):

    adapts(DefinitionBinding, FunctionProbe)

    def __call__(self):
        if self.probe.key == normalize(self.binding.name):
            if (not self.binding.subnames and
                    self.binding.arguments is not None and
                    len(self.binding.arguments) == self.probe.arity):
                return SubstitutionRecipe(self.binding.base,
                                          self.binding.subnames,
                                          self.binding.arguments,
                                          self.binding.body)
        return super(LookupFunctionInDefinition, self).__call__()


class LookupInRedirect(Lookup):

    adapts(RedirectBinding, Probe)

    def __call__(self):
        return lookup(self.binding.pointer, self.probe)


class DirectDirection(Lookup):

    adapts(DirectionBinding, DirectionProbe)

    def __call__(self):
        return self.binding.direction


def lookup(binding, probe):
    lookup = Lookup(binding, probe)
    return lookup()


def lookup_attribute(binding, name):
    probe = AttributeProbe(name)
    return lookup(binding, probe)


def lookup_function(binding, name, arity):
    probe = FunctionProbe(name, arity)
    return lookup(binding, probe)


def lookup_reference(binding, name):
    probe = ReferenceProbe(name)
    return lookup(binding, probe)


def lookup_complement(binding):
    probe = ComplementProbe()
    return lookup(binding, probe)


def expand(binding, is_soft=True, is_hard=True):
    probe = ExpansionProbe(is_soft=is_soft, is_hard=is_hard)
    bindings = lookup(binding, probe)
    if bindings is not None:
        bindings = list(bindings)
    return bindings


def guess_name(binding):
    probe = GuessNameProbe()
    return lookup(binding, probe)


def direct(binding):
    probe = DirectionProbe()
    return lookup(binding, probe)


def entitle(binding):
    pass


