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


from ..adapter import Adapter, adapts, adapts_many
from ..context import context
from ..introspect import Introspect
from ..entity import DirectJoin, ReverseJoin
from .syntax import Syntax, IdentifierSyntax
from .binding import (Binding, RootBinding, ChainBinding,
                      TableBinding, FreeTableBinding, AttachedTableBinding,
                      ColumnBinding, SieveBinding, WrapperBinding, SortBinding,
                      QuotientBinding, ComplementBinding, KernelBinding,
                      DefinitionBinding, AliasBinding)
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


class LookupItemizeMixin(object):
    """
    Encapsulates common methods and attributes of the :class:`Lookup`
    and :class:`Itemize` adapters.

    This is a mixin class; see subclasses for concrete adapters.

    Attributes:

    `binding` (:class:`htsql.tr.binding.Binding`)
        The lookup context.

    `catalog` (:class:`htsql.entity.CatalogEntity`)
        The database metadata.
    """

    # FIXME: a way to unite `Lookup` and `Itemize` adapters in a single class?

    def __init__(self, binding):
        # This is the lookup context to which both `Lookup` and `
        self.binding = binding
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
        # By default, both `Lookup` and `Itemize` adapters return ``None``,
        # which means the operation is not applicable to the given binding.
        return None


class Lookup(Adapter, LookupItemizeMixin):
    """
    Looks for a member with the given name in the specified binding.

    This is an iterface adapter; see subclasses for implementations.

    The :class:`Lookup` adapter has the following signature::

        Lookup: (Binding, IdentifierSyntax) -> maybe(ChainBinding)

    The first argument is a binding node that serves as a lookup context;
    it is the polymorphic argument of the adapter.  The adapter finds
    a member of the specified binding that matches the given identifier
    and generates a corresponding binding node.  If there are no members
    matching the identifier, or the `Lookup` operation is not applicable
    to the specified binding, ``None`` is returned.

    `binding` (:class:`htsql.tr.binding.Binding`)
        The lookup context.

    `identifier` (:class:`htsql.tr.syntax.IdentifierSyntax`)
        The identifier to look for.
    """

    adapts(Binding)

    def __init__(self, binding, identifier):
        assert isinstance(identifier, IdentifierSyntax)
        super(Lookup, self).__init__(binding)
        self.identifier = identifier
        # We are going to seek objects which normalized name coincides
        # with the normalized (basically, converted to lower case) identifier.
        # This is the value of the normalized identifier.
        self.key = normalize(identifier.value)


class Itemize(Adapter, LookupItemizeMixin):
    """
    Produces all public members of the specified binding.

    This is an iterface adapter; see subclasses for implementations.

    The :class:`Itemize` adapter has the following signature::

        Itemize: (Binding, Syntax) -> maybe((ChainBinding, ...))

    The first argument is a binding node that serves as a lookup context;
    it is the polymorphic argument of the adapter.  The second argument
    is only used for cosmetic purposes, it has no semantic meaning.
    The adapter generates a sequence of bindings corresponding to all
    public members of the given lookup context.  If the `Itemize` operation
    is not applicable to the specified binding, ``None`` is returned.

    `binding` (:class:`htsql.tr.binding.Binding`)
        The lookup context.

    `syntax` (:class:`htsql.tr.syntax.Syntax`)
        The syntax node that caused the operation.
    """

    adapts(Binding)

    def __init__(self, binding, syntax):
        assert isinstance(syntax, Syntax)
        super(Itemize, self).__init__(binding)
        self.syntax = syntax


class GetComplement(Adapter):

    adapts(Binding)

    def __init__(self, binding, syntax):
        assert isinstance(binding, Binding)
        assert isinstance(syntax, Syntax)
        self.binding = binding
        self.syntax = syntax

    def __call__(self):
        return None


class GetKernel(Adapter):

    adapts(Binding)

    def __init__(self, binding, syntax):
        assert isinstance(binding, Binding)
        assert isinstance(syntax, Syntax)
        self.binding = binding
        self.syntax = syntax

    def __call__(self):
        return None


class LookupRoot(Lookup):
    """
    Finds a member with the given name in the root context.

    Members of the root context are free tables (that is,
    they give rise to :class:`htsql.tr.binding.FreeTableBinding` instances).
    """

    adapts(RootBinding)

    def __call__(self):
        # Check if we could find a table with the given name.
        binding = self.lookup_table()
        if binding is not None:
            return binding
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
                if normalize(table.name) == self.key:
                    candidates.append(table)
        # If we find one and only one matching table, generate a binding
        # node for it.
        if len(candidates) == 1:
            table = candidates[0]
            return FreeTableBinding(self.binding, table, self.identifier)


class LookupItemizeTableMixin(object):
    """
    Encapsulates common operations between `Lookup` and `Itemize` adapters
    over :class:`htsql.tr.binding.TableBinding`.
    """

    def find_link(self, column, syntax):
        # Determines if the column represents a link to another table.

        # A column may represent another table if it is a foreign key or
        # a part of a multi-column foreign key.  Then the column represents
        # the referenced table chained to the referencing table by the
        # join condition imposed by the key.  Moreover, if the referenced
        # column is also part of some foreign key, the link could be extended
        # include the next referenced table.
        #
        # More formally, consider a directed graph with vertices corresponding
        # to table columns and arcs corresponding to foreign keys.  A single
        # column foreign key generates a single arc between the referencing
        # and the referenced columns, while a multi-column foreign key
        # generates an arc per each pair of referencing and referenced columns.
        #
        # Each path in the graph represents a link between two tables:
        # the table of the start column and the table of the end column.
        # The tables are linked by the join conditions corresponding to
        # the foreign keys that compose the path.
        #
        # If for the given column, there are no outgoing arcs, the column
        # does not represent a link.  Otherwise consider all paths starting
        # from the given column vertex.  If there exists a unique longest
        # path, it is used to generate the link; otherwise it is assumed
        # that the link cannot be established without ambiguity.

        # A list of chains of foreign keys that originate in the given column.
        candidates = []
        # A list of pairs `(path, column)` where `path` represents a chain
        # of foreign keys and `column` is the referenced column of the trailing
        # foreign key.  The list contains chains which we will try to extend.
        # We start with an empty path originated from the given column.
        queue = [([], column)]
        # Continue while we have potential chains to extend.
        while queue:
            # Get a potential chain.  We are going to find all its extensions.
            path, column = queue.pop(0)
            # Get the table entity of the target column.
            schema = self.catalog.schemas[column.schema_name]
            table = schema.tables[column.table_name]
            # Go through all the foreign keys that include the column.
            for fk in table.foreign_keys:
                if column.name not in fk.origin_column_names:
                    continue
                # Find the referenced column.
                target_schema = self.catalog.schemas[fk.target_schema_name]
                target = target_schema.tables[fk.target_name]
                idx = fk.origin_column_names.index(column.name)
                target_column_name = fk.target_column_names[idx]
                target_column = target.columns[target_column_name]
                # Ignore the case whan the column points to itself.  This may
                # actually happen when the foreign key is multicolumn
                # self-referential link.
                if target_column is column:
                    continue
                # We got a chain extension.  Add it to the list of candidate
                # chains and to the queue of potentially extendable chains.
                candidate = path+[fk]
                candidates.append(candidate)
                queue.append((candidate, target_column))

        # Return immediately if there are no candidate chains.
        if not candidates:
            return None
        # Leave only the longest chains.
        max_length = max(len(candidate) for candidate in candidates)
        candidates = [candidate for candidate in candidates
                                if len(candidate) == max_length]

        # If there's only one longest chain, we got our link.
        if len(candidates) == 1:
            # Generate the link joins.
            foreign_keys = candidates[0]
            joins = []
            for fk in foreign_keys:
                origin_schema = self.catalog.schemas[fk.origin_schema_name]
                origin = origin_schema.tables[fk.origin_name]
                target_schema = self.catalog.schemas[fk.target_schema_name]
                target = target_schema.tables[fk.target_name]
                join = DirectJoin(origin, target, fk)
                joins.append(join)
            # Build and return the link binding.
            return AttachedTableBinding(self.binding, target, joins, syntax)


class LookupTable(Lookup, LookupItemizeTableMixin):
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

    adapts(TableBinding)

    def __call__(self):
        # Check if we could find a column with the given name.
        binding = self.lookup_column()
        if binding is not None:
            return binding
        # If not, check for a referenced table with the given name.
        binding = self.lookup_direct_join()
        if binding is not None:
            return binding
        # Finally, check for a referencing table with the given name.
        binding = self.lookup_reverse_join()
        if binding is not None:
            return binding
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
            if normalize(column.name) == self.key:
                candidates.append(column)
        # FIXME: if there are more than one candidate, we should stop
        # the lookup process instead of passing to the next step.
        if len(candidates) == 1:
            # We found a matching column, generate the corresponding
            # binding node.
            column = candidates[0]
            link = self.find_link(column, self.identifier)
            return ColumnBinding(self.binding, column, link,
                                 self.identifier)

    def lookup_direct_join(self):
        # Finds a table referenced from the context table that matches
        # the given identifier.

        origin = self.binding.table
        # Candidates are foreign keys with the context table as the origin
        # and a table matching the given name as the target.
        candidates = []
        for foreign_key in origin.foreign_keys:
            if normalize(foreign_key.target_name) == self.key:
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
            return AttachedTableBinding(self.binding, target, [join],
                                        self.identifier)

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
                if normalize(target.name) != self.key:
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
            return AttachedTableBinding(self.binding, target, [join],
                                        self.identifier)


class ItemizeTable(Itemize, LookupItemizeTableMixin):
    """
    Produces all public members of a table context.

    Public members of a table binding are the columns of the table.
    """

    adapts(TableBinding)

    def __call__(self):
        # Produce a list of column bindings.
        return self.itemize_columns()

    def itemize_columns(self):
        # Produce a binding for each column of the table.
        for column in self.binding.table.columns:
            # Note that we create a "virtual" syntax node for each column,
            # and only use the `mark` attribute from the original syntax node.
            identifier = IdentifierSyntax(column.name, self.syntax.mark)
            link = self.find_link(column, identifier)
            yield ColumnBinding(self.binding, column, link, identifier)


class LookupColumn(Lookup):
    """
    Finds a member with the given name in a column context.

    A column binding delegates all `Lookup` and `Itemize` requests to
    the corresponding link node.  If there is no associated link node,
    the requests fail.
    """

    adapts(ColumnBinding)

    def __call__(self):
        # If there is an associated link node, delegate the request to it.
        if self.binding.link is not None:
            return self.lookup_link()
        # Otherwise, no luck.
        return None

    def lookup_link(self):
        # Delegate the request to the associated link.
        binding = lookup(self.binding.link, self.identifier)
        # Reparent the result to the original node.
        if binding is not None:
            assert isinstance(binding, ChainBinding)
            assert binding.base is self.binding.link
            return binding.clone(base=self.binding)


class ItemizeColumn(Itemize):
    """
    Produces all public members of a column context.

    A column binding delegates all `Lookup` and `Itemize` requests to
    the corresponding link node.  If there is no associated link node,
    the requests fail.
    """

    adapts(ColumnBinding)

    def __call__(self):
        # If there is an associated link node, delegate the request to it.
        if self.binding.link is not None:
            return self.itemize_link()
        # Otherwise, no luck.
        return None

    def itemize_link(self):
        # Delegate the request to the associated link.
        bindings = itemize(self.binding.link, self.syntax)
        if bindings is None:
            return None
        # Reparent the produced binding nodes to the context node.
        assert all(isinstance(binding, ChainBinding) and
                   binding.base is self.binding.link
                   for binding in bindings)
        return (binding.clone(base=self.binding) for binding in bindings)


class LookupWrapper(Lookup):
    """
    Finds a member with the given name in a wrapper node.

    All requests are delegated to the base node.
    """

    adapts_many(WrapperBinding,
                SieveBinding,
                SortBinding)

    def __call__(self):
        # Delegate the request to the base node.
        binding = lookup(self.binding.base, self.identifier)
        # Reparent the result to the original node.
        if binding is not None:
            assert isinstance(binding, ChainBinding)
            assert binding.base is self.binding.base
            return binding.clone(base=self.binding)


class ItemizeWrapper(Itemize):
    """
    Produces all public members of a wrapper node.

    All requests are delegated to the base node.
    """

    adapts_many(WrapperBinding,
                SieveBinding,
                SortBinding,
                DefinitionBinding)

    def __call__(self):
        # Delegate the request to the base node.
        bindings = itemize(self.binding.base, self.syntax)
        if bindings is None:
            return None
        # Reparent the produced binding nodes to the context node.
        assert all(isinstance(binding, ChainBinding) and
                   binding.base is self.binding.base
                   for binding in bindings)
        return (binding.clone(base=self.binding) for binding in bindings)


class GetComplementFromWrapper(GetComplement):

    adapts_many(WrapperBinding,
                SieveBinding,
                SortBinding,
                DefinitionBinding)

    def __call__(self):
        binding = get_complement(self.binding.base, self.syntax)
        if binding is None:
            return None
        assert isinstance(binding, ChainBinding)
        assert binding.base is self.binding.base
        return binding.clone(base=self.binding)


class GetKernelFromWrapper(GetKernel):

    adapts_many(WrapperBinding,
                SieveBinding,
                SortBinding,
                DefinitionBinding)

    def __call__(self):
        bindings = get_kernel(self.binding.base, self.syntax)
        if bindings is None:
            return None
        assert all(isinstance(binding, ChainBinding) and
                   binding.base is self.binding.base
                   for binding in bindings)
        return (binding.clone(base=self.binding) for binding in bindings)


class ItemizeQuotient(Itemize):

    adapts(QuotientBinding)

    def __call__(self):
        return get_kernel(self.binding, self.syntax)


class GetComplementFromQuotient(GetComplement):

    adapts(QuotientBinding)

    def __call__(self):
        seed = self.binding.seed
        return ComplementBinding(self.binding, seed,
                                 seed.syntax.clone(mark=self.syntax.mark))


class GetKernelFromQuotient(GetKernel):

    adapts(QuotientBinding)

    def __call__(self):
        for index, binding in enumerate(self.binding.kernel):
            yield KernelBinding(self.binding, index, binding.domain,
                                binding.syntax.clone(mark=self.syntax.mark))


class LookupComplement(Lookup):

    adapts(ComplementBinding)

    def __call__(self):
        binding = lookup(self.binding.seed, self.identifier)
        if binding is None:
            return None
        assert isinstance(binding, ChainBinding)
        assert binding.base is self.binding.seed
        return binding.clone(base=self.binding)


class ItemizeComplement(Itemize):

    adapts(ComplementBinding)

    def __call__(self):
        bindings = itemize(self.binding.seed, self.syntax)
        if bindings is None:
            return None
        assert all(isinstance(binding, ChainBinding) and
                   binding.base is self.binding.seed
                   for binding in bindings)
        return (binding.clone(base=self.binding) for binding in bindings)


class GetComplementFromComplement(GetComplement):

    adapts(ComplementBinding)

    def __call__(self):
        binding = get_complement(self.binding.seed, self.syntax)
        if binding is None:
            return None
        assert isinstance(binding, ChainBinding)
        assert binding.base is self.binding.seed
        return binding.clone(base=self.binding)


class GetKernelFromComplement(GetKernel):

    adapts(ComplementBinding)

    def __call__(self):
        bindings = get_complement(self.binding.seed, self.syntax)
        if bindings is None:
            return None
        assert all(isinstance(binding, ChainBinding) and
                   binding.base is self.binding.seed
                   for binding in bindings)
        return (binding.clone(base=self.binding) for binding in bindings)


class LookupDefinition(LookupWrapper):

    adapts(DefinitionBinding)

    def __call__(self):
        if self.key == normalize(self.binding.name):
            return AliasBinding(self.binding, self.binding.binding,
                                self.identifier)
        return super(LookupDefinition, self).__call__()


def lookup(binding, identifier):
    """
    Looks for a member of the specified binding that matches
    the given identifier.

    The function returns an instance of :class:`htsql.tr.binding.ChainBinding`
    attached to the given binding, or ``None`` if no matching members were
    found.

    `binding` (:class:`htsql.tr.binding.Binding`)
        The lookup context.

    `identifier` (:class:`htsql.tr.syntax.IdentifierSyntax`)
        The identifier to look for.
    """
    # Realize and apply the `Lookup` adapter.
    lookup = Lookup(binding, identifier)
    binding = lookup()
    return binding


def itemize(binding, syntax):
    """
    Produces all public members of the given binding.

    The function returns a list of :class:`htsql.tr.binding.ChainBinding`
    instances attached to the given binding, or ``None`` if the operation
    is not applicable to the given binding.

    `binding` (:class:`htsql.tr.binding.Binding`)
        The lookup context.

    `syntax` (:class:`htsql.tr.syntax.Syntax`)
        The syntax node that caused the operation.
    """
    # Realize and apply the `Itemize` adapter.
    itemize = Itemize(binding, syntax)
    bindings = itemize()
    # Convert a generator to a regular list.
    if bindings is not None:
        bindings = list(bindings)
    return bindings


class LookupAlias(Lookup):

    adapts(AliasBinding)

    def __call__(self):
        binding = lookup(self.binding.binding, self.identifier)
        if binding is not None:
            assert isinstance(binding, ChainBinding)
            assert binding.base is self.binding.binding
            return binding.clone(base=self.binding)


class ItemizeAlias(Itemize):

    adapts(AliasBinding)

    def __call__(self):
        bindings = itemize(self.binding.binding, self.syntax)
        if bindings is None:
            return None
        assert all(isinstance(binding, ChainBinding) and
                   binding.base is self.binding.binding
                   for binding in bindings)
        return (binding.clone(base=self.binding) for binding in bindings)


class GetComplementFromAlias(GetComplement):

    adapts(AliasBinding)

    def __call__(self):
        binding = get_complement(self.binding.binding, self.syntax)
        if binding is None:
            return None
        assert isinstance(binding, ChainBinding)
        assert binding.base is self.binding.binding
        return binding.clone(base=self.binding)


class GetKernelFromAlias(GetKernel):

    adapts(AliasBinding)

    def __call__(self):
        bindings = get_kernel(self.binding.binding, self.syntax)
        if bindings is None:
            return None
        assert all(isinstance(binding, ChainBinding) and
                   binding.base is self.binding.binding
                   for binding in bindings)
        return (binding.clone(base=self.binding) for binding in bindings)


def get_complement(binding, syntax):
    get_complement = GetComplement(binding, syntax)
    binding = get_complement()
    return binding


def get_kernel(binding, syntax):
    get_kernel = GetKernel(binding, syntax)
    bindings = get_kernel()
    if bindings is not None:
        bindings = list(bindings)
    return bindings


