#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.lookup`
======================

This module implements name resolution adapters.
"""


from ..adapter import Adapter, adapts, find_adapters
from .binding import (Binding, RootBinding, QueryBinding, SegmentBinding,
                      TableBinding, FreeTableBinding, JoinedTableBinding,
                      ColumnBinding, LiteralBinding, SieveBinding)
from .syntax import Syntax, IdentifierSyntax
from ..error import InvalidArgumentError
from ..entity import DirectJoin, ReverseJoin


class Lookup(Adapter):

    adapts(Binding)

    def __init__(self, binding):
        self.binding = binding

    def __call__(self, identifier):
        return self.lookup(identifier)

    def lookup(self, identifier):
        assert isinstance(identifier, IdentifierSyntax)
        raise InvalidArgumentError("unknown identifier: %s" % identifier,
                                   identifier.mark)

    def enumerate(self, syntax):
        assert isinstance(syntax, Syntax)
        return iter([])


class LookupRoot(Lookup):

    adapts(RootBinding)

    def lookup(self, identifier):
        binding = self.lookup_table(identifier)
        if binding is not None:
            return binding
        return super(LookupRoot, self).lookup(identifier)

    def lookup_table(self, identifier):
        catalog = self.binding.catalog
        candidates = []
        for schema in catalog.schemas:
            for table in schema.tables:
                if table.name == identifier.value:
                    candidates.append(table)
        if len(candidates) > 1:
            raise InvalidArgumentError("ambiguous table name: %s" % identifier,
                                       identifier.mark)
        elif len(candidates) == 1:
            table = candidates[0]
            return FreeTableBinding(self.binding, table, identifier)

    def enumerate(self, syntax):
        raise InvalidArgumentError("unexpected wildcard", syntax.mark)


class LookupTable(Lookup):

    adapts(TableBinding)

    def lookup(self, identifier):
        binding = self.lookup_column(identifier)
        if binding is not None:
            return binding
        binding = self.lookup_direct_join(identifier)
        if binding is not None:
            return binding
        binding = self.lookup_reverse_join(identifier)
        if binding is not None:
            return binding
        return super(LookupTable, self).lookup(identifier)

    def lookup_column(self, identifier):
        table = self.binding.table
        if identifier.value in table.columns:
            column = table.columns[identifier.value]
            return ColumnBinding(self.binding, column, identifier)

    def lookup_direct_join(self, identifier):
        catalog = self.binding.root.catalog
        origin = self.binding.table
        candidates = []
        for foreign_key in origin.foreign_keys:
            if foreign_key.target_name == identifier.value:
                candidates.append(foreign_key)
        if len(candidates) > 1:
            raise InvalidArgumentError("ambiguous table name: %s" % identifier,
                                       identifier.mark)
        if len(candidates) == 1:
            foreign_key = candidates[0]
            target_schema = catalog.schemas[foreign_key.target_schema_name]
            target = target_schema.tables[foreign_key.target_name]
            join = DirectJoin(origin, target, foreign_key)
            return JoinedTableBinding(self.binding, target, [join],
                                      identifier)

    def lookup_reverse_join(self, identifier):
        catalog = self.binding.root.catalog
        origin = self.binding.table
        candidates = []
        for schema in catalog.schemas:
            if identifier.value not in schema.tables:
                continue
            target = schema.tables[identifier.value]
            for foreign_key in target.foreign_keys:
                if (foreign_key.target_schema_name == origin.schema_name
                        and foreign_key.target_name == origin.name):
                    candidates.append(foreign_key)
        if len(candidates) > 1:
            raise InvalidArgumentError("ambiguous table name: %s" % identifier,
                                       identifier.mark)
        if len(candidates) == 1:
            foreign_key = candidates[0]
            target_schema = catalog.schemas[foreign_key.origin_schema_name]
            target = target_schema.tables[foreign_key.origin_name]
            join = ReverseJoin(origin, target, foreign_key)
            return JoinedTableBinding(self.binding, target, [join],
                                      identifier)

    def enumerate(self, syntax):
        for binding in self.enumerate_columns(syntax):
            yield binding
        for binding in super(LookupTable, self).enumerate(syntax):
            yield binding

    def enumerate_columns(self, syntax):
        for column in self.binding.table.columns:
            identifier = IdentifierSyntax(column.name, syntax.mark)
            yield ColumnBinding(self.binding, column, identifier)


class LookupColumn(Lookup):

    adapts(ColumnBinding)

    def lookup(self, identifier):
        binding = self.as_table()
        if binding is not None:
            lookup = Lookup(binding)
            return lookup(identifier)
        return super(LookupColumn, self).lookup(identifier)

    def enumerate(self, syntax):
        binding = self.as_table()
        if binding is not None:
            lookup = Lookup(binding)
            return lookup.enumerate(syntax)
        return super(LookupColumn, self).enumerate(identifier)

    def as_table(self):
        catalog = self.binding.root.catalog
        column = self.binding.column
        candidates = []
        queue = [([], self.binding.column)]
        while queue:
            path, column = queue.pop(0)
            schema = catalog.schemas[column.schema_name]
            table = schema.tables[column.table_name]
            for fk in table.foreign_keys:
                if column.name not in fk.origin_column_names:
                    continue
                target_schema = catalog.schemas[fk.target_schema_name]
                target = target_schema.tables[fk.target_name]
                index = fk.origin_column_names.index(column.name)
                target_column_name = fk.target_column_names[index]
                target_column = target.columns[target_column_name]
                candidate = path+[fk]
                candidates.append(candidate)
                queue.append((candidates, target_column))
        max_length = max(len(candidate) for candidate in candidates)
        candidates = [candidate for candidate in candidates
                                if len(candidate) == max_length]
        if len(candidates) > 1:
            raise InvalidArgumentError("ambiguous reference",
                                       self.binding.mark)
        if len(candidates) == 1:
            foreign_keys = candidates[0]
            joins = []
            for fk in foreign_keys:
                origin_schema = catalog.schemas[fk.origin_schema_name]
                origin = catalog.schemas[fk.origin_name]
                target_schema = catalog.schemas[fk.target_schema_name]
                target = target_schema.tables[fk.target_name]
                join = DirectJoin(origin, target, fk)
                joins.append(join)
            return JoinedTableBinding(self.binding, target, joins,
                                      self.binding.syntax)


class ProxyMixin(object):

    def lookup(self, identifier):
        lookup = Lookup(self.binding.parent)
        binding = lookup(identifier)
        binding = self.replace_parent(binding)
        return binding

    def enumerate(self, syntax):
        lookup = Lookup(self.binding.parent)
        for binding in lookup.enumerate(syntax):
            binding = self.replace_parent(binding)
            yield binding

    def replace_parent(self, binding):
        if binding.parent is self.binding.parent:
            return binding.clone(parent=self.binding)
        if binding is binding.root:
            return binding
        parent = self.replace_parent(binding.parent)
        if parent is not binding.parent:
            binding = binding.clone(parent=parent)
        return binding


class LookupSieve(ProxyMixin, Lookup):

    adapts(SieveBinding)


lookup_adapters = find_adapters()


