#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


from htsql.entity import DirectJoin, ReverseJoin
from htsql.tr.lookup import LookupRoot, LookupTable, normalize
from htsql.tr.recipe import (FreeTableRecipe, AttachedTableRecipe,
                             AmbiguousRecipe)


class SchemaLookupRoot(LookupRoot):

    def __call__(self):
        recipe = super(SchemaLookupRoot, self).__call__()
        if recipe is not None:
            return recipe
        recipe = self.lookup_schema_table()
        if recipe is not None:
            return recipe
        return None

    def lookup_schema_table(self):
        candidates = []
        for schema in self.catalog.schemas:
            for table in schema.tables:
                if normalize(schema.name+'_'+table.name) == self.key:
                    candidates.append(table)
        if len(candidates) == 1:
            table = candidates[0]
            return FreeTableRecipe(table)
        if len(candidates) > 1:
            return AmbiguousRecipe()


class SchemaLookupTable(LookupTable):

    def lookup_direct_join(self):
        recipe = super(SchemaLookupTable, self).lookup_direct_join()
        if recipe is not None:
            return recipe
        origin = self.binding.table
        candidates = []
        for foreign_key in origin.foreign_keys:
            name = foreign_key.target_schema_name+'_'+foreign_key.target_name
            if normalize(name) == self.key:
                candidates.append(foreign_key)
        if len(candidates) == 1:
            foreign_key = candidates[0]
            target_schema = self.catalog.schemas[foreign_key.target_schema_name]
            target = target_schema.tables[foreign_key.target_name]
            join = DirectJoin(origin, target, foreign_key)
            return AttachedTableRecipe([join])
        if len(candidates) > 1:
            return AmbiguousRecipe()

    def lookup_reverse_join(self):
        recipe = super(SchemaLookupTable, self).lookup_reverse_join()
        if recipe is not None:
            return recipe
        origin = self.binding.table
        candidates = []
        for target_schema in self.catalog.schemas:
            for target in target_schema.tables:
                name = target.schema_name+'_'+target.name
                if normalize(name) != self.key:
                    continue
                for foreign_key in target.foreign_keys:
                    if (foreign_key.target_schema_name == origin.schema_name
                            and foreign_key.target_name == origin.name):
                        candidates.append(foreign_key)
        if len(candidates) == 1:
            foreign_key = candidates[0]
            target_schema = self.catalog.schemas[foreign_key.origin_schema_name]
            target = target_schema.tables[foreign_key.origin_name]
            join = ReverseJoin(origin, target, foreign_key)
            return AttachedTableRecipe([join])
        if len(candidates) > 1:
            return AmbiguousRecipe()


