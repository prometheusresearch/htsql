#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


from htsql.tr.lookup import LookupRoot, normalize
from htsql.tr.recipe import FreeTableRecipe, AmbiguousRecipe


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


