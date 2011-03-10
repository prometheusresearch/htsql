#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


from htsql.mark import EmptyMark
from htsql.tr.lookup import ItemizeTable
from htsql.tr.syntax import IdentifierSyntax
from htsql.tr.recipe import ColumnRecipe


class OracleItemizeTable(ItemizeTable):

    def itemize_columns(self):
        for column in self.binding.table.columns:
            name = column.name
            if name.isupper():
                name = name.lower()
            identifier = IdentifierSyntax(name, EmptyMark())
            link = self.find_link(column)
            recipe = ColumnRecipe(column, link)
            yield (identifier, recipe)


