#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.tr.lookup import ExpandTable
from htsql.tr.syntax import IdentifierSyntax
from htsql.tr.binding import ColumnRecipe


class OracleExpandTable(ExpandTable):

    def itemize_columns(self):
        for column in self.binding.table.columns:
            name = column.name
            if name.isupper():
                name = name.lower()
            identifier = IdentifierSyntax(name, self.binding.mark)
            link = self.find_link(column)
            recipe = ColumnRecipe(column, link)
            yield (identifier, recipe)


