#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


from ..util import maybe, listof
from ..entity import TableEntity, ColumnEntity, Join
from .binding import Binding
from .syntax import Syntax


class Recipe(object):
    pass


class FreeTableRecipe(Recipe):

    def __init__(self, table):
        assert isinstance(table, TableEntity)
        self.table = table


class AttachedTableRecipe(Recipe):

    def __init__(self, joins):
        assert isinstance(joins, listof(Join)) and len(joins) > 0
        self.joins = joins


class ColumnRecipe(Recipe):

    def __init__(self, column, link=None):
        assert isinstance(column, ColumnEntity)
        assert isinstance(link, maybe(AttachedTableRecipe))
        self.column = column
        self.link = link


class ComplementRecipe(Recipe):

    def __init__(self, seed):
        assert isinstance(seed, Binding)
        self.seed = seed


class KernelRecipe(Recipe):

    def __init__(self, kernel, index):
        assert isinstance(kernel, listof(Binding))
        assert isinstance(index, int)
        assert 0 <= index < len(kernel)
        self.kernel = kernel
        self.index = index


class SubstitutionRecipe(Recipe):

    def __init__(self, base, subnames, arguments, body):
        assert isinstance(base, Binding)
        assert isinstance(subnames, listof(str))
        assert isinstance(arguments, maybe(listof(str)))
        assert isinstance(body, Syntax)
        self.base = base
        self.subnames = subnames
        self.body = body
        self.arguments = arguments


class BindingRecipe(Recipe):

    def __init__(self, binding):
        assert isinstance(binding, Binding)
        self.binding = binding


class AmbiguousRecipe(Recipe):
    pass


