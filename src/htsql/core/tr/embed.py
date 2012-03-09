#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ..adapter import Adapter, adapt, adapt_many
from ..domain import (UntypedDomain, BooleanDomain, IntegerDomain, FloatDomain,
                      DecimalDomain, DateDomain, TimeDomain, DateTimeDomain)
from .binding import LiteralRecipe, SelectionRecipe
import types
import datetime
import decimal


class Embed(Adapter):

    adapt(object)

    def __init__(self, value):
        self.value = value

    def __call__(self):
        raise ValueError("unable to embed a value of type %s"
                         % type(self.value))


class EmbedUntyped(Embed):

    adapt_many(str, unicode)

    def __call__(self):
        value = self.value
        if isinstance(value, str):
            try:
                value = value.decode('utf-8')
            except UnicodeDecodeError:
                raise ValueError("a string is expected to be encoded in UTF-8:"
                                 " %s" % repr(value))
        if u"\0" in value:
            raise ValueError("a string should not contain a NIL character:"
                             " %s" % repr(value))
        return LiteralRecipe(value, UntypedDomain())


class EmbedNull(Embed):

    adapt(types.NoneType)

    def __call__(self):
        return LiteralRecipe(None, UntypedDomain())


class EmbedBoolean(Embed):

    adapt(bool)

    def __call__(self):
        return LiteralRecipe(self.value, BooleanDomain())


class EmbedInteger(Embed):

    adapt_many(int, long)

    def __call__(self):
        return LiteralRecipe(self.value, IntegerDomain())


class EmbedFloat(Embed):

    adapt(float)

    def __call__(self):
        return LiteralRecipe(self.value, FloatDomain())


class EmbedDecimal(Embed):

    adapt(decimal.Decimal)

    def __call__(self):
        return LiteralRecipe(self.value, DecimalDomain())


class EmbedDate(Embed):

    adapt(datetime.date)

    def __call__(self):
        return LiteralRecipe(self.value, DateDomain())


class EmbedTime(Embed):

    adapt(datetime.time)

    def __call__(self):
        return LiteralRecipe(self.value, TimeDomain())


class EmbedDateTime(Embed):

    adapt(datetime.datetime)

    def __call__(self):
        return LiteralRecipe(self.value, DateTimeDomain())


class EmbedList(Embed):

    adapt_many(list, tuple)

    def __call__(self):
        recipes = []
        for value in self.value:
            recipe = embed(value)
            recipes.append(recipe)
        return SelectionRecipe(recipes)


embed = Embed.__invoke__


