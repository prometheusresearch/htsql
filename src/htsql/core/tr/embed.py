#
# Copyright (c) 2006-2012, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from ..adapter import Adapter, adapts, adapts_many
from ..domain import (UntypedDomain, BooleanDomain, IntegerDomain, FloatDomain,
                      DecimalDomain, DateDomain, TimeDomain, DateTimeDomain)
from .binding import LiteralRecipe, SelectionRecipe
import types
import datetime
import decimal


class Embed(Adapter):

    adapts(object)

    def __init__(self, value):
        self.value = value

    def __call__(self):
        raise ValueError("unable to embed a value of type %s"
                         % type(self.value))


class EmbedUntyped(Embed):

    adapts_many(str, unicode)

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

    adapts(types.NoneType)

    def __call__(self):
        return LiteralRecipe(None, UntypedDomain())


class EmbedBoolean(Embed):

    adapts(bool)

    def __call__(self):
        return LiteralRecipe(self.value, BooleanDomain())


class EmbedInteger(Embed):

    adapts_many(int, long)

    def __call__(self):
        return LiteralRecipe(self.value, IntegerDomain())


class EmbedFloat(Embed):

    adapts(float)

    def __call__(self):
        return LiteralRecipe(self.value, FloatDomain())


class EmbedDecimal(Embed):

    adapts(decimal.Decimal)

    def __call__(self):
        return LiteralRecipe(self.value, DecimalDomain())


class EmbedDate(Embed):

    adapts(datetime.date)

    def __call__(self):
        return LiteralRecipe(self.value, DateDomain())


class EmbedTime(Embed):

    adapts(datetime.time)

    def __call__(self):
        return LiteralRecipe(self.value, TimeDomain())


class EmbedDateTime(Embed):

    adapts(datetime.datetime)

    def __call__(self):
        return LiteralRecipe(self.value, DateTimeDomain())


class EmbedList(Embed):

    adapts_many(list, tuple)

    def __call__(self):
        recipes = []
        for value in self.value:
            recipe = embed(value)
            recipes.append(recipe)
        return SelectionRecipe(recipes)


def embed(value):
    embed = Embed(value)
    return embed()


