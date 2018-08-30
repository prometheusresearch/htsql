#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ..util import isfinite, to_name
from ..adapter import Adapter, adapt, adapt_many
from ..domain import (UntypedDomain, BooleanDomain, IntegerDomain, FloatDomain,
        DecimalDomain, DateDomain, TimeDomain, DateTimeDomain, ListDomain,
        IdentityDomain, ID, Value)
import types
import datetime
import decimal


class Embed(Adapter):

    adapt(object)

    def __init__(self, data):
        self.data = data

    def __call__(self):
        raise TypeError("unable to embed a value of type %s"
                        % type(self.data))


class EmbedValue(Embed):

    adapt(Value)

    def __call__(self):
        return self.data


class EmbedUntyped(Embed):

    adapt_many(str, str)

    def __call__(self):
        data = self.data
        if isinstance(data, str):
            try:
                data = data.decode('utf-8')
            except UnicodeDecodeError:
                raise TypeError("a string is expected to be encoded in UTF-8:"
                                " %s" % repr(data))
        if "\0" in data:
            raise TypeError("a string should not contain a NIL character:"
                            " %s" % repr(data))
        return Value(UntypedDomain(), data)


class EmbedNull(Embed):

    adapt(type(None))

    def __call__(self):
        return Value(UntypedDomain(), None)


class EmbedBoolean(Embed):

    adapt(bool)

    def __call__(self):
        return Value(BooleanDomain(), self.data)


class EmbedInteger(Embed):

    adapt_many(int, int)

    def __call__(self):
        return Value(IntegerDomain(), self.data)


class EmbedFloat(Embed):

    adapt(float)

    def __call__(self):
        if not isfinite(self.data):
            raise TypeError("a float value must be finite")
        return Value(FloatDomain(), self.data)


class EmbedDecimal(Embed):

    adapt(decimal.Decimal)

    def __call__(self):
        if not isfinite(self.data):
            raise TypeError("a decimal value must be finite")
        return Value(DecimalDomain(), self.data)


class EmbedDate(Embed):

    adapt(datetime.date)

    def __call__(self):
        return Value(DateDomain(), self.data)


class EmbedTime(Embed):

    adapt(datetime.time)

    def __call__(self):
        return Value(TimeDomain(), self.data)


class EmbedDateTime(Embed):

    adapt(datetime.datetime)

    def __call__(self):
        return Value(DateTimeDomain(), self.data)


class EmbedList(Embed):

    adapt_many(list, tuple)

    def __call__(self):
        entry_values = [Embed.__invoke__(entry) for entry in self.data]
        domain_set = set(entry_value.domain for entry_value in entry_values
                         if not isinstance(entry_value.domain, UntypedDomain))
        if not domain_set:
            domain = UntypedDomain()
            return Value(ListDomain(domain), [entry_value.data
                                              for entry_value in entry_values])
        if len(domain_set) > 1:
            domain_names = sorted(str(domain) for domain in domain_set)
            raise TypeError("multiple entry domains: %s"
                            % ", ".join(domain_names))
        domain = domain_set.pop()
        entries = [entry_value.data if entry_value.domain == domain else
                   domain.parse(entry_value.data)
                   for entry_value in entry_values]
        return Value(ListDomain(domain), entries)


class EmbedIdentity(Embed):

    adapt(ID)

    def __call__(self):
        entry_values = [Embed.__invoke__(entry) for entry in self.data]
        if any(value.data is None for value in entry_values):
            raise TypeError("an ID value should not contain a NULL entry")
        domain = IdentityDomain([value.domain for value in entry_values])
        entries = tuple(value.data for value in entry_values)
        return Value(domain, entries)


def embed(base_environment, **parameters):
    environment = {}
    if base_environment:
        for name in sorted(base_environment):
            value = base_environment[name]
            if not isinstance(value, Value):
                value = Embed.__invoke__(value)
            name = to_name(name)
            environment[name] = value
    for name in sorted(parameters):
        value = parameters[name]
        if not isinstance(value, Value):
            value = Embed.__invoke__(value)
        name = to_name(name)
        environment[name] = value
    return environment


