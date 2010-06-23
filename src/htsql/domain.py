#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.domain`
===================

This module defines abstract HTSQL domains.
"""


from .util import maybe, listof
import re


class Domain(object):

    name = None

    def parse(self, data):
        assert isinstance(data, maybe(str))
        if data is None:
            return None
        raise ValueError("invalid literal")

    def dump(self, value):
        assert value is None
        return None

    def __str__(self):
        return self.name

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self)


class BooleanDomain(Domain):

    name = 'boolean'

    def parse(self, data):
        assert isinstance(data, maybe(str))
        if data is None:
            return None
        if data.lower() == 'true':
            return True
        if data.lower() == 'false':
            return False
        raise ValueError("invalid Boolean literal: expected 'true' or 'false';"
                         " got %r" % data)

    def dump(self, value):
        assert isinstance(value, maybe(bool))
        if value is None:
            return None
        if value is True:
            return 'true'
        if value is False:
            return 'false'


class NumberDomain(Domain):

    is_exact = True
    radix = 2


class IntegerDomain(NumberDomain):

    name = 'integer'

    def __init__(self, size=None):
        self.size = size

    def parse(self, data):
        assert isinstance(data, maybe(str))
        if data is None:
            return None
        try:
            value = int(data)
        except ValueError, exc:
            raise ValueError("invalid integer literal: %s" % exc)
        return value

    def dump(self, value):
        assert isinstance(value, maybe(int))
        if value is None:
            return None
        return str(value)


class FloatDomain(NumberDomain):

    name = 'float'
    is_exact = False

    def __init__(self, size=None):
        self.size = size

    def parse(self, data):
        assert isinstance(data, maybe(str))
        if data is None:
            return None
        try:
            value = float(data)
        except ValueError, exc:
            raise ValueError("invalid float literal: %s" % exc)
        return value

    def dump(self, value):
        assert isinstance(value, maybe(float))
        if value is None:
            return None
        return str(value)


class DecimalDomain(NumberDomain):

    name = 'decimal'
    radix = 10

    def __init__(self, precision=None, scale=None):
        assert isinstance(precision, maybe(int))
        assert isinstance(scale, maybe(int))
        self.precision = precision
        self.scale = scale

    def parse(self, data):
        assert isinstance(data, maybe(str))
        if data is None:
            return None
        try:
            value = decimal.Decimal(data)
        except decimal.InvalidOperation, exc:
            raise ValueError("invalid decimal literal: %s" % exc)
        return value

    def dump(self, value):
        assert isinstance(value, maybe(decimal.Decimal))
        if value is None:
            return None
        return str(value)


class StringDomain(Domain):

    name = 'string'

    def __init__(self, length=None, is_varying=True):
        assert isinstance(length, maybe(int))
        assert isinstance(is_varying, bool)
        self.length = length
        self.is_varying = is_varying

    def parse(self, data):
        assert isinstance(data, maybe(str))
        if data is None:
            return None
        return data

    def dump(self, value):
        assert isinstance(value, maybe(str))
        if value is None:
            return None
        return value


class EnumDomain(Domain):

    name = 'enum'

    def __init__(self, labels=None):
        assert isinstance(labels, maybe(listof(str)))
        self.labels = labels


class DateDomain(Domain):

    name = 'date'

    pattern = r'^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})$'
    regexp = re.compile(pattern)

    def parse(self, data):
        assert isinstance(data, maybe(str))
        if data is None:
            return None
        match = self.regexp.match(data)
        if match is None:
            raise ValueError("invalid date literal: expected 'YYYY-MM-DD';"
                             " got %r" % data)
        year = int(match.group('year'))
        month = int(match.group('month'))
        day = int(match.group('day'))
        try:
            value = datetime.date(year, month, day)
        except ValueError, exc:
            raise ValueError("invalid date literal: %s" % exc)
        return value

    def dump(self, value):
        assert isinstance(value, maybe(datetime.date))
        if value is None:
            return None
        return str(value)


class VoidDomain(Domain):

    name = 'void'


class OpaqueDomain(Domain):

    name = 'opaque'


class UntypedDomain(Domain):

    name = 'untyped'


class UntypedNumberDomain(UntypedDomain):
    pass


class UntypedStringDomain(UntypedDomain):
    pass


