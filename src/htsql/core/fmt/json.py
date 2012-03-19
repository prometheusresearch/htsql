#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql.core.fmt.json`
==========================

This module implements the JSON renderer.
"""


from ..util import Printable
from ..adapter import Adapter, Protocol, adapt, adapt_many, call
from ..domain import (Domain, BooleanDomain, NumberDomain, FloatDomain,
                      StringDomain, EnumDomain, DateDomain, TimeDomain,
                      DateTimeDomain, ListDomain, RecordDomain,
                      VoidDomain, OpaqueDomain, Profile)
from .format import RawFormat, JSONFormat, EmitHeaders, Emit
import re
import math
import decimal


class JSIndicator(Printable):

    def __init__(self, symbol):
        self.symbol = symbol

    def __str__(self):
        return self.symbol


JS_SEQ = JSIndicator("[]")
JS_MAP = JSIndicator("{}")
JS_END = JSIndicator("$")
JS_DONE = JSIndicator("!")


def purge_null_keys(iterator):
    states = []
    context = None
    token = next(iterator, JS_DONE)
    while token is not JS_DONE:
        yield token
        if token is JS_SEQ or token is JS_MAP:
            states.append(context)
            context = token
        elif token is JS_END and states:
            context = states.pop()
        token = next(iterator, JS_DONE)
        if context is JS_MAP:
            while isinstance(token, unicode):
                key = token
                token = next(iterator, JS_DONE)
                if token is None:
                    token = next(iterator, JS_DONE)
                else:
                    yield key
                    break


def escape_json(value,
                escape_regexp=re.compile(r"""[\x00-\x1F\\/"]"""),
                escape_table={'"': '"', '\\': '\\', '/': '/',
                              '\x08': 'b', '\x0C': 'f', '\x0A': 'n',
                              '\x0D': 'r', '\x09': 't'}):
    def replace(match):
        char = match.group()
        if char in escape_table:
            return '\\'+escape_table[char]
        return '\\u%04X' % ord(char)
    return escape_regexp.sub(replace, value)


def dump_json(iterator):
    states = []
    context = None
    prefix = u""
    next_token = next(iterator, JS_DONE)
    while True:
        token = next_token
        next_token = next(iterator, JS_DONE)
        if token is None:
            line = u"null"
        elif token is True:
            line = u"true"
        elif token is False:
            line = u"false"
        elif isinstance(token, unicode):
            line = u"\"%s\"" % escape_json(token)
        elif isinstance(token, (int, long)):
            line = unicode(token)
        elif isinstance(token, float):
            if math.isinf(token) or math.isnan(token):
                line = u"null"
            else:
                line = unicode(token)
        elif isinstance(token, decimal.Decimal):
            if not token.is_finite():
                line = u"null"
            else:
                line = unicode(token)
        elif token is JS_SEQ:
            if next_token is JS_END:
                line = u"[]"
                next_token = next(iterator, JS_DONE)
            else:
                yield prefix+u"[\n"
                states.append(context)
                context = token
                prefix = u"  "*len(states)
                continue
        elif token is JS_MAP:
            if next_token is JS_END:
                line = u"{}"
                next_token = next(iterator, JS_DONE)
            else:
                assert isinstance(next_token, unicode), repr(next_token)
                yield prefix+u"{\n"
                states.append(context)
                context = token
                prefix = u"  "*len(states) + \
                         u"\"%s\": " % escape_json(next_token)
                next_token = next(iterator, JS_DONE)
                continue
        else:
            assert False, repr(token)
        if next_token is not JS_END and next_token is not JS_DONE:
            yield prefix+line+u",\n"
        else:
            yield prefix+line+u"\n"
        while next_token is JS_END and states:
            next_token = next(iterator, JS_DONE)
            if context is JS_SEQ:
                line = u"]"
            elif context is JS_MAP:
                line = u"}"
            context = states.pop()
            prefix = u"  "*len(states)
            if next_token is not JS_END and next_token is not JS_DONE:
                yield prefix+line+u",\n"
            else:
                yield prefix+line+u"\n"
        if context is JS_MAP:
            assert isinstance(next_token, unicode), repr(next_token)
            prefix = u"  "*len(states) + u"\"%s\": " % escape_json(next_token)
            next_token = next(iterator, JS_DONE)
        if context is None:
            assert next_token is JS_DONE, repr(next_token)
            break


class EmitJSONHeaders(EmitHeaders):

    adapt_many(JSONFormat,
               RawFormat)

    def __call__(self):
        filename = None
        if self.meta.header:
            filename = self.meta.header.encode('utf-8')
        if not filename:
            filename = '_'
        filename = filename.replace('\\', '\\\\').replace('"', '\\"')
        # The specification `http://www.ietf.org/rfc/rfc4627.txt` recommends
        # `application/json`, but we use `application/javascript` to prevent
        # the browser from opening "Save As" window.
        yield ('Content-Type', 'application/javascript')
        yield ('Content-Disposition', 'inline; filename="%s.js"' % filename)


class EmitRaw(Emit):

    adapt(RawFormat)

    def __call__(self):
        return dump_json(purge_null_keys(self.emit()))

    def emit(self):
        data_to_raw = to_raw(self.meta.domain)
        yield JS_MAP
        yield u"meta"
        for token in profile_to_raw(self.meta):
            yield token
        yield u"data"
        for token in data_to_raw(self.data):
            yield token
        yield JS_END


class EmitJSON(Emit):

    adapt(JSONFormat)

    def __call__(self):
        return dump_json(purge_null_keys(self.emit()))

    def emit(self):
        product_to_json = to_json(self.meta.domain)
        if self.meta.tag:
            key = self.meta.tag
        else:
            key = unicode(0)
        yield JS_MAP
        yield key
        for token in product_to_json(self.data):
            yield token
        yield JS_END


class ToRaw(Adapter):

    adapt(Domain)

    def __init__(self, domain):
        assert isinstance(domain, Domain)
        self.domain = domain

    def __call__(self):
        return self.scatter

    def scatter(self, value):
        if value is None:
            yield None
        else:
            yield self.domain.dump(value)


class RecordToRaw(ToRaw):

    adapt(RecordDomain)

    def __init__(self, domain):
        super(RecordToRaw, self).__init__(domain)
        self.fields_to_raw = [to_raw(field.domain)
                               for field in domain.fields]

    def scatter(self, value):
        if value is None:
            yield None
        else:
            yield JS_SEQ
            for item, field_to_raw in zip(value, self.fields_to_raw):
                for token in field_to_raw(item):
                    yield token
            yield JS_END


class ListToRaw(ToRaw):

    adapt(ListDomain)

    def __init__(self, domain):
        super(ListToRaw, self).__init__(domain)
        self.item_to_raw = to_raw(domain.item_domain)

    def scatter(self, value):
        if value is None:
            yield None
        else:
            item_to_raw = self.item_to_raw
            yield JS_SEQ
            for item in value:
                for token in item_to_raw(item):
                    yield token
            yield JS_END


class NativeToRaw(ToRaw):

    adapt_many(BooleanDomain,
               NumberDomain,
               StringDomain,
               EnumDomain)

    @staticmethod
    def scatter(value):
        yield value


class NativeStringToRaw(ToRaw):

    adapt_many(DateDomain,
               TimeDomain)

    @staticmethod
    def scatter(value):
        if value is None:
            yield None
        else:
            yield unicode(value)


class DateTimeToRaw(ToRaw):

    adapt(DateTimeDomain)

    @staticmethod
    def scatter(value):
        if value is None:
            yield None
        elif not value.time():
            yield unicode(value.date())
        else:
            yield unicode(value)


class OpaqueToRaw(ToRaw):

    adapt(OpaqueDomain)

    @staticmethod
    def scatter(value):
        if value is None:
            yield None
            return
        if not isinstance(value, unicode):
            try:
                value = str(value).decode('utf-8')
            except UnicodeDecodeError:
                value = unicode(repr(value))
        yield value


class MetaToRaw(Protocol):

    def __init__(self, name, profile):
        assert isinstance(name, str)
        assert isinstance(profile, Profile)
        self.name = name
        self.profile = profile

    def __call__(self):
        yield None


class DomainMetaToRaw(MetaToRaw):

    call('domain')

    def __call__(self):
        return domain_to_raw(self.profile.domain)


class SyntaxMetaToRaw(MetaToRaw):

    call('syntax')

    def __call__(self):
        if self.profile.syntax is None:
            yield None
        else:
            yield unicode(self.profile.syntax)


class TagMetaToRaw(MetaToRaw):

    call('tag')

    def __call__(self):
        if self.profile.tag is None:
            yield None
        else:
            yield self.profile.tag


class HeaderMetaToRaw(MetaToRaw):

    call('header')

    def __call__(self):
        yield self.profile.header


class PathMetaToRaw(MetaToRaw):

    call('path')

    def __call__(self):
        # FIXME: circular import?
        from ..classify import relabel
        if self.profile.path is None:
            yield None
        else:
            names = []
            for arc in self.profile.path:
                labels = relabel(arc)
                if not labels:
                    yield None
                    return
                names.append(labels[0].name)
            yield u".".join(names)


class DomainToRaw(Adapter):

    adapt(Domain)

    def __init__(self, domain):
        assert isinstance(domain, Domain)
        self.domain = domain

    def __call__(self):
        yield JS_MAP
        yield u"type"
        yield unicode(self.domain.family)
        yield JS_END


class VoidDomainToRaw(DomainToRaw):

    adapt(VoidDomain)

    def __call__(self):
        yield None


class ListDomainToRaw(DomainToRaw):

    adapt(ListDomain)

    def __call__(self):
        yield JS_MAP
        yield u"type"
        yield unicode(self.domain.family)
        yield u"item"
        yield JS_MAP
        yield u"domain"
        for token in domain_to_raw(self.domain.item_domain):
            yield token
        yield JS_END
        yield JS_END


class RecordDomainToRaw(DomainToRaw):

    adapt(RecordDomain)

    def __call__(self):
        yield JS_MAP
        yield u"type"
        yield unicode(self.domain.family)
        yield u"fields"
        yield JS_SEQ
        for field in self.domain.fields:
            for token in profile_to_raw(field):
                yield token
        yield JS_END
        yield JS_END


class ToJSON(Adapter):

    adapt(Domain)

    def __init__(self, domain):
        assert isinstance(domain, Domain)
        self.domain = domain

    def __call__(self):
        return to_raw(self.domain)


class RecordToJSON(ToJSON):

    adapt(RecordDomain)

    def __init__(self, domain):
        super(RecordToJSON, self).__init__(domain)
        self.fields_to_json = [to_json(field.domain) for field in domain.fields]
        self.field_keys = []
        duplicates = set()
        for idx, field in enumerate(self.domain.fields):
            if field.tag and field.tag not in duplicates:
                key = field.tag
                duplicates.add(key)
            else:
                key = unicode(idx)
            self.field_keys.append(key)

    def __call__(self):
        return self.scatter

    def scatter(self, value):
        if value is None:
            yield None
        else:
            yield JS_MAP
            for item, field_to_json, field_key in zip(value,
                                                      self.fields_to_json,
                                                      self.field_keys):
                yield field_key
                for token in field_to_json(item):
                    yield token
            yield JS_END


class ListToJSON(ToJSON):

    adapt(ListDomain)

    def __init__(self, domain):
        super(ListToJSON, self).__init__(domain)
        self.item_to_json = to_json(domain.item_domain)

    def __call__(self):
        return self.scatter

    def scatter(self, value):
        if value is None:
            yield None
        else:
            yield JS_SEQ
            item_to_json = self.item_to_json
            for item in value:
                for token in item_to_json(item):
                    yield token
            yield JS_END


def profile_to_raw(profile):
    yield JS_MAP
    for name in MetaToRaw.__catalogue__():
        yield unicode(name)
        for token in MetaToRaw.__invoke__(name, profile):
            yield token
    yield JS_END


def domain_to_raw(domain):
    return DomainToRaw.__invoke__(domain)


def to_raw(domain):
    return ToRaw.__invoke__(domain)


def to_json(domain):
    return ToJSON.__invoke__(domain)


