#
# Copyright (c) 2006-2012, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql.core.fmt.json`
==========================

This module implements the JSON renderer.
"""


from ..util import Printable
from ..adapter import Adapter, Protocol, adapts, adapts_many, named
from ..domain import (Domain, BooleanDomain, NumberDomain, FloatDomain,
                      StringDomain, EnumDomain, DateDomain, TimeDomain,
                      DateTimeDomain, ListDomain, RecordDomain)
from .format import JSONFormat, ObjFormat, EmitHeaders, Emit
from .format import Renderer, Format
import re
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
            if str(token) in ['inf', '-inf', 'nan']:
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

    adapts_many(JSONFormat,
                ObjFormat)

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
        return [('Content-Type', 'application/javascript'),
                ('Content-Disposition',
                 'inline; filename="%s.js"' % filename)]


class EmitJSON(Emit):

    adapts(JSONFormat)

    def __call__(self):
        return dump_json(purge_null_keys(self.emit()))

    def emit(self):
        yield JS_MAP
        yield u"meta"
        for token in self.emit_meta():
            yield token
        yield u"data"
        for token in self.emit_data():
            yield token
        yield JS_END

    def emit_meta(self):
        return serialize_profile(self.meta)

    def emit_data(self):
        serialize = to_json(self.meta.domain)
        return serialize(self.data)


class EmitObj(Emit):

    adapts(ObjFormat)

    def __call__(self):
        return dump_json(purge_null_keys(self.emit()))

    def emit(self):
        if self.meta.tag:
            key = unicode(self.meta.tag)
        else:
            key = u"0"
        serialize = to_obj(self.meta.domain)
        yield JS_MAP
        yield key
        for token in serialize(self.data):
            yield token
        yield JS_END


class ToJSON(Adapter):

    adapts(Domain)

    def __init__(self, domain):
        self.domain = domain

    def __call__(self):
        def serialize(value, dump=self.domain.dump):
            if value is None:
                yield None
            else:
                yield dump(value)
        return serialize


class RecordToJSON(ToJSON):

    adapts(RecordDomain)

    def __call__(self):
        serializers = [to_json(field.domain)
                       for field in self.domain.fields]
        def serialize_record(value, serializers=serializers):
            if value is None:
                yield None
            else:
                yield JS_SEQ
                for item, serializer in zip(value, serializers):
                    for token in serializer(item):
                        yield token
                yield JS_END
        return serialize_record


class ListToJSON(ToJSON):

    adapts(ListDomain)

    def __call__(self):
        serializer = to_json(self.domain.item_domain)
        def serialize_list(value, serializer=serializer):
            if value is None:
                yield None
            else:
                yield JS_SEQ
                for item in value:
                    for token in serializer(item):
                        yield token
                yield JS_END
        return serialize_list


class BooleanToJSON(ToJSON):

    adapts(BooleanDomain)

    def __call__(self):
        def serialize_boolean(value):
            yield value
        return serialize_boolean


class NumberToJSON(ToJSON):

    adapts(NumberDomain)

    def __call__(self):
        def serialize_number(value):
            yield value
        return serialize_number


class StringToJSON(ToJSON):

    adapts_many(StringDomain,
                EnumDomain)

    def __call__(self):
        def serialize_string(value):
            yield value
        return serialize_string


class DateToJSON(ToJSON):

    adapts(DateDomain)

    def __call__(self):
        def serialize_date(value):
            if value is None:
                yield None
            else:
                yield unicode(value)
        return serialize_date


class TimeToJSON(ToJSON):

    adapts(TimeDomain)

    def __call__(self):
        def serialize_time(value):
            if value is None:
                yield None
            else:
                yield unicode(value)
        return serialize_time


class DateTimeToJSON(ToJSON):

    adapts(DateTimeDomain)

    def __call__(self):
        def serialize_datetime(value):
            if value is None:
                yield None
            elif not value.time():
                yield unicode(value.date())
            else:
                yield unicode(value)
        return serialize_datetime


class ToObj(Adapter):

    adapts(Domain)

    def __init__(self, domain):
        self.domain = domain

    def __call__(self):
        return to_json(self.domain)


class RecordToObj(ToObj):

    adapts(RecordDomain)

    def __call__(self):
        serializers = []
        keys = []
        duplicates = set()
        for idx, field in enumerate(self.domain.fields):
            serializer = to_obj(field.domain)
            if field.tag and field.tag not in duplicates:
                key = unicode(field.tag)
                duplicates.add(field.tag)
            else:
                key = unicode(idx)
            serializers.append(serializer)
            keys.append(key)
        def serialize_record(value, serializers=serializers, keys=keys):
            if value is None:
                yield None
            else:
                yield JS_MAP
                for item, serializer, key in zip(value, serializers, keys):
                    yield key
                    for token in serializer(item):
                        yield token
                yield JS_END
        return serialize_record


class ListToObj(ToObj):

    adapts(ListDomain)

    def __call__(self):
        serializer = to_obj(self.domain.item_domain)
        def serialize_list(value, serializer=serializer):
            if value is None:
                yield None
            else:
                yield JS_SEQ
                for item in value:
                    for token in serializer(item):
                        yield token
                yield JS_END
        return serialize_list


class SerializeProfile(Protocol):

    def __init__(self, name, profile):
        self.name = name
        self.profile = profile

    def __call__(self):
        yield None


class SerializeProfileDomain(SerializeProfile):

    named('domain')

    def __call__(self):
        serialize = SerializeDomain(self.profile.domain)
        return serialize()


class SerializeProfileSyntax(SerializeProfile):

    named('syntax')

    def __call__(self):
        if self.profile.syntax is None:
            yield None
        else:
            yield unicode(self.profile.syntax)


class SerializeProfileTag(SerializeProfile):

    named('tag')

    def __call__(self):
        if self.profile.tag is None:
            yield None
        else:
            yield unicode(self.profile.tag)


class SerializeProfileHeader(SerializeProfile):

    named('header')

    def __call__(self):
        yield self.profile.header


class SerializeProfilePath(SerializeProfile):

    named('path')

    def __call__(self):
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


class SerializeDomain(Adapter):

    adapts(Domain)

    def __init__(self, domain):
        self.domain = domain

    def __call__(self):
        yield JS_MAP
        yield u"type"
        yield unicode(self.domain.family)
        yield JS_END


class SerializeList(SerializeDomain):

    adapts(ListDomain)

    def __call__(self):
        yield JS_MAP
        yield u"type"
        yield unicode(self.domain.family)
        yield u"item"
        yield JS_MAP
        yield u"domain"
        serialize = SerializeDomain(self.domain.item_domain)
        for token in serialize():
            yield token
        yield JS_END
        yield JS_END


class SerializeRecord(SerializeDomain):

    adapts(RecordDomain)

    def __call__(self):
        yield JS_MAP
        yield u"type"
        yield unicode(self.domain.family)
        yield u"fields"
        yield JS_SEQ
        for field in self.domain.fields:
            for token in serialize_profile(field):
                yield token
        yield JS_END
        yield JS_END


def serialize_profile(profile):
    names = set()
    for component in SerializeProfile.implementations():
        for name in component.names:
            names.add(name)
    yield JS_MAP
    for name in sorted(names):
        serialize = SerializeProfile(name, profile)
        yield unicode(name)
        for token in serialize():
            yield token
    yield JS_END


def to_json(domain):
    to_json = ToJSON(domain)
    return to_json()


def to_obj(domain):
    to_obj = ToObj(domain)
    return to_obj()


class JSONRenderer(Renderer):

    name = 'application/javascript'
    aliases = ['js', 'application/json', 'json']

    def render(self, product):
        status = self.generate_status(product)
        headers = self.generate_headers(product)
        body = self.generate_body(product)
        return status, headers, body

    def generate_status(self, product):
        return "200 OK"

    def generate_headers(self, product):
        format = JSONFormat()
        emit_headers = EmitHeaders(format, product)
        return emit_headers()

    def generate_body(self, product):
        format = JSONFormat()
        emit_body = Emit(format, product)
        for line in emit_body():
            if isinstance(line, unicode):
                line = line.encode('utf-8')
            yield line


class ObjRenderer(Renderer):

    name = 'obj'

    def render(self, product):
        status = self.generate_status(product)
        headers = self.generate_headers(product)
        body = self.generate_body(product)
        return status, headers, body

    def generate_status(self, product):
        return "200 OK"

    def generate_headers(self, product):
        format = ObjFormat()
        emit_headers = EmitHeaders(format, product)
        return emit_headers()

    def generate_body(self, product):
        format = ObjFormat()
        emit_body = Emit(format, product)
        for line in emit_body():
            if isinstance(line, unicode):
                line = line.encode('utf-8')
            yield line


class FormatDomain(Format):

    adapts(JSONRenderer, Domain)

    def __call__(self, value):
        if value is None:
            return "null"
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        else:
            value = str(value)
        try:
            value.decode('utf-8')
        except UnicodeDecodeError:
            value = repr(value)
        return escape(value)


class FormatBoolean(Format):

    adapts(JSONRenderer, BooleanDomain)

    def __call__(self, value):
        if value is None:
            return "null"
        if value is True:
            return "true"
        if value is False:
            return "false"


class FormatNumber(Format):

    adapts(JSONRenderer, NumberDomain)

    def __call__(self, value):
        if value is None:
            return "null"
        return str(value)


class FormatFloat(Format):

    adapts(JSONRenderer, FloatDomain)

    def __call__(self, value):
        if value is None:
            return "null"
        return repr(value)


class FormatString(Format):

    adapts(JSONRenderer, StringDomain)

    def __call__(self, value):
        if value is None:
            return "null"
        return escape(value.encode('utf-8'))


class FormatEnum(Format):

    adapts(JSONRenderer, EnumDomain)

    def __call__(self, value):
        if value is None:
            return "null"
        return escape(value.encode('utf-8'))


class FormatDate(Format):

    adapts(JSONRenderer, DateDomain)

    def __call__(self, value):
        if value is None:
            return "null"
        return escape(str(value))


class FormatTime(Format):

    adapts(JSONRenderer, TimeDomain)

    def __call__(self, value):
        if value is None:
            return "null"
        return escape(str(value))


class FormatDateTime(Format):

    adapts(JSONRenderer, DateTimeDomain)

    def __call__(self, value):
        if value is None:
            return "null"
        if not value.time():
            return escape(str(value.date()))
        return escape(str(value))


class EntitleDomain(Adapter):

    adapts(Domain)
    name = "unknown"

    def __init__(self, domain):
        self.domain = domain

    def __call__(self):
        return self.name


class EntitleBoolean(EntitleDomain):

    adapts(BooleanDomain)
    name = "boolean"


class EntitleNumber(EntitleDomain):

    adapts(NumberDomain)
    name = "number"


class EntitleString(EntitleDomain):

    adapts(StringDomain)
    name = "string"


class EntitleEnum(EntitleDomain):

    adapts(EnumDomain)
    name = "enum"


class EntitleDate(EntitleDomain):

    adapts(DateDomain)
    name = "date"


class EntitleTime(EntitleDomain):

    adapts(TimeDomain)
    name = "time"


class EntitleDateTime(EntitleDomain):

    adapts(DateTimeDomain)
    name = "datetime"


class Escape(object):

    escape_pattern = r"""[\x00-\x1F\\/"]"""
    escape_regexp = re.compile(escape_pattern)
    escape_table = {
            '"': '"',
            '\\': '\\',
            '/': '/',
            '\x08': 'b',
            '\x0C': 'f',
            '\x0A': 'n',
            '\x0D': 'r',
            '\x09': 't',
    }

    @classmethod
    def replace(cls, match):
        char = match.group()
        if char in cls.escape_table:
            return '\\'+cls.escape_table[char]
        return '\\u%04X' % ord(char)

    @classmethod
    def escape(cls, value):
        value = value.decode('utf-8')
        value = cls.escape_regexp.sub(cls.replace, value)
        value = value.encode('utf-8')
        return '"%s"' % value


escape = Escape.escape


def entitle_domain(domain):
    entitle = EntitleDomain(domain)
    return entitle()


