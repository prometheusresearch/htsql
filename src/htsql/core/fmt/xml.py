#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ..util import Printable, listof, tupleof
from ..adapter import Adapter, Protocol, adapt, adapt_many, call
from ..domain import (Domain, BooleanDomain, NumberDomain, DecimalDomain,
        TextDomain, EnumDomain, DateDomain, TimeDomain, DateTimeDomain,
        ListDomain, RecordDomain, UntypedDomain, VoidDomain, OpaqueDomain,
        Profile)
from .format import XMLFormat
from .emit import EmitHeaders, Emit
import re
import decimal


class XML_SIGNAL(Printable):

    is_start = False
    is_end = False
    is_text = False


class XML_START(XML_SIGNAL):

    is_start = True

    def __init__(self, tag, attributes=[]):
        assert isinstance(tag, str)
        assert isinstance(attributes, listof(tupleof(str, str)))
        self.tag = tag
        self.attributes = attributes

    def __str__(self):
        return "<%s%s>" % (self.tag.encode('utf-8'),
                           "".join(" %s=\"%s\""
                                    % (attribute.encode('utf-8'),
                                       escape_xml(value).encode('utf-8'))
                                   for attribute, value in self.attributes))


class XML_END(XML_SIGNAL):

    is_end = True

    def __str__(self):
        return "</>"


class XML_TEXT(XML_SIGNAL):

    is_text = True

    def __init__(self, data):
        assert isinstance(data, str)
        self.data = data

    def __str__(self):
        return escape_xml(self.data).encode('utf-8')


def escape_xml(data, escape_regexp=re.compile(r"""[\x00-\x1F\x7F<>&"]"""),
                     escape_table={'<': '&lt;', '>': '&gt;',
                                   '"': '&quot;', '&': '&amp;'}):
    def replace(match):
        char = match.group()
        if char in escape_table:
            return escape_table[char]
        code = ord(char)
        if code < 0x100:
            return '&#x%02X;' % code
        else:
            return '&#x%04X;' % code
    return escape_regexp.sub(replace, data)


def dump_xml(iterator):
    def pull():
        try:
            signal = next(iterator)
            if isinstance(signal, tuple) and len(signal) > 0:
                return signal[0](*signal[1:])
            elif isinstance(signal, type):
                return signal()
            elif isinstance(signal, str):
                return XML_TEXT(signal)
            return signal
        except StopIteration:
            return None
    tags = []
    is_newline = True
    next_signal = pull()
    yield "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n"
    while next_signal is not None:
        signal = next_signal
        next_signal = pull()
        chunks = []
        if signal.is_start:
            if not is_newline:
                chunks.append("\n")
            chunks.append("  "*len(tags))
            chunks.append("<")
            chunks.append(signal.tag)
            for attribute, value in signal.attributes:
                chunks.append(" %s=\"%s\"" % (attribute,
                                               escape_xml(value)))
            if next_signal.is_end:
                chunks.append(" />\n")
            else:
                chunks.append(">")
            if next_signal.is_end:
                signal = next_signal
                next_signal = pull()
                is_newline = True
            else:
                tags.append(signal.tag)
                is_newline = False
        elif signal.is_end:
            tag = tags.pop()
            chunks = []
            if is_newline:
                chunks.append("  "*len(tags))
            chunks.append("</")
            chunks.append(tag)
            chunks.append(">\n")
            is_newline = True
        elif signal.is_text:
            chunks = []
            if not next_signal.is_end and not is_newline:
                chunks.append("\n")
                is_newline = True
            if is_newline:
                chunks.append("  "*len(tags))
            chunks.append(escape_xml(signal.data))
            if is_newline:
                chunks.append("\n")
        yield "".join(chunks)


class EmitXMLHeaders(EmitHeaders):

    adapt(XMLFormat)

    def __call__(self):
        filename = None
        if self.meta.header:
            filename = self.meta.header.encode('utf-8')
        if not filename:
            filename = '_'
        filename = filename.replace('\\', '\\\\').replace('"', '\\"')
        yield ('Content-Type', 'application/xml')
        yield ('Content-Disposition', 'inline; filename="%s.xml"' % filename)


class EmitXML(Emit):

    adapt(XMLFormat)

    def __call__(self):
        return dump_xml(self.emit())

    def emit(self):
        if (self.meta.tag is not None and
                not re.match(r"""^[Xx][Mm][Ll]|^_\d*$""", self.meta.tag)):
            tag = self.meta.tag
        else:
            tag = "_"
        product_to_xml = to_xml(self.meta.domain, tag)
        yield XML_START, "htsql:result", [("xmlns:htsql",
                                            "http://htsql.org/2010/xml")]
        for signal in product_to_xml(self.data):
            yield signal
        yield XML_END


class ToXML(Adapter):

    adapt(Domain)

    def __init__(self, domain, tag):
        assert isinstance(domain, Domain)
        assert isinstance(tag, str)
        self.domain = domain
        self.tag = tag

    def __call__(self):
        return self.scatter

    def scatter(self, value):
        if value is not None:
            yield XML_START, self.tag
            yield self.domain.dump(value)
            yield XML_END


class RecordToXML(ToXML):

    adapt(RecordDomain)

    def __init__(self, domain, tag):
        super(RecordToXML, self).__init__(domain, tag)
        self.fields_to_xml = []
        used = set()
        for idx, field in enumerate(self.domain.fields):
            if (field.tag and field.tag not in used and
                    not re.match(r"""^[Xx][Mm][Ll]|^_\d*$""", field.tag)):
                tag = field.tag
                used.add(tag)
            else:
                tag = "_%s" % (idx+1)
            field_to_xml = to_xml(field.domain, tag)
            self.fields_to_xml.append(field_to_xml)

    def scatter(self, value):
        if value is not None:
            yield XML_START, self.tag
            for item, field_to_xml in zip(value, self.fields_to_xml):
                for signal in field_to_xml(item):
                    yield signal
            yield XML_END


class ListToXML(ToXML):

    adapt(ListDomain)

    def __init__(self, domain, tag):
        super(ListToXML, self).__init__(domain, tag)
        self.item_to_xml = to_xml(domain.item_domain, tag)

    def scatter(self, value):
        if value is not None:
            for item in value:
                for signal in self.item_to_xml(item):
                    yield signal


class NativeToXML(ToXML):

    adapt_many(UntypedDomain,
               TextDomain,
               EnumDomain,
               NumberDomain,
               DateDomain,
               TimeDomain)

    def scatter(self, value):
        if value is not None:
            yield XML_START, self.tag
            yield str(value)
            yield XML_END


class DecimalToXML(ToXML):

    adapt(DecimalDomain)

    def scatter(self, value):
        if value is None:
            return
        sign, digits, exp = value.as_tuple()
        if not digits:
            value = str(value)
        else:
            if exp < -6 and value == value.normalize():
                value = value.normalize()
                sign, digits, exp = value.as_tuple()
            if exp > 0:
                value = value.quantize(decimal.Decimal(1))
            value = str(value)
        yield XML_START, self.tag
        yield value
        yield XML_END


class DateTimeToXML(ToXML):

    adapt(DateTimeDomain)

    def dump(self, value):
        if value is None:
            return
        yield XML_START, self.tag
        if not value.time():
            yield str(value.date())
        else:
            yield str(value)
        yield XML_END


class OpaqueToXML(ToXML):

    adapt(OpaqueDomain)

    def dump(self, value):
        if value is None:
            return
        yield XML_START, self.tag
        if not isinstance(value, str):
            try:
                value = str(value).decode('utf-8')
            except UnicodeDecodeError:
                value = str(repr(value))
        yield value
        yield XML_END


to_xml = ToXML.__invoke__


