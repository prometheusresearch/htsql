#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql.core.fmt.text`
==========================

This module implements the plain text renderer.
"""


from ..adapter import Adapter, adapt, adapt_many
from ..util import maybe, oneof
from ..context import context
from .format import TextFormat, EmitHeaders, Emit
from ..domain import (Domain, BooleanDomain, NumberDomain, IntegerDomain,
                      DecimalDomain, FloatDomain, StringDomain, EnumDomain,
                      DateDomain, TimeDomain, DateTimeDomain,
                      ListDomain, RecordDomain, VoidDomain,
                      OpaqueDomain, Profile)
import re
import decimal
import datetime


class EmitTextHeaders(EmitHeaders):

    adapt(TextFormat)

    def __call__(self):
        yield ('Content-Type', 'text/plain; charset=UTF-8')


class EmitText(Emit):

    adapt(TextFormat)

    def __call__(self):
        addon = context.app.htsql
        product_to_text = profile_to_text(self.meta)
        size = product_to_text.size
        if size == 0:
            return
        widths = product_to_text.widths(self.data)
        depth = product_to_text.head_depth()
        head = product_to_text.head(depth)
        if depth > 0:
            bar = [(None, 0)]*size
            for row_idx in range(depth):
                row = next(head, [])
                last_bar = bar
                bar = []
                while len(bar) < size:
                    idx = len(bar)
                    text, tail = last_bar[idx]
                    if tail > 0:
                        bar.append((text, tail-1))
                    else:
                        text, rowspan, colspan = row.pop(0)
                        bar.append((text, rowspan-1))
                        for span in range(colspan-1):
                            bar.append((None, rowspan-1))
                assert not row
                if row_idx > 0:
                    line = [u" "]
                    for idx in range(0, size+1):
                        is_horiz = False
                        is_vert = False
                        if idx > 0:
                            text, tail = last_bar[idx-1]
                            if tail == 0:
                                is_horiz = True
                        if idx < size:
                            text, tail = last_bar[idx]
                            if tail == 0:
                                is_horiz = True
                        if idx < size:
                            text, tail = last_bar[idx]
                            if text is not None:
                                is_vert = True
                            text, tail = bar[idx]
                            if text is not None:
                                is_vert = True
                        else:
                            is_vert = True
                        if is_horiz and is_vert:
                            line.append(u"+")
                        elif is_horiz:
                            line.append(u"-")
                        elif is_vert:
                            line.append(u"|")
                        else:
                            line.append(u" ")
                        if idx < size:
                            text, tail = last_bar[idx]
                            if tail == 0:
                                line.append(u"-"*(widths[idx]+2))
                            else:
                                line.append(u" "*(widths[idx]+2))
                        else:
                            line.append(u"\n")
                    yield "".join(line)
                extent = 0
                line = []
                for idx in range(size):
                    text, tail = bar[idx]
                    if text is not None:
                        assert extent == 0, extent
                        line.append(u" | ")
                    else:
                        if extent < 3:
                            line.append(u" "*(3-extent))
                            extent = 0
                        else:
                            extent -= 3
                    width = widths[idx]
                    if text is not None and tail == 0:
                        line.append(text)
                        extent = len(text)
                    if extent < width:
                        line.append(u" "*(width-extent))
                        extent = 0
                    else:
                        extent -= width
                assert extent == 0
                line.append(u" |\n")
                yield "".join(line)
            line = [u"-+-"]
            for width in widths:
                line.append(u"-"*width)
                line.append(u"-+-")
            line.append(u"\n")
            yield u"".join(line)
        body = product_to_text.body(self.data, widths)
        for row in body:
            line = []
            is_last_solid = False
            for chunk, is_solid in row:
                if is_last_solid or is_solid:
                    line.append(u" | ")
                else:
                    line.append(u" : ")
                line.append(chunk)
                is_last_solid = is_solid
            if is_last_solid:
                line.append(u" |\n")
            else:
                line.append(u" :\n")
            yield u"".join(line)
        yield u"\n"
        if (addon.debug and self.meta.plan is not None and
                self.meta.plan.statement is not None):
            yield u" ----\n"
            if self.meta.syntax:
                yield u" %s\n" % self.meta.syntax
            queue = [(0, self.meta.plan.statement)]
            while queue:
                depth, statement = queue.pop(0)
                sql = re.sub(ur'[\0-\x09\x0b-\x1f\x7f]', u'\ufffd',
                             statement.sql)
                if depth:
                    yield u"\n"
                for line in sql.splitlines():
                    yield u" "*(depth*2+1) + u"%s\n" % line
                for substatement in statement.substatements:
                    queue.append((depth+1, substatement))


class ToText(Adapter):

    adapt(Domain)

    def __init__(self, domain):
        self.domain = domain
        self.size = 1

    def __call__(self):
        return self

    def head_depth(self):
        return 0

    def head(self, depth):
        if not self.size or not depth:
            return
        yield [(u"", depth, self.size)]

    def body(self, data, widths):
        [width] = widths
        cell = self.dump(data)
        yield [(u"%*s" % (-width, cell), True)]

    def widths(self, data):
        return [len(self.dump(data))]

    def dump(self, value):
        if value is None:
            return u""
        return self.domain.dump(value)


class StringToText(ToText):

    adapt_many(StringDomain,
               EnumDomain)

    threshold = 32

    boundary_pattern = u"""(?<=\S) (?=\S)"""
    boundary_regexp = re.compile(boundary_pattern)

    unescaped_pattern = ur"""^(?=[^ "])[^\x00-\x1F]+(?<=[^ "])$"""
    unescaped_regexp = re.compile(unescaped_pattern)

    escape_pattern = ur"""[\x00-\x1F"\\]"""
    escape_regexp = re.compile(escape_pattern)
    escape_table = {
            u'\\': u'\\\\',
            u'"': u'\\"',
            u'\b': u'\\b',
            u'\f': u'\\f',
            u'\n': u'\\n',
            u'\r': u'\\r',
            u'\t': u'\\t',
    }

    def escape_replace(self, match):
        char = match.group()
        if char in self.escape_table:
            return self.escape_table[char]
        return u"\\u%04x" % ord(char)

    def escape(self, value):
        if self.unescaped_regexp.match(value):
            return value
        return u'"%s"' % self.escape_regexp.sub(self.escape_replace, value)

    def body(self, data, widths):
        [width] = widths
        if data is None:
            yield [(u" "*width, True)]
            return
        value = self.escape(data)
        if len(value) <= width:
            yield [(u"%*s" % (-width, value), True)]
            return
        chunks = self.boundary_regexp.split(value)
        best_badnesses = []
        best_lengths = []
        best_sizes = []
        for idx in range(len(chunks)):
            chunk = chunks[idx]
            best_badness = None
            best_size = None
            best_length = None
            length = len(chunk)
            size = 1
            while length <= width and idx-size >= -1:
                if size > idx:
                    badness = 0
                else:
                    tail = width - best_lengths[idx-size]
                    badness = best_badnesses[idx-size] + tail*tail
                if best_badness is None or best_badness > badness:
                    best_badness = badness
                    best_size = size
                    best_length = length
                if idx >= size:
                    length += len(chunks[idx-size]) + 1
                size += 1
            assert best_badness is not None and best_length <= width
            best_badnesses.append(best_badness)
            best_lengths.append(best_length)
            best_sizes.append(best_size)
        lines = []
        idx = len(chunks)
        while idx > 0:
            size = best_sizes[idx-1]
            group = u" ".join(chunks[idx-size:idx])
            assert len(group) <= width
            line = u"%*s" % (-width, group)
            lines.insert(0, line)
            idx -= size
        is_first = True
        for line in lines:
            yield [(line, is_first)]
            is_first = False

    def widths(self, data):
        if data is None:
            return [0]
        value = self.escape(data)
        if len(value) <= self.threshold:
            return [len(value)]
        chunks = self.boundary_regexp.split(value)
        max_length = max(len(chunk) for chunk in chunks)
        if max_length >= self.threshold:
            return [max_length]
        max_length = length = 0
        start = end = 0
        while end < len(chunks):
            length += len(chunks[end])
            if end != 0:
                length += 1
            end += 1
            while length > self.threshold:
                length -= len(chunks[start])
                if start != 0:
                    length -= 1
                start += 1
            assert start < end
            if length > max_length:
                max_length = length
        return [max_length]


class NativeStringToText(ToText):

    adapt_many(NumberDomain,
               DateDomain,
               TimeDomain)

    def dump(self, value):
        if value is None:
            return u""
        return unicode(value)


class NumberToText(NativeStringToText):

    adapt(NumberDomain)

    def body(self, data, widths):
        [width] = widths
        cell = self.dump(data)
        yield [(u"%*s" % (width, cell), True)]


class DecimalToText(ToText):

    adapt(DecimalDomain)

    def dump(self, value):
        if value is None:
            return u""
        sign, digits, exp = value.as_tuple()
        if not digits:
            return unicode(value)
        if exp < -6 and value == value.normalize():
            value = value.normalize()
            sign, digits, exp = value.as_tuple()
        if exp > 0:
            value = value.quantize(decimal.Decimal(1))
        return unicode(value)


class DateTimeToText(ToText):

    adapt(DateTimeDomain)

    def dump(self, value):
        if value is None:
            return u""
        elif not value.time():
            return unicode(value.date())
        else:
            return unicode(value)


class OpaqueToText(ToText):

    adapt(OpaqueDomain)

    def dump(self, value):
        if value is None:
            return u""
        if not isinstance(value, unicode):
            try:
                value = str(value).decode('utf-8')
            except UnicodeDecodeError:
                value = unicode(repr(value))
        return value


class VoidToText(ToText):

    adapt(VoidDomain)

    def __init__(self, domain):
        super(VoidToText, self).__init__(domain)
        self.size = 0


class RecordToText(ToText):

    adapt(RecordDomain)

    def __init__(self, domain):
        super(RecordToText, self).__init__(domain)
        self.fields_to_text = [profile_to_text(field)
                               for field in domain.fields]
        self.size = sum(field_to_text.size
                        for field_to_text in self.fields_to_text)

    def head_depth(self):
        if not self.size:
            return 0
        return max(field_to_text.head_depth()
                   for field_to_text in self.fields_to_text)

    def head(self, depth):
        if not self.size or not depth:
            return
        streams = [field_to_text.head(depth)
                   for field_to_text in self.fields_to_text]
        is_done = False
        while not is_done:
            is_done = True
            row = []
            for stream in streams:
                subrow = next(stream, None)
                if subrow is not None:
                    row.extend(subrow)
                    is_done = False
            if not is_done:
                yield row

    def body(self, data, widths):
        if not self.size:
            return
        dummies = [(u" "*width, False) for width in widths]
        if data is None:
            yield dummies
            return
        streams = []
        start = 0
        for field_to_text, item in zip(self.fields_to_text, data):
            size = field_to_text.size
            stream = field_to_text.body(item, widths[start:start+size])
            streams.append((stream, size))
            start += size
        is_done = False
        while not is_done:
            is_done = True
            row = []
            for stream, size in streams:
                subrow = next(stream, None)
                if subrow is not None:
                    row.extend(subrow)
                    is_done = False
                else:
                    row.extend(dummies[len(row):len(row)+size])
            if not is_done:
                yield row

    def widths(self, data):
        widths = []
        if data is None:
            data = [None]*self.size
        for item, field_to_text in zip(data, self.fields_to_text):
            widths += field_to_text.widths(item)
        return widths


class ListToText(ToText):

    adapt(ListDomain)

    def __init__(self, domain):
        self.item_to_text = to_text(domain.item_domain)
        self.size = self.item_to_text.size

    def head_depth(self):
        return self.item_to_text.head_depth()

    def head(self, depth):
        return self.item_to_text.head(depth)

    def body(self, data, widths):
        if not data:
            return
        for item in data:
            for row in self.item_to_text.body(item, widths):
                yield row

    def widths(self, data):
        widths = [0]*self.size
        if not data:
            data = [None]
        for item in data:
            widths = [max(width, item_width)
                      for width, item_width
                            in zip(widths, self.item_to_text.widths(item))]
        return widths


class MetaToText(object):

    def __init__(self, profile):
        self.profile = profile
        self.domain_to_text = to_text(profile.domain)
        self.size = self.domain_to_text.size

    def head_depth(self):
        depth = self.domain_to_text.head_depth()
        if self.profile.header:
            depth += 1
        return depth

    def head(self, depth):
        if not self.size or not depth:
            return
        if not self.profile.header:
            for row in self.domain_to_text.head(depth):
                yield row
            return
        domain_depth = self.domain_to_text.head_depth()
        if domain_depth > 0:
            head_depth = 1
        else:
            head_depth = depth
        yield [(self.profile.header, head_depth, self.size)]
        if domain_depth > 0:
            for row in self.domain_to_text.head(depth-1):
                yield row

    def body(self, data, widths):
        return self.domain_to_text.body(data, widths)

    def widths(self, data):
        if not self.size:
            return []
        widths = self.domain_to_text.widths(data)
        total = sum(widths) + 3*(self.size-1)
        if self.profile.header and len(self.profile.header) > total:
            extra = len(self.profile.header) - total
            inc = extra/self.size
            rem = extra - inc*self.size
            for idx in range(len(widths)):
                widths[idx] += inc
                if idx < rem:
                    widths[idx] += 1
        return widths


def to_text(domain):
    return ToText.__invoke__(domain)


def profile_to_text(profile):
    return MetaToText(profile)


