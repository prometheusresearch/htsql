#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.fmt.text`
=====================

This module implements the plain text renderer.
"""


from ..adapter import adapts
from ..util import maybe, oneof
from .format import Format, Formatter, Renderer
from .entitle import entitle
from ..domain import (Domain, BooleanDomain, NumberDomain, IntegerDomain,
                      DecimalDomain, FloatDomain, StringDomain, EnumDomain,
                      DateDomain, TimeDomain, DateTimeDomain)
import re
import decimal
import datetime


class Layout(object):

    def __init__(self, caption, headers, total, table_width, column_widths):
        self.caption = caption
        self.headers = headers
        self.total = total
        self.table_width = table_width
        self.column_widths = column_widths


class TextRenderer(Renderer):

    name = 'text/plain'
    aliases = ['txt', '']

    def render(self, product):
        status = self.generate_status(product)
        headers = self.generate_headers(product)
        body = list(self.generate_body(product))
        return status, headers, body

    def generate_status(self, product):
        return "200 OK"

    def generate_headers(self, product):
        return [('Content-Type', 'text/plain; charset=UTF-8')]

    def calculate_layout(self, product, formats):
        segment = product.profile.binding.segment
        caption = entitle(segment.base).decode('utf-8')
        headers = [entitle(element).decode('utf-8')
                   for element in segment.elements]
        column_widths = [len(header) for header in headers]
        total = 0
        for record in product:
            for idx, (format, value) in enumerate(zip(formats, record)):
                width = format.measure(value)
                column_widths[idx] = max(column_widths[idx], width)
            total += 1
        table_width = len(caption)
        if total == 0:
            total = u"(no rows)"
        elif total == 1:
            total = u"(1 row)"
        else:
            total = u"(%s rows)" % total
        table_width = max(table_width, len(total)-2)
        if formats:
            columns_width = sum(column_widths)+3*(len(formats)-1)
            table_width = max(table_width, columns_width)
            if columns_width < table_width:
                extra = table_width-columns_width
                inc = extra/len(formats)
                rem = extra - inc*len(formats)
                for idx in range(len(formats)):
                    column_widths[idx] += inc
                    if idx < rem:
                        column_widths[idx] += 1
        caption = (u"%*s" % (-table_width, caption)).encode('utf-8')
        headers = [(u"%*s" % (-width, header)).encode('utf-8')
                   for width, header in zip(column_widths, headers)]
        total = (u"%*s" % (table_width+4, total)).encode('utf-8')
        return Layout(caption, headers, total, table_width, column_widths)

    def generate_body(self, product):
        request_title = str(product.profile.syntax)
        if not product:
            yield "(no data)\n"
            yield "\n"
            yield " ----\n"
            yield " %s\n" % request_title
            return
        domains = [element.domain
                   for element in product.profile.segment.elements]
        tool = TextFormatter(self)
        formats = [Format(self, domain, tool) for domain in domains]
        layout = self.calculate_layout(product, formats)
        yield " | " + layout.caption + " |\n"
        yield "-+-" + "-"*layout.table_width + "-+-\n"
        if product.profile.segment.elements:
            yield (" | " +
                   " | ".join(header for header in layout.headers) +
                   " |\n")
            yield ("-+-" +
                   "-+-".join("-"*width for width in layout.column_widths) +
                   "-+-\n")
            for record in product:
                columns = [format(value, width)
                           for format, value, width
                                in zip(formats, record, layout.column_widths)]
                height = max(len(column) for column in columns)
                for row_idx in range(height):
                    if row_idx == 0:
                        left, mid, right = " | ", " | ", " |\n"
                    else:
                        left, mid, right = " : ", " : ", " :\n"
                    cells = []
                    for idx, column in enumerate(columns):
                        if row_idx < len(column):
                            cell = column[row_idx]
                        else:
                            cell = " "*layout.column_widths[idx]
                        cells.append(cell)
                    if row_idx == 0:
                        yield " | " + " | ".join(cells) + " |\n"
                    else:
                        yield " : " + " : ".join(cells) + " :\n"
        yield " " + layout.total + "\n"
        yield "\n"
        yield " ----\n"
        yield " %s\n" % request_title
        for line in product.profile.plan.sql.splitlines():
            yield " %s\n" % line


class TextFormatter(Formatter):

    adapts(TextRenderer)


class FormatDomain(Format):

    adapts(TextRenderer, Domain)

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

    def escape_string(self, value):
        if self.unescaped_regexp.match(value):
            return value
        return u'"%s"' % self.escape_regexp.sub(self.escape_replace, value)

    def format_null(self, width):
        return [" "*width]

    def measure(self, value):
        if value is None:
            return 0
        if not isinstance(value, unicode):
            try:
                value = self.escape_string(str(value).decode('utf-8'))
            except UnicodeDecodeError:
                value = unicode(repr(value))
        return len(value)

    def __call__(self, value, width):
        if value is None:
            return self.format_null(width)
        if not isinstance(value, unicode):
            try:
                value = self.escape_string(str(value).decode('utf-8'))
            except UnicodeDecodeError:
                value = unicode(repr(value))
        line = u"%*s" % (-width, value)
        return [line.encode('utf-8')]


class FormatBoolean(Format):

    adapts(TextRenderer, BooleanDomain)

    def measure(self, value):
        if value is None:
            return 0
        if value is True:
            return 4
        if value is False:
            return 5

    def __call__(self, value, width):
        assert isinstance(value, maybe(bool))
        if value is None:
            return self.format_null(width)
        if value is True:
            return ["%*s" % (-width, "true")]
        if value is False:
            return ["%*s" % (-width, "false")]


class FormatNumber(Format):

    adapts(TextRenderer, NumberDomain)

    def measure(self, value):
        if value is None:
            return 0
        return len(str(value))

    def __call__(self, value, width):
        if value is None:
            return self.format_null(width)
        return ["%*s" % (width, value)]


class FormatInteger(Format):

    adapts(TextRenderer, IntegerDomain)

    def __call__(self, value, width):
        assert isinstance(value, maybe(oneof(int, long)))
        return super(FormatInteger, self).__call__(value, width)


class FormatDecimal(Format):

    adapts(TextRenderer, DecimalDomain)

    def __call__(self, value, width):
        assert isinstance(value, maybe(decimal.Decimal))
        return super(FormatDecimal, self).__call__(value, width)


class FormatFloat(Format):

    adapts(TextRenderer, FloatDomain)

    def __call__(self, value, width):
        assert isinstance(value, maybe(float))
        return super(FormatFloat, self).__call__(value, width)


class FormatString(Format):

    adapts(TextRenderer, StringDomain)

    threshold = 32

    boundary_pattern = u"""(?<=\S) (?=\S)"""
    boundary_regexp = re.compile(boundary_pattern)

    def measure(self, value):
        if value is None:
            return 0
        value = value.decode('utf-8')
        value = self.escape_string(value)
        if len(value) <= self.threshold:
            return len(value)
        chunks = self.boundary_regexp.split(value)
        max_length = max(len(chunk) for chunk in chunks)
        if max_length >= self.threshold:
            return max_length
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
        return max_length

    def __call__(self, value, width):
        assert isinstance(value, maybe(str))
        if value is None:
            return self.format_null(width)
        value = value.decode('utf-8')
        value = self.escape_string(value)
        if len(value) <= width:
            line = u"%*s" % (-width, value)
            return [line.encode('utf-8')]
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
            lines.insert(0, line.encode('utf-8'))
            idx -= size
        return lines


class FormatEnum(Format):

    adapts(TextRenderer, EnumDomain)

    def measure(self, value):
        if value is None:
            return 0
        value = value.decode('utf-8')
        value = self.escape_string(value)
        return len(value)

    def __call__(self, value, width):
        assert isinstance(value, maybe(str))
        if value is None:
            return self.format_null(width)
        value = value.decode('utf-8')
        value = self.escape_string(value)
        line = u"%*s" % (-width, value)
        return [line.encode('utf-8')]


class FormatDate(Format):

    adapts(TextRenderer, DateDomain)

    def measure(self, value):
        if value is None:
            return 0
        return 10

    def __call__(self, value, width):
        assert isinstance(value, maybe(datetime.date))
        if value is None:
            return self.format_null(width)
        return ["%*s" % (-width, value)]


class FormatTime(Format):

    adapts(TextRenderer, TimeDomain)

    def measure(self, value):
        if value is None:
            return 0
        return len(str(value))

    def __call__(self, value, width):
        assert isinstance(value, maybe(datetime.time))
        if value is None:
            return self.format_null(width)
        return ["%*s" % (-width, value)]


class FormatDateTime(Format):

    adapts(TextRenderer, DateTimeDomain)

    def measure(self, value):
        if value is None:
            return 0
        if not value.time():
            return 10
        return len(str(value))

    def __call__(self, value, width):
        assert isinstance(value, maybe(datetime.datetime))
        if value is None:
            return self.format_null(width)
        if not value.time():
            return ["%*s" % (-width, value.date())]
        return ["%*s" % (-width, value)]


