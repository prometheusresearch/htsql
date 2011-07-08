#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql.fmt.text`
=====================

This module implements the plain text renderer.
"""


from ..adapter import adapts
from ..util import maybe, oneof
from .format import Format, Formatter, Renderer
from .entitle import guess_title
from ..domain import (Domain, BooleanDomain, NumberDomain, IntegerDomain,
                      DecimalDomain, FloatDomain, StringDomain, EnumDomain,
                      DateDomain, TimeDomain, DateTimeDomain)
import re
import decimal
import datetime


class HeaderLayout(object):

    def __init__(self, text, row, column, colspan, rowspan):
        self.text = text
        self.row = row
        self.column = column
        self.colspan = colspan
        self.rowspan = rowspan


class Layout(object):

    def __init__(self, headers, total, column_widths):
        self.headers = headers
        self.total = total
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

    def calculate_header_layout(self, segment):
        headers = [guess_title(element) for element in segment.elements]
        layouts = []
        height = max(len(header) for header in headers)
        width = len(segment.elements)
        for line in range(height):
            index = 0
            while index < width:
                while index < width and len(headers[index]) <= line:
                    index += 1
                if index == width:
                    break
                colspan = 1
                if len(headers[index]) > line+1:
                    while (index+colspan < width and
                           len(headers[index+colspan]) > line+1 and
                           headers[index][:line+1] ==
                               headers[index+colspan][:line+1]):
                        colspan += 1
                rowspan = 1
                if len(headers[index]) == line+1:
                    rowspan = height-line
                title = headers[index][line]
                layout = HeaderLayout(title, line, index, colspan, rowspan)
                layouts.append(layout)
                index += colspan
        return layouts

    def calculate_layout(self, product, formats):
        segment = product.profile.binding.segment
        headers = self.calculate_header_layout(segment)
        column_widths = [1 for element in segment.elements]
        total = 0
        for record in product:
            for idx, (format, value) in enumerate(zip(formats, record)):
                width = format.measure(value)
                column_widths[idx] = max(column_widths[idx], width)
            total += 1
        if total == 0:
            total = "(no rows)"
        elif total == 1:
            total = "(1 row)"
        else:
            total = "(%s rows)" % total
        constraints = []
        constraints.append((0, len(segment.elements), len(total)-4))
        for header in headers:
            constraints.append((header.column, header.column+header.colspan,
                                len(header.text.decode('utf-8'))))
        constraints.reverse()
        for start, end, width in constraints:
            current_width = (end-start-1)*3
            for index in range(start, end):
                current_width += column_widths[index]
            if width <= current_width:
                continue
            extra = width-current_width
            inc = extra/(end-start)
            rem = extra - inc*(end-start)
            for index in range(start, end):
                column_widths[index] += inc
                if index < start+rem:
                    column_widths[index] += 1
        return Layout(headers, total, column_widths)

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
        if product.profile.segment.elements:
            height = max(header.row+header.rowspan
                         for header in layout.headers)
            for line in range(height):
                cells = []
                borders = []
                is_prior_solid = False
                is_solid = False
                idx = 0
                while idx < len(layout.column_widths):
                    headers = [header for header in layout.headers
                               if header.row <= line
                                             < header.row+header.rowspan and
                                  header.column == idx]
                    assert len(headers) == 1, headers
                    header = headers[0]
                    cell_width = (header.colspan-1)*3
                    for width in layout.column_widths[idx:idx+header.colspan]:
                        cell_width += width
                    if line < header.row+header.rowspan-1:
                        cell = " "*cell_width
                        border = " "*(cell_width+2)
                        is_solid = False
                    else:
                        cell = u"%-*s" % (cell_width,
                                          header.text.decode('utf-8'))
                        cell = cell.encode('utf-8')
                        border = "-"*(cell_width+2)
                        is_solid = True
                    cells.append(cell)
                    if is_prior_solid or is_solid:
                        borders.append("+")
                    else:
                        borders.append("|")
                    borders.append(border)
                    is_prior_solid = is_solid
                    idx += header.colspan
                if is_prior_solid:
                    borders.append("+")
                else:
                    borders.append("|")
                yield " | " + " | ".join(cells) + " |\n"
                if line < height-1:
                    yield " " + "".join(borders) + "\n"
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
        table_width = len(layout.column_widths)*3+1
        for width in layout.column_widths:
            table_width += width
        yield " " + "%*s" % (table_width, layout.total) + "\n"
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


