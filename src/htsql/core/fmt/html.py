#
# Copyright (c) 2006-2012, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql.core.fmt.html`
==========================

This module implements the HTML renderer.
"""


from ..adapter import Adapter, adapts, adapts_many
from .format import HTMLFormat, EmitHeaders, Emit
from .format import Format, Formatter, Renderer
from ..domain import (Domain, BooleanDomain, NumberDomain, DecimalDomain,
                      StringDomain, EnumDomain, DateDomain,
                      TimeDomain, DateTimeDomain, ListDomain, RecordDomain,
                      VoidDomain, Profile)
import cgi


class EmitHTMLHeaders(EmitHeaders):

    adapts(HTMLFormat)

    def __call__(self):
        yield ('Content-Type', 'text/html; charset=UTF-8')


class EmitHTML(Emit):

    adapts(HTMLFormat)

    def __call__(self):
        product_to_html = profile_to_html(self.meta)
        headers_height = product_to_html.headers_height()
        cells_height = product_to_html.cells_height(self.data)
        title = self.meta.header
        if not title:
            title = u""
        yield u"<!DOCTYPE html>\n"
        yield u"<html>\n"
        yield u"<head>\n"
        yield u"<meta http-equiv=\"Content-Type\"" \
              u" content=\"text/html; charset=UTF-8\">\n"
        yield u"<title>%s</title>\n" % cgi.escape(title)
        yield u"<style type=\"text/css\">\n"
        yield u"table.htsql-output {" \
              u" font-family: \"Arial\", sans-serif;" \
              u" font-size: 14px; line-height: 1.4; margin: 1em auto;" \
              u" color: #000000; background-color: #ffffff;" \
              u" border-collapse: collapse; border: 1px double #f2f2f2;" \
              u" -moz-box-shadow: 1px 1px 3px rgba(0,0,0,0.25);" \
              u" -webkit-box-shadow: 1px 1px 3px rgba(0,0,0,0.25);" \
              u" box-shadow: 1px 1px 3px rgba(0,0,0,0.25) }\n"
        yield u"table.htsql-output > thead {" \
              u" background-color: #f2f2f2 }\n"
        yield u"table.htsql-output > thead {" \
              u" border-color: #1a1a1a; border-width: 0 0 1px;" \
              u" border-style: solid }\n"
        yield u"table.htsql-output > thead > tr > th {" \
              u" font-weight: bold; padding: 0.2em 0.5em;" \
              u" text-align: center; vertical-align: bottom;" \
              u" overflow: hidden; word-wrap: break-word;" \
              u" border-color: #999999; border-width: 1px 1px 0;" \
              u" border-style: solid }\n"
        yield u"table.htsql-output > thead > tr" \
              u" > th.htsql-empty-header:after {" \
              u" content: \"\\A0\" }\n"
        yield u"table.htsql-output > tbody > tr.htsql-odd-row {" \
              u" background-color: #ffffff }\n"
        yield u"table.htsql-output > tbody > tr.htsql-even-row {" \
              u" background-color: #f2f2f2 }\n"
        yield "table.htsql-output > tbody > tr:hover {" \
              u" color: #ffffff; background-color: #333333 }\n"
        yield u"table.htsql-output > tbody > tr > td {" \
              u" padding: 0.2em 0.5em; vertical-align: baseline;" \
              u" overflow: hidden; word-wrap: break-word;" \
              u" border-color: #999999; border-width: 0 1px;" \
              u" border-style: solid; }\n"
        yield u"table.htsql-output > tbody > tr > td.htsql-index {" \
              u" font-size: 90%; font-weight: bold;" \
              u" text-align: right; width: 0; color: #999999;" \
              u" border-color: #1a1a1a;" \
              u" -moz-user-select: none; -webkit-user-select: none;"\
              u" user-select: none }\n"
        yield u"table.htsql-output > tbody > tr > td.htsql-integer-type {" \
              u" text-align: right }\n"
        yield u"table.htsql-output > tbody > tr > td.htsql-decimal-type {" \
              u" text-align: right }\n"
        yield u"table.htsql-output > tbody > tr > td.htsql-float-type {" \
              u" text-align: right }\n"
        yield u"table.htsql-output > tbody > tr" \
              u" > td.htsql-null-value:after {" \
              u" content: \"\\A0\" }\n"
        yield u"table.htsql-output > tbody > tr > td.htsql-empty-value {" \
              u" color: #999999 }\n"
        yield u"table.htsql-output > tbody > tr" \
              u" > td.htsql-empty-value:after {" \
              u" content: \"\\2B1A\" }\n"
        yield u"table.htsql-output > tbody > tr > td.htsql-false-value {" \
                u" font-style: italic }\n"
        yield u"table.htsql-output > tbody > tr" \
              u" > td.htsql-null-record-value {" \
              u" border-style: dashed }\n"
        yield u"</style>\n"
        yield u"</head>\n"
        yield u"<body>\n"
        if headers_height > 0 or cells_height > 0:
            yield u"<table class=\"htsql-output\" summary=\"%s\">\n" \
                    % cgi.escape(title, True)
            if headers_height > 0:
                yield u"<thead>\n"
                for row in product_to_html.headers(headers_height):
                    yield u"<tr>%s</tr>\n" % u"".join(row)
                yield u"</thead>\n"
            if cells_height > 0:
                yield u"<tbody>\n"
                index = 0
                for row in product_to_html.cells(self.data, cells_height):
                    index += 1
                    attributes = []
                    if index % 2:
                        attributes.append(" class=\"htsql-odd-row\"")
                    else:
                        attributes.append(" class=\"htsql-even-row\"")
                    yield u"<tr%s>%s</tr>\n" % (u"".join(attributes),
                                                u"".join(row))
                yield u"</tbody>\n"
            yield u"</table>\n"
        yield u"</body>\n"
        yield u"</html>\n"


class ToHTML(Adapter):

    adapts(Domain)

    def __init__(self, domain):
        assert isinstance(domain, Domain)
        self.domain = domain
        self.width = 1

    def __call__(self):
        return self

    def headers(self, height):
        if height > 0:
            attributes = []
            if height == 1:
                attributes.append(u" rowspan=\"%s\"" % height)
            attributes.append(u" class=\"htsql-empty-header\"")
            yield [u"<th%s></th>" % "".join(attributes)]

    def headers_height(self):
        return 0

    def cells(self, value, height):
        assert height > 0
        classes = []
        classes.append(u"htsql-%s-type" % self.domain.family)
        content = self.dump(value)
        if content is None:
            content = u""
            classes.append(u"htsql-null-value")
        elif not content:
            classes.append(u"htsql-empty-value")
        else:
            content = cgi.escape(content)
        classes.extend(self.classes(value))
        attributes = []
        if height > 1:
            attributes.append(u" rowspan=\"%s\"" % height)
        attributes.append(u" class=\"%s\"" % u" ".join(classes))
        yield [u"<td%s>%s</td>" % (u"".join(attributes), content)]

    def cells_height(self, value):
        return 1

    def classes(self, value):
        return []

    def dump(self, value):
        return self.domain.dump(value)


class VoidToHTML(ToHTML):

    adapts(VoidDomain)

    def __init__(self, domain):
        super(VoidToHTML, self).__init__(domain)
        self.width = 0

    def cells_height(self, value):
        return 0


class RecordToHTML(ToHTML):

    adapts(RecordDomain)

    def __init__(self, domain):
        super(RecordToHTML, self).__init__(domain)
        self.fields_to_html = [profile_to_html(field)
                               for field in domain.fields]
        self.width = sum(field_to_html.width
                         for field_to_html in self.fields_to_html)

    def headers(self, height):
        if not self.width:
            return
        streams = [field_to_html.headers(height)
                   for field_to_html in self.fields_to_html]
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

    def headers_height(self):
        if not self.width:
            return 0
        return max(field_to_html.headers_height()
                   for field_to_html in self.fields_to_html)

    def cells(self, value, height):
        if not self.width or not height:
            return
        if value is None:
            attributes = []
            if height > 1:
                attributes.append(u" rowspan=\"%s\"" % height)
            attributes.append(u" class=\"htsql-null-record-value\"")
            yield [u"<td%s></td>" % u"".join(attributes)]*self.width
        else:
            streams = [field_to_html.cells(item, height)
                       for item, field_to_html in zip(value,
                                                      self.fields_to_html)]
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

    def cells_height(self, value):
        if not self.width or value is None:
            return 0
        return max(field_to_html.cells_height(item)
                   for field_to_html, item in zip(self.fields_to_html, value))


class ListToHTML(ToHTML):

    adapts(ListDomain)

    def __init__(self, domain):
        super(ListToHTML, self).__init__(domain)
        self.item_to_html = to_html(domain.item_domain)
        self.width = self.item_to_html.width+1

    def headers(self, height):
        if height > 0:
            item_stream = self.item_to_html.headers(height)
            first_row = next(item_stream)
            attributes = []
            if height > 1:
                attributes.append(u" rowspan=\"%s\"" % height)
            attributes.append(" class=\"htsql-empty-header\"")
            first_row.insert(0, u"<th%s></th>" % u"".join(attributes))
            yield first_row
            for row in item_stream:
                yield row

    def headers_height(self):
        return self.item_to_html.headers_height()

    def cells(self, value, height):
        if not height:
            return
        if not value:
            attributes = []
            if self.width > 1:
                attributes.append(u" colspan=\"%s\"" % self.width)
            if height > 1:
                attributes.append(u" rowspan=\"%s\"" % height)
            attributes.append(u" class=\"htsql-null-value\"")
            yield [u"<td%s></td>" % u"".join(attributes)]
        items = iter(value)
        item = next(items)
        is_last = False
        total_height = height
        index = 1
        while not is_last:
            try:
                next_item = next(items)
            except StopIteration:
                next_item = None
                is_last = True
            item_height = max(1, self.item_to_html.cells_height(item))
            if is_last:
                item_height = total_height
            total_height -= item_height
            item_stream = self.item_to_html.cells(item, item_height)
            first_row = next(item_stream)
            attributes = []
            if item_height > 1:
                attributes.append(u" rowspan=\"%s\"" % height)
            attributes.append(u" class=\"htsql-index\"")
            first_row.insert(0, u"<td%s>%s</td>"
                                % (u"".join(attributes), index))
            yield first_row
            for row in item_stream:
                yield row
            item = next_item
            index += 1

    def cells_height(self, value):
        if not value:
            return 0
        return sum(max(1, self.item_to_html.cells_height(item))
                   for item in value)


class NativeToHTML(ToHTML):

    adapts_many(StringDomain,
                EnumDomain)

    def dump(self, value):
        return value
        if value == u"":
            return u"\u2B1A"
        return value


class NativeStringToHTML(ToHTML):

    adapts_many(NumberDomain,
                DateDomain,
                TimeDomain)

    def dump(self, value):
        if value is None:
            return None
        return unicode(value)


class DecimalToHTML(ToHTML):

    adapts(DecimalDomain)

    def dump(self, value):
        if value is None:
            return value
        sign, digits, exp = value.as_tuple()
        if not digits:
            return value
        if exp < -6:
            value = value.normalize()
            sign, digits, exp = value.as_tuple()
        if exp > 0:
            value = value.quantize(decimal.Decimal(1))
        return unicode(value)


class BooleanToHTML(ToHTML):

    adapts(BooleanDomain)

    def classes(self, value):
        if value is True:
            return [u"htsql-true-value"]
        if value is False:
            return [u"htsql-false-value"]

    def dump(self, value):
        if value is None:
            return None
        elif value is True:
            return u"true"
        elif value is False:
            return u"false"


class DateTimeToHTML(ToHTML):

    adapts(DateTimeDomain)

    def dump(self, value):
        if value is None:
            return None
        elif not value.time():
            return unicode(value.date())
        else:
            return unicode(value)


class MetaToHTML(object):

    def __init__(self, profile):
        assert isinstance(profile, Profile)
        self.profile = profile
        self.domain_to_html = to_html(profile.domain)
        self.width = self.domain_to_html.width
        self.header_level = self.domain_to_html.headers_height()+1

    def headers(self, height):
        if not self.width or not height:
            return
        if not self.profile.header:
            for row in self.domain_to_html.headers(height):
                yield row
            return
        content = cgi.escape(self.profile.header)
        attributes = []
        if self.width > 1:
            attributes.append(u" colspan=\"%s\"" % self.width)
        if height > 1 and self.header_level == 1:
            attributes.append(u" rowspan=\"%s\"" % height)
        if not content:
            attributes.append(u" class=\"htsql-empty-header\"")
        yield [u"<th%s>%s</th>" % (u"".join(attributes), content)]
        if self.header_level > 1:
            for row in self.domain_to_html.headers(height-1):
                yield row

    def headers_height(self):
        height = self.domain_to_html.headers_height()
        if self.profile.header:
            height += 1
        return height

    def cells(self, value, height):
        return self.domain_to_html.cells(value, height)

    def cells_height(self, value):
        return self.domain_to_html.cells_height(value)


def to_html(domain):
    to_html = ToHTML(domain)
    return to_html

def profile_to_html(profile):
    return MetaToHTML(profile)


class HTMLRenderer(Renderer):

    name = 'text/html'
    aliases = ['html']

    def render(self, product):
        status = self.generate_status(product)
        headers = self.generate_headers(product)
        body = list(self.generate_body(product))
        return status, headers, body

    def generate_status(self, product):
        return "200 OK"

    def generate_headers(self, product):
        format = HTMLFormat()
        emit_headers = EmitHeaders(format, product)
        return list(emit_headers())

    def generate_body(self, product):
        format = HTMLFormat()
        emit_body = Emit(format, product)
        for line in emit_body():
            if isinstance(line, unicode):
                line = line.encode('utf-8')
            yield line


