#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.fmt.html`
=====================

This module implements the HTML renderer.
"""


from ..adapter import adapts
from .format import Format, Formatter, Renderer
from .entitle import entitle
from ..domain import (Domain, BooleanDomain, NumberDomain,
                      StringDomain, EnumDomain, DateDomain,
                      TimeDomain, DateTimeDomain)
import cgi


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
        return [('Content-Type', 'text/html; charset=UTF-8')]

    def generate_body(self, product):
        for chunk in self.serialize_html(product):
            yield chunk

    def serialize_html(self, product):
        yield "<!DOCTYPE HTML PUBLIC"
        yield " \"-//W3C//DTD HTML 4.01 Transitional//EN\""
        yield " \"http://www.w3.org/TR/html4/loose.dtd\">\n"
        yield "<html>\n"
        yield "<head>\n"
        for chunk in self.serialize_head(product):
            yield chunk
        yield "</head>\n"
        yield "<body>\n"
        for chunk in self.serialize_body(product):
            yield chunk
        yield "</body>\n"
        yield "</html>\n"

    def serialize_head(self, product):
        title = str(product.profile.syntax)
        yield "<meta http-equiv=\"Content-Type\""
        yield " content=\"text/html; charset=UTF-8\">\n"
        yield "<title>%s</title>\n" % cgi.escape(title)
        yield "<style type=\"text/css\">\n"
        for chunk in self.serialize_style():
            yield chunk
        yield "</style>\n"

    def serialize_style(self):
        yield "body { font-family: sans-serif; font-size: 90%;"
        yield " color: #515151; background: #ffffff }\n"
        yield "a:link, a:visited { color: #1f4884; text-decoration: none }\n"
        yield "a:hover { text-decoration: underline }\n"
        yield "table { border-collapse: collapse;"
        yield " margin: 0.5em auto; width: 100% }\n"
        yield "table, tr { border-style: solid; border-width: 0 }\n"
        yield "td, th { padding: 0.2em 0.5em; vertical-align: top;"
        yield " text-align: left }\n"
        yield "div.tab { position: relative; left: -1px; margin-right: 60%;"
        yield " padding: 0.2em 0.5em; background: #ffffff;"
        yield " border-style: solid; border-width: 5px 1px 0;"
        yield " border-top-left-radius: 10px; border-top-right-radius: 10px;"
        yield " -moz-border-radius-topleft: 10px;"
        yield " -moz-border-radius-topright: 10px;"
        yield " -webkit-border-top-left-radius: 10px;"
        yield " -webkit-border-top-right-radius: 10px }\n"
        yield "table.page { border: 0; padding: 1em; width: auto }\n"
        yield "tr.content { padding: 1em 1em 0.5em }\n"
        yield "tr.footer { padding: 0 1em 1em; text-align: left;"
        yield " font-style: italic }\n"
        yield "table.chart .number { text-align: right }\n"
        yield "tr.caption { font-size: 105%; background: transparent }\n"
        yield "tr.caption th { padding: 0 }\n"
        yield "div.tab { border-color: #6f9ad3 #c3c3c3 }\n"
        yield "tr.header { background: #dae3ea; border-color: #c3c3c3;"
        yield " border-width: 1px 1px 0 }\n"
        yield "tr.odd { background: #ffffff; border-color: #c3c3c3;"
        yield " border-width: 0 1px }\n"
        yield "tr.even { background: #f2f2f2; border-color: #c3c3c3;"
        yield " border-width: 0 1px }\n"
        yield "tr.odd:hover, tr.even:hover { background: #ffe3bd }\n"
        yield "tr.total { background: transparent;"
        yield "border-color: #c3c3c3; border-width: 1px 0 0 }\n"
        yield "tr.total td { text-align: right; font-size: 75%;"
        yield " font-style: italic; padding: 0.3em 0.5em 0 }\n"
        yield "table.void { text-align: center;"
        yield" border-color: #c3c3c3; border-width: 1px 0 }\n"

    def serialize_body(self, product):
        title = str(product.profile.syntax)
        yield "<table class=\"page\" summary=\"%s\">\n" % cgi.escape(title)
        yield "<tr>\n"
        yield "<td class=\"content\">\n"
        if product:
            for chunk in self.serialize_content(product):
                yield chunk
        else:
            for chunk in self.serialize_no_content():
                yield chunk
        yield "</td>\n"
        yield "</tr>\n"
        yield "<tr><td class=\"footer\">%s</td></tr>\n" % cgi.escape(title)
        yield "</table>\n"

    def serialize_no_content(self):
        yield "<table class=\"void\">\n"
        yield "<tr><td>no data</td></tr>\n"
        yield "</table>\n"

    def serialize_content(self, product):
        segment = product.profile.binding.segment
        caption = entitle(segment.base)
        headers = [entitle(element) for element in segment.elements]
        width = len(segment.elements)
        domains = [element.domain for element in segment.elements]
        tool = HTMLFormatter(self)
        formats = [Format(self, domain, tool) for domain in domains]
        colspan = " colspan=\"%s\"" % width if width > 1 else ""
        yield "<table class=\"chart\" summary=\"%s\">\n" % cgi.escape(caption)
        yield "<tr class=\"caption\">"
        yield ("<th%s><div class=\"tab\">%s</div></th>"
                % (colspan, cgi.escape(caption)))
        yield "</tr>\n"
        if width:
            yield "<tr class=\"header\">"
            for (header, format) in zip(headers, formats):
                style = (" class=\"%s\"" % format.style
                         if format.style is not None else "")
                yield "<th%s>%s</th>" % (style, cgi.escape(header))
            yield "</tr>\n"
        is_odd = False
        total = 0
        for record in product:
            total += 1
            is_odd = not is_odd
            if width:
                if is_odd:
                    style = " class=\"odd\""
                else:
                    style = " class=\"even\""
                yield "<tr%s>" % style
                for value, format in zip(record, formats):
                    style = (" class=\"%s\"" % format.style
                             if format.style is not None else "")
                    output = format(value)
                    yield "<td%s>%s</td>" % (style, output)
                yield "</tr>\n"
        if total == 0:
            total = "(no rows)"
        elif total == 1:
            total = "(1 row)"
        else:
            total = "(%s rows)" % total
        yield "<tr class=\"total\"><td%s>%s</td></tr>" % (colspan, total)
        yield "</table>"


class HTMLFormatter(Formatter):

    adapts(HTMLRenderer)


class FormatDomain(Format):

    adapts(HTMLRenderer, Domain)

    style = None

    def format_null(self):
        return "<em>&mdash;</em>"

    def __call__(self, value):
        if value is None:
            return self.format_null()
        if isinstance(value, str):
            try:
                value.decode('utf-8')
            except UnicodeDecodeError:
                value = repr(value)
        elif isinstance(value, unicode):
            value = value.encode('utf-8')
        else:
            value = str(value)
        return "<em>%s</em>" % cgi.escape(value)


class FormatBoolean(Format):

    adapts(HTMLRenderer, BooleanDomain)

    def __call__(self, value):
        if value is None:
            return self.format_null()
        if value is True:
            return "<em>true</em>"
        if value is False:
            return "<em>false</em>"


class FormatNumber(Format):

    adapts(HTMLRenderer, NumberDomain)

    style = 'number'

    def __call__(self, value):
        if value is None:
            return self.format_null()
        return str(value)


class FormatString(Format):

    adapts(HTMLRenderer, StringDomain)

    def __call__(self, value):
        if value is None:
            return self.format_null()
        return cgi.escape(value)


class FormatEnum(Format):

    adapts(HTMLRenderer, EnumDomain)

    def __call__(self, value):
        if value is None:
            return self.format_null()
        return cgi.escape(value)


class FormatDate(Format):

    adapts(HTMLRenderer, DateDomain)

    def __call__(self, value):
        if value is None:
            return self.format_null()
        return str(value)


class FormatTime(Format):

    adapts(HTMLRenderer, TimeDomain)

    def __call__(self, value):
        if value is None:
            return self.format_null()
        return str(value)


class FormatDateTime(Format):

    adapts(HTMLRenderer, DateTimeDomain)

    def __call__(self, value):
        if value is None:
            return self.format_null()
        if not value.time():
            return str(value.date())
        return str(value)


