#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.fmt.json`
=====================

This module implements the JSON renderer.
"""


from ..adapter import adapts
from .format import Format, Formatter, Renderer
from ..domain import (Domain, BooleanDomain, NumberDomain, FloatDomain,
                      StringDomain, EnumDomain, DateDomain)
import re


class JSONRenderer(Renderer):

    # Note: see `http://www.ietf.org/rfc/rfc4627.txt`.
    name = 'application/json'
    aliases = ['json']

    def render(self, product):
        status = self.generate_status(product)
        headers = self.generate_headers(product)
        body = self.generate_body(product)
        return status, headers, body

    def generate_status(self, product):
        return "200 OK"

    def generate_headers(self, product):
        return [('Content-Type', 'application/json')]

    def generate_body(self, product):
        if not product:
            yield "[]\n"
            return
        domains = [element.domain
                   for element in product.profile.segment.elements]
        tool = JSONFormatter(self)
        formats = [Format(self, domain, tool) for domain in domains]
        record = None
        for next_record in product:
            if record is not None:
                items = [format(value)
                         for format, value in zip(formats, record)]
                yield "  [%s],\n" % ", ".join(items)
            else:
                yield "[\n"
            record = next_record
        if record is not None:
            items = [format(value)
                     for format, value in zip(formats, record)]
            yield "  [%s]\n" % ", ".join(items)
            yield "]\n"
        else:
            yield "[]\n"


class JSONFormatter(Formatter):

    adapts(JSONRenderer)


class FormatDomain(Format):

    adapts(JSONRenderer, Domain)

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

    def replace(self, match):
        char = match.group()
        if char in self.escape_table:
            return '\\'+self.escape_table[char]
        return '\\u%04X' % ord(char)

    def escape(self, value):
        value = value.decode('utf-8')
        value = self.escape_regexp.sub(self.replace, value)
        value = value.encode('utf-8')
        return '"%s"' % value

    def __call__(self, value):
        if value is None:
            return "null"
        return "\"?\""


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
        return self.escape(value)


class FormatEnum(Format):

    adapts(JSONRenderer, EnumDomain)

    def __call__(self, value):
        if value is None:
            return "null"
        return self.escape(value)


class FormatDate(Format):

    adapts(JSONRenderer, DateDomain)

    def __call__(self, value):
        if value is None:
            return "null"
        return str(value)


