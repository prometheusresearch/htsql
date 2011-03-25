#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.fmt.spreadsheet`
============================

This module implements the CSV renderer.
"""


from ..adapter import adapts
from .format import Format, Formatter, Renderer
from ..domain import (Domain, BooleanDomain, NumberDomain, FloatDomain,
                      StringDomain, EnumDomain, DateDomain, TimeDomain,
                      DateTimeDomain)
from .entitle import entitle
import csv
import cStringIO


class CSVRenderer(Renderer):

    name = 'text/csv'
    aliases = ['csv']

    def render(self, product):
        status = self.generate_status(product)
        headers = self.generate_headers(product)
        body = self.generate_body(product)
        return status, headers, body

    def generate_status(self, product):
        return "200 OK"

    def generate_headers(self, product):
        filename = str(product.profile.segment.syntax)
        filename = filename.replace('\\', '\\\\').replace('"', '\\"')
        return [('Content-Type', 'text/csv; charset=UTF-8'),
                ('Content-Disposition',
                 'attachment; filename="(%s).csv"' % filename)]

    def generate_body(self, product):
        if not product:
            return
        titles = [entitle(element.binding)
                  for element in product.profile.segment.elements]
        domains = [element.domain
                   for element in product.profile.segment.elements]
        tool = Formatter(self)
        formats = [Format(self, domain, tool) for domain in domains]
        output = cStringIO.StringIO()
        writer = csv.writer(output)
        writer.writerow(titles)
        yield output.getvalue()
        output.seek(0)
        output.truncate()
        for record in product:
            items = [format(value)
                     for format, value in zip(formats, record)]
            writer.writerow(items)
            yield output.getvalue()
            output.seek(0)
            output.truncate()


class CSVFormatter(Formatter):

    adapts(CSVRenderer)


class FormatDomain(Format):

    adapts(CSVRenderer, Domain)

    def __call__(self, value):
        if value is None:
            return ""
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        value = str(value)
        try:
            value.decode('utf-8')
        except UnicodeDecodeError:
            value = repr(value)
        return value


class FormatBoolean(Format):

    adapts(CSVRenderer, BooleanDomain)

    def __call__(self, value):
        if value is None:
            return ""
        if value is True:
            return "true"
        if value is False:
            return "false"


class FormatNumber(Format):

    adapts(CSVRenderer, NumberDomain)

    def __call__(self, value):
        if value is None:
            return ""
        return str(value)


class FormatFloat(Format):

    adapts(CSVRenderer, FloatDomain)

    def __call__(self, value):
        if value is None:
            return ""
        return repr(value)


class FormatString(Format):

    adapts(CSVRenderer, StringDomain)

    def __call__(self, value):
        if value is None:
            return ""
        return value


class FormatEnum(Format):

    adapts(CSVRenderer, EnumDomain)

    def __call__(self, value):
        if value is None:
            return ""
        return value


class FormatDate(Format):

    adapts(CSVRenderer, DateDomain)

    def __call__(self, value):
        if value is None:
            return ""
        return str(value)


class FormatTime(Format):

    adapts(CSVRenderer, TimeDomain)

    def __call__(self, value):
        if value is None:
            return ""
        return str(value)


class FormatDateTime(Format):

    adapts(CSVRenderer, DateTimeDomain)

    def __call__(self, value):
        if value is None:
            return ""
        if not value.time():
            return str(value.date())
        return str(value)


