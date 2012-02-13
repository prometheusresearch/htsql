#
# Copyright (c) 2006-2012, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql.core.fmt.tabular`
=============================

This module implements the CSV and TSV renderers.
"""


from ..util import listof
from ..adapter import Adapter, adapts, adapts_many
from .format import Emit, EmitHeaders, CSVFormat
from .format import Format, Formatter, Renderer
from ..domain import (Domain, BooleanDomain, NumberDomain, FloatDomain,
                      DecimalDomain, StringDomain, EnumDomain, DateDomain,
                      TimeDomain, DateTimeDomain, ListDomain, RecordDomain,
                      VoidDomain, Profile)
import csv
import cStringIO


class EmitCSVHeaders(EmitHeaders):

    adapts(CSVFormat)

    content_types = {
            'excel': 'text/csv; charset=UTF-8',
            'excel-tab': 'text/tab-separated-values; charset=UTF-8',
    }
    extensions = {
            'excel': 'csv',
            'excel-tab': 'tsv',
    }

    def __call__(self):
        content_type = self.content_types[self.format.dialect]
        extension = self.extensions[self.format.dialect]
        filename = None
        if self.meta.header:
            filename = self.meta.header.encode('utf-8')
        if not filename:
            filename = '_'
        filename = filename.replace('\\', '\\\\').replace('"', '\\"')
        yield ('Content-Type', content_type)
        yield ('Content-Disposition',
               'attachment; filename="%s.%s"' % (filename, extension))


class EmitCSV(Emit):

    adapts(CSVFormat)

    def __call__(self):
        product_to_csv = to_csv(self.meta.domain, [self.meta])
        if not product_to_csv.width:
            return
        headers = product_to_csv.headers()
        to_cells = product_to_csv.cells
        assert len(headers) == product_to_csv.width
        output = cStringIO.StringIO()
        writer = csv.writer(output, dialect=self.format.dialect)
        writer.writerow([header.encode('utf-8') if header is not None else ""
                         for header in headers])
        yield output.getvalue()
        output.seek(0)
        output.truncate()
        for row in to_cells(self.data):
            writer.writerow([item.encode('utf-8') if item is not None else ""
                             for item in row])
            yield output.getvalue()
            output.seek(0)
            output.truncate()


class ToCSV(Adapter):

    adapts(Domain)

    def __init__(self, domain, profiles):
        assert isinstance(domain, Domain)
        assert isinstance(profiles, listof(Profile)) and len(profiles) > 0
        self.domain = domain
        self.profiles = profiles
        self.width = 1

    def __call__(self):
        return self

    def headers(self):
        return [self.profiles[-1].header]

    def cells(self, value):
        if value is None:
            return [None]
        else:
            return [self.domain.dump(value)]


class VoidToCSV(ToCSV):

    adapts(VoidDomain)

    def __init__(self, domain, profiles):
        super(VoidToCSV, self).__init__(domain, profiles)
        self.width = 0

    def headers(self):
        return []

    def cells(self):
        if False:
            yield []


class RecordToCSV(ToCSV):

    adapts(RecordDomain)

    def __init__(self, domain, profiles):
        super(RecordToCSV, self).__init__(domain, profiles)
        self.fields_to_csv = [to_csv(field.domain, profiles+[field])
                              for field in domain.fields]
        self.width = 0
        for field_to_csv in self.fields_to_csv:
            self.width += field_to_csv.width

    def headers(self):
        row = []
        for field_to_csv in self.fields_to_csv:
            row.extend(field_to_csv.headers())
        return row

    def cells(self, value):
        if not self.width:
            return
        if value is None:
            yield [None]*self.width
        else:
            streams = [(field_to_csv.cells(item), field_to_csv.width)
                       for item, field_to_csv in zip(value, self.fields_to_csv)]
            is_done = False
            while not is_done:
                is_done = True
                row = []
                for stream, width in streams:
                    subrow = next(stream, None)
                    if subrow is None:
                        subrow = [None]*width
                    else:
                        is_done = False
                    row.extend(subrow)
                if not is_done:
                    yield row


class ListToCSV(ToCSV):

    adapts(ListDomain)

    def __init__(self, domain, profiles):
        super(ListToCSV, self).__init__(domain, profiles)
        self.item_to_csv = to_csv(domain.item_domain, profiles)
        self.width = self.item_to_csv.width

    def headers(self):
        return self.item_to_csv.headers()

    def cells(self, value):
        if not self.width:
            return
        if value is not None:
            item_to_cells = self.item_to_csv.cells
            for item in value:
                for row in item_to_cells(item):
                    yield row


class BooleanToCSV(ToCSV):

    adapts(BooleanDomain)

    def cells(self, value):
        if value is None:
            yield [None]
        elif value is True:
            yield [u"1"]
        elif value is False:
            yield [u"0"]


class NumberToCSV(ToCSV):

    adapts(NumberDomain)

    def cells(self, value):
        if value is None:
            yield [None]
        else:
            yield [unicode(value)]


class FloatToCSV(ToCSV):

    adapts(FloatDomain)

    def cells(self, value):
        if value is None or str(value) in ['inf', '-inf', 'nan']:
            yield [None]
        else:
            yield [unicode(value)]


class DecimalToCSV(ToCSV):

    adapts(DecimalDomain)

    def cells(self, value):
        if value is None or not value.is_finite():
            yield [None]
        else:
            yield [unicode(value)]


class StringToCSV(ToCSV):

    adapts_many(StringDomain,
                EnumDomain)

    def cells(self, value):
        yield [value]


class DateToCSV(ToCSV):

    adapts(DateDomain)

    def cells(self, value):
        if value is None:
            yield [None]
        else:
            yield [unicode(value)]


class TimeToCSV(ToCSV):

    adapts(TimeDomain)

    def cells(self, value):
        if value is None:
            yield [None]
        else:
            yield [unicode(value)]


class DateTimeToCSV(ToCSV):

    adapts(DateTimeDomain)

    def cells(self, value):
        if value is None:
            yield [None]
        elif not value.time():
            yield [unicode(value.date())]
        else:
            yield [unicode(value)]


def to_csv(domain, profiles):
    to_csv = ToCSV(domain, profiles)
    return to_csv()


class CSVRenderer(Renderer):

    name = 'text/csv'
    aliases = ['csv']
    content_type = 'text/csv'
    extension = 'csv'
    dialect = 'excel'

    def render(self, product):
        status = self.generate_status(product)
        headers = self.generate_headers(product)
        body = self.generate_body(product)
        return status, headers, body

    def generate_status(self, product):
        return "200 OK"

    def generate_headers(self, product):
        format = CSVFormat(self.dialect)
        emit_headers = EmitHeaders(format, product)
        return list(emit_headers())

    def generate_body(self, product):
        format = CSVFormat(self.dialect)
        emit = Emit(format, product)
        return emit()


class TSVRenderer(CSVRenderer):

    name = 'text/tab-separated-values'
    aliases = ['tsv']
    content_type = 'text/tab-separated-values'
    extension = 'tsv'
    dialect = 'excel-tab'


