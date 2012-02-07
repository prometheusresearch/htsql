#
# Copyright (c) 2006-2012, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql.core.fmt.spreadsheet`
=================================

This module implements the CSV and TSV renderers.
"""


from ..adapter import Adapter, adapts, adapts_many
from .format import Emit, EmitHeaders, CSVFormat
from .format import Format, Formatter, Renderer
from ..domain import (Domain, BooleanDomain, NumberDomain, FloatDomain,
                      DecimalDomain, StringDomain, EnumDomain, DateDomain,
                      TimeDomain, DateTimeDomain, ListDomain, RecordDomain,
                      VoidDomain)
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
        return [('Content-Type', content_type),
                ('Content-Disposition',
                 'attachment; filename="%s.%s"' % (filename, extension))]


class EmitCSV(Emit):

    adapts(CSVFormat)

    def __call__(self):
        headers = to_csv_headers(self.meta.domain, [self.meta.header])
        if not headers:
            return
        row_maker = to_csv(self.meta.domain)
        assert len(headers) == row_maker.width
        output = cStringIO.StringIO()
        writer = csv.writer(output, dialect=self.format.dialect)
        writer.writerow([header.encode('utf-8') if header is not None else ""
                         for header in headers])
        yield output.getvalue()
        output.seek(0)
        output.truncate()
        for row in row_maker(self.data):
            writer.writerow([item.encode('utf-8') if item is not None else ""
                             for item in row])
            yield output.getvalue()
            output.seek(0)
            output.truncate()


class ToCSVHeaders(Adapter):

    adapts(Domain)

    def __init__(self, domain, headers):
        self.domain = domain
        self.headers = headers

    def __call__(self):
        return [self.headers[-1]]


class RecordToCSVHeaders(ToCSVHeaders):

    adapts(RecordDomain)

    def __call__(self):
        row = []
        for field in self.domain.fields:
            row.extend(to_csv_headers(field.domain,
                                      self.headers+[field.header]))
        return row


class ListToCSVHeaders(ToCSVHeaders):

    adapts(ListDomain)

    def __call__(self):
        return to_csv_headers(self.domain.item_domain, self.headers)


class VoidToCSVHeaders(ToCSVHeaders):

    adapts(VoidDomain)

    def __call__(self):
        return []


class ToCSV(Adapter):

    adapts(Domain)

    def __init__(self, domain):
        self.domain = domain

    def __call__(self):
        def serialize(value, dump=self.domain.dump):
            if value is None:
                return [None]
            else:
                return [dump(value)]
        serialize.width = 1
        return serialize


class RecordToCSV(ToCSV):

    adapts(RecordDomain)

    def __call__(self):
        width = 0
        serializers = []
        for field in self.domain.fields:
            serializer = to_csv(field.domain)
            serializers.append(serializer)
            width += serializer.width
        if not width:
            def serialize_null_record(value):
                return iter([])
            serialize_null_record.width = 0
            return serialize_null_record
        else:
            def serialize_record(value, serializers=serializers, width=width):
                if value is None:
                    yield [None]*width
                else:
                    iterators = [serialize(item)
                                 for item, serialize in zip(value, serializers)]
                    is_done = False
                    while not is_done:
                        is_done = True
                        row = []
                        for iterator, serializer in zip(iterators, serializers):
                            subrow = next(iterator, None)
                            if subrow is None:
                                subrow = [None]*serializer.width
                            else:
                                is_done = False
                            row.extend(subrow)
                        if not is_done:
                            yield row
            serialize_record.width = width
            return serialize_record


class ListToCSV(ToCSV):

    def __call__(self):
        serializer = to_csv(self.domain.item_domain)
        if not serializer.width:
            def serialize_null_list(value):
                return iter([])
            serialize_null_list.width = 0
            return serialize_null_list
        else:
            def serialize_list(value, serializer=serializer):
                if value is not None:
                    for item in value:
                        for row in serializer(item):
                            yield row
            serialize_list.width = serializer.width
            return serialize_list


class BooleanToCSV(ToCSV):

    adapts(BooleanDomain)

    def __call__(self):
        def serialize_boolean(value):
            if value is None:
                yield [None]
            elif value is True:
                yield [u"1"]
            elif value is False:
                yield [u"0"]
        serialize_boolean.width = 1
        return serialize_boolean


class NumberToCSV(ToCSV):

    adapts(NumberDomain)

    def __call__(self):
        def serialize_number(value):
            if value is None:
                yield [None]
            else:
                yield [unicode(value)]
        serialize_number.width = 1
        return serialize_number


class FloatToCSV(ToCSV):

    adapts(FloatDomain)

    def __call__(self):
        def serialize_float(value):
            if value is None or str(value) in ['inf', '-inf', 'nan']:
                yield [None]
            else:
                yield [unicode(value)]
        serialize_float.width = 1
        return serialize_float


class DecimalToCSV(ToCSV):

    adapts(DecimalDomain)

    def __call__(self):
        def serialize_decimal(value):
            if value is None or value.is_finite():
                yield [None]
            else:
                yield [unicode(value)]
        serialize_decimal.width = 1
        return serialize_decimal


class StringToCSV(ToCSV):

    adapts_many(StringDomain,
                EnumDomain)

    def __call__(self):
        def serialize_string(value):
            yield [value]
        serialize_string.width = 1
        return serialize_string


class DateToCSV(ToCSV):

    adapts(DateDomain)

    def __call__(self):
        def serialize_date(value):
            if value is None:
                yield [None]
            else:
                yield [unicode(value)]
        serialize_date.width = 1
        return serialize_date


class TimeToCSV(ToCSV):

    adapts(TimeDomain)

    def __call__(self):
        def serialize_time(value):
            if value is None:
                yield [None]
            else:
                yield [unicode(value)]
        serialize_time.width = 1
        return serialize_time


class DateTimeToCSV(ToCSV):

    adapts(DateTimeDomain)

    def __call__(self):
        def serialize_datetime(value):
            if value is None:
                yield [None]
            elif not value.time():
                yield [unicode(value.date())]
            else:
                yield [unicode(value)]
        serialize_datetime.width = 1
        return serialize_datetime


def to_csv_headers(domain, headers):
    to_csv_headers = ToCSVHeaders(domain, headers)
    return to_csv_headers()


def to_csv(domain):
    to_csv = ToCSV(domain)
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
        return emit_headers()

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


