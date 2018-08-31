#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ..util import setof
from ..adapter import Adapter, Utility, adapt
from ..domain import Domain, ListDomain, RecordDomain, Profile


class Format:
    pass


class DefaultFormat(Format):
    pass


class RawFormat(Format):

    def __init__(self, with_null=False):
        self.with_null = with_null


class JSONFormat(Format):

    def __init__(self, with_null=False):
        self.with_null = with_null


class CSVFormat(Format):

    def __init__(self, dialect='excel'):
        assert dialect in ['excel', 'excel-tab']
        self.dialect = dialect


class TSVFormat(CSVFormat):

    def __init__(self, dialect='excel-tab'):
        super(TSVFormat, self).__init__(dialect)


class HTMLFormat(Format):
    pass


class TextFormat(Format):
    pass


class XMLFormat(Format):
    pass


class ProxyFormat(Format):

    def __init__(self, format):
        assert isinstance(format, Format)
        self.format = format


