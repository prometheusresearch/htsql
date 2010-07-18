#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.fmt`
================

This package implements product formatters.
"""


from ..adapter import find_adapters
from .format import FindRenderer
from .json import JSONRenderer
from .spreadsheet import CSVRenderer
from .html import HTMLRenderer
from .text import TextRenderer


class FindStandardRenderer(FindRenderer):

    def get_renderers(self):
        return ([CSVRenderer, JSONRenderer, HTMLRenderer, TextRenderer]
                + super(FindStandardRenderer, self).get_renderers())


fmt_adapters = find_adapters()

