#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql.core.fmt`
=====================

This package implements product formatters.
"""


from . import entitle, format, html, json, spreadsheet, text
from .format import FindRenderer
from .json import JSONRenderer
from .spreadsheet import CSVRenderer, TSVRenderer
from .html import HTMLRenderer
from .text import TextRenderer


class FindStandardRenderer(FindRenderer):

    def get_renderers(self):
        return ([CSVRenderer, TSVRenderer, JSONRenderer,
                 HTMLRenderer, TextRenderer]
                + super(FindStandardRenderer, self).get_renderers())


