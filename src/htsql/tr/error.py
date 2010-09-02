#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.error`
=====================

This module implements HTSQL translation errors.
"""


from ..error import BadRequestError


class ScanError(BadRequestError):
    """
    Represents a scanner error.

    This exception is raised when the scanner cannot tokenize a query.
    """

    kind = "scan error"


class ParseError(BadRequestError):
    """
    Represents a parser error.

    This exception is raised by the parser when it encounters an unexpected
    token.
    """

    kind = "parse error"


