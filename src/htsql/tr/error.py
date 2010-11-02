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


class TranslateError(BadRequestError):
    """
    Represents a translation error.
    """


class ScanError(TranslateError):
    """
    Represents a scanner error.

    This exception is raised when the scanner cannot tokenize a query.
    """

    kind = "scan error"


class ParseError(TranslateError):
    """
    Represents a parser error.

    This exception is raised by the parser when it encounters an unexpected
    token.
    """

    kind = "parse error"


class BindError(TranslateError):
    """
    Represents a binder error.

    This error is raised when the binder is unable to bind a syntax node.
    """

    kind = "bind error"


class EncodeError(TranslateError):
    """
    Represents an encoder error.

    This error is raised when the encoder is unable to encode or relate
    a binding node.
    """

    kind = "encode error"


class CompileError(TranslateError):
    """
    Represents an compiler error.

    This error is raised when the compiler is unable to generate a term node.
    """

    kind = "compile error"


