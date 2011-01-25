#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.error`
=====================

This module declares exceptions that can be raised by the HTSQL-to-SQL
translator.
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
    Represents a compiler error.

    This error is raised when the compiler is unable to generate a term node.
    """

    kind = "compile error"


class AssembleError(TranslateError):
    """
    Represents an assembler error.

    This error is raised when the assembler is unable to generate a frame
    or a phrase node.
    """

    kind = "assemble error"


class SerializeError(TranslateError):
    """
    Represents a serializer error.

    This error is raized when the serializer is unable to translate a clause
    node to SQL.
    """

    kind = "serialize error"


