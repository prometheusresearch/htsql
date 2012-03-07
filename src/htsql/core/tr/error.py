#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql.core.tr.error`
==========================

This module declares exceptions that can be raised by the HTSQL-to-SQL
translator.
"""


from ..util import maybe
from ..error import BadRequestError
from ..mark import Mark


class TranslateError(BadRequestError):
    """
    Represents a translation error.
    """

    def __init__(self, detail, mark, hint=None):
        assert isinstance(mark, Mark)
        assert isinstance(hint, maybe(str))
        super(TranslateError, self).__init__(detail)
        self.mark = mark
        self.hint = hint

    def __str__(self):
        lines = self.mark.excerpt()
        mark_detail = "\n".join("    "+line.encode('utf-8') for line in lines)
        return "%s: %s:\n%s%s" % (self.kind, self.detail, mark_detail,
                                  "\n(%s)" % self.hint
                                    if self.hint is not None else "")


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

    Raised when the binder is unable to bind a syntax node.
    """

    kind = "bind error"


class EncodeError(TranslateError):
    """
    Represents an encoder error.

    Raised when the encoder is unable to encode or relate a binding node.
    """

    kind = "encode error"


class CompileError(TranslateError):
    """
    Represents a compiler error.

    Raised when the compiler is unable to generate a term node.
    """

    kind = "compile error"


class AssembleError(TranslateError):
    """
    Represents an assembler error.

    Raised when the assembler is unable to generate a frame or a phrase node.
    """

    kind = "assemble error"


class SerializeError(TranslateError):
    """
    Represents a serializer error.

    Raised when the serializer is unable to translate a clause node to SQL.
    """

    kind = "serialize error"


