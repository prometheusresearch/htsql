#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.error`
==================

This module implements generic HTSQL exceptions.
"""


from .mark import Mark


class HTTPError(Exception):
    """
    An error associated with an HTSQL query.

    An instance of :class:`HTTPError` contains a pointer to an HTSQL
    expression that caused the error.  The traceback produced by the exception
    includes the original HTSQL query and a pointer to the erroneous
    expression.

    An instance of :class:`HTTPError` could also serve as a simple WSGI
    application generating an appropriate HTTP error code and displaying
    the error message.

    This is an abstract exception class.  To implement a concrete exception,
    add a subclass of :class:`HTTPError` and override the following class
    attributes:

    `code`
        The HTTP status line.

    `kind`
        The description of the error class.

    The constructor of :class:`HTTPError` accepts the following parameters:

    `detail` (a string)
        The description of the error.

    `mark` (class:`htsql.mark.Mark`)
        The slice of an HTSQL query that caused the error.
    """

    code = None
    kind = None

    def __init__(self, detail, mark):
        assert isinstance(detail, str)
        assert isinstance(mark, Mark)

        self.detail = detail
        self.mark = mark

    def __call__(self, environ, start_response):
        """
        Implements the WSGI entry point.
        """
        start_response(self.code,
                       [('Content-Type', 'text/plain; charset=UTF-8')])
        return [str(self), "\n"]

    def __str__(self):
        mark_detail = "    %s\n    %s" % (self.mark.input, self.mark.pointer())
        return "%s: %s:\n%s" % (self.kind, self.detail, mark_detail)

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self.detail)


#
# Generic HTTP Errors.
#


class BadRequestError(HTTPError):
    """
    Represents ``400 Bad Request``.
    """

    code = "400 Bad Request"
    kind = "invalid request"


class ForbiddenError(HTTPError):
    """
    Represents ``403 Forbidden``.
    """

    code = "403 Forbidden"
    kind = "the request is denied for security reasons"


class NotFoundError(HTTPError):
    """
    Represents ``404 Not Found``.
    """

    code = "404 Not Found"
    kind = "resource not found"


class ConflictError(HTTPError):
    """
    Represents ``409 Conflict``.
    """

    code = "409 Conflict"
    kind = "conflict"


class InternalServerError(HTTPError):
    """
    Represents ``500 Internal Server Error``.
    """

    code = "500 Internal Server Error"
    kind = "implementation error"


class NotImplementedError(HTTPError):
    """
    Represents ``501 Not Implemented``.
    """

    code = "501 Not Implemented"
    kind = "not implemented"


#
# Concrete HTSQL errors.
#


class InvalidSyntaxError(BadRequestError):
    """
    Represents an invalid syntax error.

    This exception is raised by the scanner when it cannot tokenize the query,
    or by the parser when it finds an unexpected token.
    """

    kind = "invalid syntax"


class InvalidArgumentError(BadRequestError):

    kind = "invalid argument"


class EngineError(ConflictError):

    kind = "engine failure"


