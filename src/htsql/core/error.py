#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql.core.error`
=======================

This module implements generic HTSQL exceptions.
"""


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
    """

    code = None
    kind = None

    def __init__(self, detail):
        assert isinstance(detail, str)
        self.detail = detail

    def __call__(self, environ, start_response):
        """
        Implements the WSGI entry point.
        """
        start_response(self.code,
                       [('Content-Type', 'text/plain; charset=UTF-8')])
        return [str(self), "\n"]

    def __str__(self):
        return "%s: %s" % (self.kind, self.detail)

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


class EngineError(ConflictError):

    kind = "engine failure"


class PermissionError(ForbiddenError):

    kind = "not enough permissions"


