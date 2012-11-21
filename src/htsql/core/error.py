#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from .util import Printable, maybe


#
# Generic HTTP Errors.
#


class HTTPError(Exception):
    """
    An exception as an HTTP response.

    An instance of :class:`HTTPError` serves as a simple WSGI application
    generating an appropriate HTTP error code and displaying the error
    message.
    """

    #: HTTP status line.
    status = None

    def __call__(self, environ, start_response):
        # Implement the WSGI entry point.
        start_response(self.status,
                       [('Content-Type', 'text/plain; charset=UTF-8')])
        return [str(self), "\n"]


class BadRequestError(HTTPError):
    """
    Represents ``400 Bad Request``.
    """

    status = "400 Bad Request"


class ForbiddenError(HTTPError):
    """
    Represents ``403 Forbidden``.
    """

    status = "403 Forbidden"


class NotFoundError(HTTPError):
    """
    Represents ``404 Not Found``.
    """

    status = "404 Not Found"


class ConflictError(HTTPError):
    """
    Represents ``409 Conflict``.
    """

    status = "409 Conflict"


class InternalServerError(HTTPError):
    """
    Represents ``500 Internal Server Error``.
    """

    status = "500 Internal Server Error"


class NotImplementedError(HTTPError):
    """
    Represents ``501 Not Implemented``.
    """

    status = "501 Not Implemented"


#
# Concrete HTSQL errors.
#


class Mark(Printable):
    """
    A fragment of an HTSQL query.

    `text`: ``unicode``
        The input query string.

    `start`: ``int``
        The starting position of the fragment.

    `end`: ``int``
        The ending position of the fragment.

    A :class:`Mark` object represents a fragment of an HTSQL query to be
    used as an error context for error reporting.
    """

    @classmethod
    def union(cls, *nodes):
        """
        Generates a new :class:`Mark` object as a cover for a fragment
        collection.

        `nodes`
            A list of :class:`Mark` instances or objects with a `mark`
            attribute; ``None`` entries are ignored.
        """
        # Get a list of `Mark` objects; if no marks are given, return an
        # empty mark.
        marks = [node if isinstance(node, Mark) else node.mark
                 for node in nodes if node is not None]
        if not marks:
            return EmptyMark()
        # It might happen that different marks refer to different query strings.
        # In this case, we choose one of them and ignore marks associated with
        # other query strings.
        text = marks[0].text
        marks = [mark for mark in marks if mark.text is text]
        # Find the boundaries of the slice and instantiate the mark.
        start = min(mark.start for mark in marks)
        end = max(mark.end for mark in marks)
        return cls(text, start, end)

    def __init__(self, text, start, end):
        # Sanity check on the arguments.
        assert isinstance(text, unicode)
        assert isinstance(start, int)
        assert isinstance(end, int)
        assert 0 <= start <= end <= len(text)

        self.text = text
        self.start = start
        self.end = end

    def excerpt(self):
        """
        Returns a list of lines that forms an excerpt of the original query
        string with ``^`` characters underlining the marked fragment.
        """
        # Find the line that contains the mark.
        excerpt_start = self.text.rfind(u'\n', 0, self.start)+1
        excerpt_end = self.text.find(u'\n', excerpt_start)
        if excerpt_end == -1:
            excerpt_end = len(self.text)

        # Assuming that the mark could be multiline, find the
        # beginning and the end of the mark in the selected excerpt.
        pointer_start = max(self.start, excerpt_start)
        pointer_end = min(self.end, excerpt_end)

        # The lenths of the indent and the underline.
        pointer_indent = pointer_start - excerpt_start
        pointer_length = pointer_end - pointer_start

        # Generate the exerpt and the pointer lines.
        lines = []
        lines.append(self.text[excerpt_start:excerpt_end])
        lines.append(u' '*pointer_indent + u'^'*max(pointer_length, 1))
        return lines

    def __unicode__(self):
        return u">>> %s <<<" % self.text[self.start:self.end]


class EmptyMark(Mark):
    """
    An empty error context.
    """

    def __init__(self):
        super(EmptyMark, self).__init__(u"", 0, 0)


class StackedError(Exception):
    """
    An exception with a query fragment as the error context.

    `detail`: ``str``
        The error message.

    `mark`: :class:`Mark`
        A pointer to a query fragment.

    `hint`: ``str``
        Explanation of the error and possible ways to fix it.
    """

    def __init__(self, detail, mark, hint=None):
        assert isinstance(mark, Mark)
        assert isinstance(hint, maybe(str))
        super(StackedError, self).__init__(detail)
        self.detail = detail
        self.mark = mark
        self.hint = hint

    def __str__(self):
        if not self.mark.text:
            return self.detail
        lines = self.mark.excerpt()
        mark_detail = "\n".join("    "+line.encode('utf-8') for line in lines)
        return "%s:\n%s%s" % (self.detail, mark_detail,
                              "\n(%s)" % self.hint
                                            if self.hint is not None else "")


class Error(StackedError, BadRequestError):
    """
    An error caused by user mistake.
    """


class EngineError(StackedError, ConflictError):
    """
    An error generated by the database server.
    """

    def __init__(self, detail):
        super(EngineError, self).__init__(detail, EmptyMark())


class PermissionError(StackedError, ForbiddenError):
    """
    An error caused by lack of read or write permissions.
    """

    def __init__(self, detail):
        super(PermissionError, self).__init__(detail, EmptyMark())


