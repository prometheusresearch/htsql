#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from .util import Clonable, Printable, maybe, listof, oneof, urlquote
import weakref


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
        # Implement a WSGI entry point.
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


class Paragraph(Printable):

    def __init__(self, message):
        assert isinstance(message, (str, unicode))
        if isinstance(message, str):
            message = message.decode('utf-8', 'replace')
        self.message = message

    def __unicode__(self):
        return self.message


class PointerPara(Paragraph):

    def __init__(self, message, mark):
        super(PointerPara, self).__init__(message)
        assert isinstance(mark, Mark)
        self.mark = mark

    def __unicode__(self):
        if not self.mark:
            return self.message
        lines = self.mark.excerpt()
        pointer = u"\n".join(u"    "+line for line in lines)
        return u"%s:\n%s" % (self.message, pointer)


class QuotePara(Paragraph):

    def __init__(self, message, quote):
        assert isinstance(quote, (str, unicode))
        if isinstance(quote, str):
            quote = quote.decode('utf-8', 'replace')
        super(QuotePara, self).__init__(message)
        self.quote = quote.rstrip()

    def __unicode__(self):
        if not self.quote:
            return self.message
        block = "\n".join(u"    "+line for line in self.quote.splitlines())
        return u"%s:\n%s" % (self.message, block)


class ChoicePara(Paragraph):

    def __init__(self, message, choices):
        super(PointerPara, self).__init__(message)
        assert isinstance(choices, listof(oneof(str, unicode)))
        choices = [choice.decode('utf-8', 'replace')
                   if isinstance(choice, str) else choice]
        self.choices = choices

    def __unicode__(self):
        if not self.choices:
            return self.message
        return u"%s:\n%s" % (self.message,
                             u"\n".join(u"    "+choice for choice in choices))


class Mark(Clonable, Printable):
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
        marks = [node if isinstance(node, Mark) else MarkRef.get_mark(node)
                 for node in nodes if node is not None]
        if not marks:
            return None
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
        if not self.text:
            return u""
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
        return u"\n".join(self.excerpt())

    def __repr__(self):
        chunk = self.text[self.start:self.end]
        return "<%s %s>" % (self.__class__.__name__,
                            urlquote(chunk, '').encode('utf-8'))

    def __nonzero__(self):
        return bool(self.text)


class MarkRef(weakref.ref):

    __slots__ = ('oid', 'mark')

    oid_to_ref = {}

    @staticmethod
    def cleanup(ref, oid_to_ref=oid_to_ref):
        del oid_to_ref[ref.oid]

    def __new__(cls, node, mark):
        self = super(MarkRef, cls).__new__(cls, node, cls.cleanup)
        self.oid = id(node)
        self.mark = mark
        cls.oid_to_ref[self.oid] = self
        return self

    def __init__(self, node, mark):
        super(MarkRef, self).__init__(node, self.cleanup)

    @classmethod
    def get_mark(cls, node):
        ref = cls.oid_to_ref.get(id(node))
        if ref is not None:
            return ref.mark

    @classmethod
    def set_mark(cls, node, mark):
        cls(node, mark)

    @classmethod
    def point(cls, node, mark):
        if node is None or mark is None:
            return
        if cls.get_mark(node) is not None:
            return node
        if not isinstance(mark, Mark):
            mark = cls.get_mark(mark)
        if mark is None:
            return node
        cls.set_mark(node, mark)
        return node


point = MarkRef.point


class Error(BadRequestError):

    def __init__(self, *paragraphs):
        self.paragraphs = [paragraph if isinstance(paragraph, Paragraph)
                           else Paragraph(paragraph)
                           for paragraph in paragraphs]

    def wrap(self, *paragraphs):
        self.paragraphs.extend(paragraph if isinstance(paragraph, Paragraph)
                               else Paragraph(paragraph)
                               for paragraph in paragraphs)

    def __unicode__(self):
        return u"\n".join(unicode(paragraph) for paragraph in self.paragraphs)

    def __str__(self):
        return unicode(self).encode('utf-8')

    def __repr__(self):
        if not self.paragraphs:
            return "<%s>" % self.__class__.__name__
        return "<%s: %s>" % (self.__class__.__name__,
                             self.paragraphs[0].message.encode('utf-8'))


class EngineError(ConflictError, Error):
    """
    An error generated by the database driver.
    """


class PermissionError(ForbiddenError, Error):
    """
    An error caused by lack of read or write permissions.
    """


class ErrorGuard(object):

    def __init__(self, *paragraphs):
        self.paragraphs = paragraphs

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if isinstance(exc_value, Error):
            exc_value.wrap(*self.paragraphs)


class PointerErrorGuard(object):

    def __init__(self, message, mark):
        self.message = message
        self.node = mark
        if mark is not None:
            if not isinstance(mark, Mark):
                mark = MarkRef.get_mark(mark)
        self.mark = mark

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if not isinstance(exc_value, Error):
            return
        if not self.mark:
            return
        if any(paragraph.mark.text == self.mark.text
               for paragraph in exc_value.paragraphs
               if isinstance(paragraph, PointerPara)):
            return
        exc_value.wrap(PointerPara(self.message, self.mark))


def parse_guard(mark):
    return PointerErrorGuard("While parsing", mark)


recognize_guard = parse_guard


def translate_guard(mark):
    return PointerErrorGuard("While translating", mark)


def act_guard(mark):
    return PointerErrorGuard("While processing", mark)


def choices_guard(choices):
    if not choices:
        return ErrorGuard()
    quote = "\n".join(choices)
    return ErrorGuard(QuotePara("Perhaps you had in mind", quote))


