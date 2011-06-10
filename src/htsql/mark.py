#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql.mark`
=================

This module implements a :class:`Mark` object.
"""


class Mark(object):
    """
    A slice of an HTSQL query.

    In the process of translation of an HTSQL query to SQL, intermediary nodes
    of different kinds are generated.  Most of the nodes have a `mark`
    attribute that points to the original HTSQL expression from which the
    node was derived.

    Note that a :class:`Mark` object should only be used for presentational
    purposes such as error reporting.  There is no guarantee that the slice
    ``mark.value[mark.start:mark.end]`` would represent a valid HTSQL
    expression.

    `input` (a string)
        The HTSQL query.

    `start` (an integer)
        The beginning of the slice.

    `end` (an integer)
        The end of the slice.
    """

    @classmethod
    def union(cls, *nodes):
        """
        Generates a new :class:`Mark` object from a collection of separate
        marks.  The returned mark will cover all the given marks.

        `nodes` (a list)
            A list of objects having a `mark` attribute.  The list may
            also include ``None``.
        """
        # Get a list of `Mark` objects; if no marks are given, return `None`.
        marks = [node.mark for node in nodes if node is not None]
        if not marks:
            return None
        # It might happen that there is more than one query associated with
        # the given marks.  In this case, there is no meaningful way to
        # construct a union from all the marks.  We deal with it by selecting
        # marks associated with one of the queries while ignoring all the
        # others.
        input = marks[0].input
        marks = [mark for mark in marks if mark.input is input]
        # Find the boundaries of the slice and instantiate the mark.
        start = min(mark.start for mark in marks)
        end = max(mark.end for mark in marks)
        return cls(input, start, end)

    def __init__(self, input, start, end):
        # Sanity check on the arguments.
        assert isinstance(input, str)
        assert isinstance(start, int)
        assert isinstance(end, int)
        assert 0 <= start <= end <= len(input)

        self.input = input
        self.start = start
        self.end = end

    def excerpt(self):
        """
        Returns a list of lines that consists an except of the original
        query with ``^`` characters underlining the mark.
        """
        # Find the line that contains the mark.
        excerpt_start = self.input.rfind('\n', 0, self.start)+1
        excerpt_end = self.input.find('\n', excerpt_start)
        if excerpt_end == -1:
            excerpt_end = len(self.input)

        # Assuming that the mark could be multiline, find the
        # beginning and the end of the mark in the selected excerpt.
        pointer_start = max(self.start, excerpt_start)
        pointer_end = min(self.end, excerpt_end)

        # For the pointer to be displayed properly, the lengths of
        # the indent and of the underline should be measured in Unicode
        # characters.
        try:
            pointer_indent = len(self.input[excerpt_start:pointer_start]
                                                        .decode('utf-8'))
            pointer_length = len(self.input[pointer_start:pointer_end]
                                                        .decode('utf-8'))
        except UnicodeDecodeError:
            # It might happen that the query is not UTF-8 encoded, in
            # which case we could only approximate.
            pointer_indent = pointer_start-excerpt_start
            pointer_length = pointer_end-pointer_start

        # Generate the exerpt and the pointer lines.
        lines = []
        lines.append(self.input[excerpt_start:excerpt_end])
        lines.append(' '*pointer_indent + '^'*max(pointer_length, 1))
        return lines

    def __str__(self):
        return "%r >>> %r <<< %r" % (self.input[:self.start],
                                     self.input[self.start:self.end],
                                     self.input[self.end:])

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self)


class EmptyMark(Mark):

    def __init__(self):
        super(EmptyMark, self).__init__("", 0, 0)


