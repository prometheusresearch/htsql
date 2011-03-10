#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
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

    def pointer(self):
        """
        Returns a string consisting of space and ``^`` characters that
        could be used for underlining the mark in the original query.

        For instance, assume the query is::

            /{2+2}

        and the mark points to the expression ``2+2``.  In this case,
        the method will produce the string::

            '  ^^^'

        If we print the query and the pointer on two subsequent lines,
        we get::

            /{2+2}
              ^^^
        """
        # For the pointer to be displayed properly, the lengths of
        # the indent and of the underline should be measured in Unicode
        # characters.
        try:
            indent_length = len(self.input[:self.start].decode('utf-8'))
            line_length = len(self.input[self.start:self.end].decode('utf-8'))
        except UnicodeDecodeError:
            # It might happen that the query is not UTF-8 encoded, in
            # which case we could only approximate.
            indent_length = self.start
            line_length = self.end-self.start
        pointer = ' '*indent_length + '^'*max(line_length, 1)
        return pointer

    def __str__(self):
        return "%r >>> %r <<< %r" % (self.input[:self.start],
                                     self.input[self.start:self.end],
                                     self.input[self.end:])

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self)


class EmptyMark(Mark):

    def __init__(self):
        super(EmptyMark, self).__init__("", 0, 0)


