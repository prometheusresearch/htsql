#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql.core.split_sql`
===========================

This module declares the SQL splitter adapter.
"""


from .adapter import Utility
from .util import maybe
import re


class SQLToken(object):
    """
    Declares a regular expression pattern to be used by the SQL splitter.

    `pattern` (a string)
        A regular expression in the verbose format.  The expression will
        be compiled using ``re.X|re.S`` flags.

    `min_level` (an integer or ``None``)
        The minimal level at which the pattern activates.

    `max_level` (an integer or ``None``)
        The maximum level at which the pattern activates.

    `only_level` (an integer or ``None``)
        The level at which the pattern activates.

    `delta` (an integer)
        When a token is detected, change the current level by `delta`.

    `is_junk` (Boolean)
        Ignore the token value.

    `is_end` (Boolean)
        If set, indicates that the splitter should stop when a token
        is detected.
    """

    def __init__(self, pattern,
                 min_level=None, max_level=None, only_level=None,
                 delta=0, is_junk=False, is_end=False):
        # Sanity check on the arguments.
        assert isinstance(pattern, str)
        assert isinstance(min_level, maybe(int))
        assert isinstance(max_level, maybe(int))
        assert isinstance(only_level, maybe(int))
        assert only_level is None or (min_level is None and max_level is None)
        assert isinstance(delta, int)
        assert isinstance(is_junk, bool)
        assert isinstance(is_end, bool)

        self.pattern = pattern
        self.regexp = re.compile(pattern, re.X|re.S)
        self.min_level = min_level
        self.max_level = max_level
        self.only_level = only_level
        self.delta = delta
        self.is_junk = is_junk
        self.is_end = is_end


class SplitSQL(Utility):
    """
    Declares the SQL splitter interface.

    A SQL splitter takes a string containing one or more SQL statements
    separated by ``;`` and produces a sequence of SQL statements.

    Usage::

        try:
            for sql in SplitSQL.__invoke__(input):
                cursor.execute(sql)
        except ValueError:
            ...

    This is an abstract utility.  To add a new splitter, create a subclass
    of :class:`SplitSQL` and override the class variable `tokens`:

    Class attributes:

    `tokens` (a list of :class:`SQLToken` instances)
        The tokens recognized by the splitter.

    Attributes:

    `input` (a string)
        A string containing SQL statements separated by ``;``.
    """

    tokens = None

    def __init__(self, input):
        assert isinstance(input, str)
        self.input = input

    def __call__(self):
        # The current position in `input`.
        start = 0
        # The current level.
        level = 0
        # The accumulated token values.
        values = []
        # Are we done?
        is_end = False
        # Till we are done.
        while not is_end:
            # Loop over the token to find one matching the input.
            for token in self.tokens:
                # Ignore tokens that are not available at the current level.
                if token.min_level is not None and level < token.min_level:
                    continue
                if token.max_level is not None and level > token.max_level:
                    continue
                if token.only_level is not None and level != token.only_level:
                    continue
                # Does the input matches the token pattern?
                match = token.regexp.match(self.input, start)
                if match is None:
                    continue
                # The value of the token.
                value = match.group()
                # Accumulate the value.
                if not token.is_junk and value:
                    values.append(value)
                # Update the current level.
                level += token.delta
                assert level >= 0
                # When we reach the level `0`, the accumulated tokens
                # are combined to a new statement.
                if level == 0 and values:
                    sql = ''.join(values)
                    yield sql
                    values = []
                # Advance the pointer and start over.
                start = match.end()
                is_end = token.is_end
                break

            # None of the tokens matched.
            else:
                # Determine the current position and complain.
                line = self.input[:start].count('\n')
                if line:
                    column = start-self.input[:start].rindex('\n')-1
                else:
                    column = start
                raise ValueError("unable to parse an SQL statement"
                                 " at line %s, column %s" % (line+1, column+1))

        # Some sanity checks.
        assert start == len(self.input)
        assert not values


split_sql = SplitSQL.__invoke__


