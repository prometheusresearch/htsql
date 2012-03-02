#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql.ctl.error`
======================

This module implements exceptions raised by command-line scripts.
"""


class ScriptError(Exception):
    """
    A fatal application error.

    `detail`
        The error message.
    """

    kind = """Fatal error"""

    def __init__(self, detail):
        assert isinstance(detail, str)
        self.detail = detail

    def __str__(self):
        return "%s: %s\n" % (self.kind, self.detail)

    def __repr__(self):
        return "<%s.%s %s>" % (self.__class__.__module__,
                               self.__class__.__name__, self)


