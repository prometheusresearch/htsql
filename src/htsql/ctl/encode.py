#
# Copyright (c) 2006-2008, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#

"""
:mod:`htsql.ctl.encode`
=====================

This module implements the `encode` routine.
"""


from .routine import Routine, Argument
from ..core.validator import BoolVal
import os, urllib


class EncodeRoutine(Routine):
    """
    Implements the `encode` routine.
    """

    name = 'encode'
    aliases = []
    arguments = []
    hint = """percent encode input for database or query URI"""
    help = """
    Run '%(executable)s encode' to percent-encode a query fragment or
    database URI fragment so that it is a suitable for HTTP queries 
    and command line arguments.  This can be run interactively or 
    as a filter using standard input/output.
    """

    def run(self):
        input = self.ctl.input("What is the query fragment?")
        self.ctl.out(urllib.quote(input))
        if self.ctl.isatty:
            self.ctl.out()
