#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql.ctl`
================

This package implements the ``htsql-ctl`` script.
"""

from ..core.util import trim_doc
from .script import Script
import sys


class HTSQL_CTL(Script):
    """
    Implements the ``htsql-ctl`` script.

    Usage::

        ctl = HTSQL_CTL(stdin, stdout, stderr)
        exit_code = ctl.main(argv)
    """

    routines_entry = 'htsql.routines'
    hint = """HTSQL command-line administrative application"""
    help = """
    Run `%(executable)s help` for general usage and list of routines.
    Run `%(executable)s help <routine>` for help on a specific routine.
    """

    def get_copyright(self):
        import htsql
        return trim_doc(htsql.__copyright__)

    def get_legal(self):
        import htsql
        return trim_doc(htsql.__legal__)


def main():
    # This function is called when the `htsql-ctl` script is started.
    # The return value is passed to `sys.exit()`.
    ctl = HTSQL_CTL(sys.stdin, sys.stdout, sys.stderr)
    return ctl.main(sys.argv)


