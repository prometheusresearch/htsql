#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.ctl`
================

This package implements the ``htsql-ctl`` script.
"""


from .script import Script
from .default import DefaultRoutine
from .help import HelpRoutine
from .version import VersionRoutine
from .server import ServerRoutine
from .shell import ShellRoutine
from .request import GetRoutine, PostRoutine
from .regress import RegressRoutine
import sys


class HTSQL_CTL(Script):
    """
    Implements the ``htsql-ctl`` script.

    Usage::

        ctl = HTSQL_CTL(stdin, stdout, stderr)
        exit_code = ctl.main(argv)
    """

    routines = [
            DefaultRoutine,
            HelpRoutine,
            VersionRoutine,
            ServerRoutine,
            ShellRoutine,
            GetRoutine,
            PostRoutine,
            RegressRoutine,
    ]
    hint = """HTSQL command-line administrative application"""
    help = """
    Run `%(executable)s help` for general usage and list of routines.
    Run `%(executable)s help <routine>` for help on a specific routine.
    """
    copyright = """Copyright (c) 2006-2011, Prometheus Research, LLC"""


def main():
    # This function is called when the `htsql-ctl` script is started.
    # The return value is passed to `sys.exit()`.
    ctl = HTSQL_CTL(sys.stdin, sys.stdout, sys.stderr)
    return ctl.main(sys.argv)


