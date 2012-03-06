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
from .default import DefaultRoutine
from .help import HelpRoutine
from .version import VersionRoutine
from .extension import ExtensionRoutine
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
            ExtensionRoutine,
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

    def get_version(self):
        import htsql
        return "%s %s" % (htsql.__name__.upper(), htsql.__version__)

    def get_copyright(self):
        import htsql
        return trim_doc(htsql.__copyright__)

    def get_license(self):
        import htsql
        return trim_doc(htsql.__license__)

    def get_appropriate_legal_notices(self):
        notices = []
        version = self.get_version()
        if version is not None:
            notices.append(version)
        copyright = self.get_copyright()
        if copyright is not None:
            notices.append(copyright)
        license = self.get_license()
        if license is not None:
            notices.append(license)
        return "\n".join(notices)

def main():
    # This function is called when the `htsql-ctl` script is started.
    # The return value is passed to `sys.exit()`.
    ctl = HTSQL_CTL(sys.stdin, sys.stdout, sys.stderr)
    return ctl.main(sys.argv)


