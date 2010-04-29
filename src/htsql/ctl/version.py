#
# Copyright (c) 2006-2008, Prometheus Research, LLC
# Authors: Kirill Simonov <xi@gamma.dn.ua>,
#          Clark C. Evans <cce@clarkevans.com>
#


"""Implementation of the 'version' routine."""


from .error import ScriptError
from .routine import Routine


class VersionRoutine(Routine):

    name = 'version'
    hint = u"""display the version of the application"""
    help = u"""
    Run '%(executable)s version' to display the version of HTSQL.
    """

    def run(self):
        import htsql
        self.ctl.out(htsql.__version__)


