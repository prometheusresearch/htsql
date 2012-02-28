#
# Copyright (c) 2006-2008, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#

"""
:mod:`htsql.ctl.version`
========================

This module implements the `version` routine.
"""

from .routine import Routine
from ..core.util import trim_doc

class VersionRoutine(Routine):
    """
    Implements the `version` routine.

    The routine displays the version of the :mod:`htsql` package.
    """

    name = 'version'
    hint = """display the version of the application"""
    help = """
    Run '%(executable)s version' to display the version of HTSQL.
    """
    message = """
        HTSQL %s
        %s

        There is NO WARRANTY, to the extent permitted by law.
    """

    def run(self):
        import htsql
        self.ctl.out(trim_doc(self.message) % (htsql.__version__,
                                               self.ctl.copyright))


