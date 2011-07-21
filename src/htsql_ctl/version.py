#
# Copyright (c) 2006-2008, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql_ctl.version`
========================

This module implements the `version` routine.
"""


from .routine import Routine


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

    def run(self):
        import htsql
        self.ctl.out(htsql.__version__)


