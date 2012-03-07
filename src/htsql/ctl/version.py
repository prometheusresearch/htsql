#
# Copyright (c) 2006-2012, Prometheus Research, LLC
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

    def run(self):
        self.ctl.out(self.ctl.get_legal())
        self.ctl.out()
