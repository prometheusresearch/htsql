#
# Copyright (c) 2006-2008, Prometheus Research, LLC
# Authors: Kirill Simonov <xi@gamma.dn.ua>,
#          Clark C. Evans <cce@clarkevans.com>
#


"""
:mod:`htsql.ctl.default`
========================

This module implements the default routine.
"""


from .routine import Routine
import os.path


class DefaultRoutine(Routine):
    """
    Implements the default routine.
    
    The default routine is executed when no explicit routine name is given.
    """

    name = ''

    def run(self):
        # Display the following information:
        # {EXECUTABLE} - {hint}
        # {copyright}
        #
        # {help}
        executable = os.path.basename(self.executable)
        hint = self.ctl.get_hint()
        if hint is not None:
            self.ctl.out(executable.upper(), '-', hint)
        else:
            self.ctl.out(executable.upper())
        copyright = self.ctl.get_copyright()
        if copyright is not None:
            self.ctl.out(copyright)
        help = self.ctl.get_help(executable=executable)
        if help is not None:
            self.ctl.out()
            self.ctl.out(help)


