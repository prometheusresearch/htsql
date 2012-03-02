#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql.ctl.default`
========================

This module implements the default routine.
"""


from .routine import Routine
from .option import HelpOption, VersionOption
from .help import HelpRoutine
from .version import VersionRoutine
import os.path


class DefaultRoutine(Routine):
    """
    Implements the default routine.
    
    The default routine is executed when no explicit routine name is given.
    """

    name = ''
    options = [
            HelpOption,
            VersionOption,
    ]

    def run(self):
        # Display the following information:
        # {EXECUTABLE} - {hint}
        # {copyright}
        #
        # {help}
        if self.help:
            routine = HelpRoutine(self.ctl, {'executable': self.executable,
                                             'routine': None,
                                             'feature': None})
            return routine.run()
        if self.version:
            routine = VersionRoutine(self.ctl, {})
            return routine.run()
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


