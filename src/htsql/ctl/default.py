#
# Copyright (c) 2006-2008, Prometheus Research, LLC
# Authors: Kirill Simonov <xi@gamma.dn.ua>,
#          Clark C. Evans <cce@clarkevans.com>
#


from .routine import Routine
import os.path


class DefaultRoutine(Routine):

    name = ''

    def run(self):
        executable = os.path.basename(self.executable)
        self.ctl.out(executable.upper(), '-', self.ctl.get_hint())
        self.ctl.out(self.ctl.get_help(executable=executable))


