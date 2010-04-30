#
# Copyright (c) 2006-2008, Prometheus Research, LLC
# Authors: Kirill Simonov <xi@gamma.dn.ua>,
#          Clark C. Evans <cce@clarkevans.com>
#


"""
This module implements the `help` routine.
"""


from .error import ScriptError
from .routine import Routine, Argument
from ..validator import WordVal
import os


class HelpRoutine(Routine):
    """
    Implements the `help` routine.

    When called without any parameters, it describes the application.
    When called with a routine name, it describes the routine.
    """

    name = 'help'
    aliases = ['h', '?']
    arguments = [
            Argument('routine', WordVal(), default=None,
                     hint="""the name of the routine to describe"""),
    ]
    hint = """describe the usage of the application and its routines"""
    help = """
    Run '%(executable)s help' to describe the usage of the application
    and get the list of available routines.

    Run '%(executable)s help <routine>' to describe the usage of
    the specified routine.
    """

    def run(self):
        # If called without any parameters, describe the application;
        # if called with a routine name, describe the routine.
        if self.routine is None:
            self.describe_script()
        else:
            if self.routine not in self.ctl.routine_by_name:
                raise ScriptError("unknown routine %r" % self.routine)
            routine_class = self.ctl.routine_by_name[self.routine]
            self.describe_routine(routine_class)

    def describe_script(self):
        # Display the following information:
        # {EXECUTABLE} - {hint}
        # {copyright}
        # Usage: {executable} <routine> [options] [arguments]
        #
        # {help}
        #
        # Available routines:
        #   {routine} : {hint}
        #   ...
        #
        executable = os.path.basename(self.executable)
        hint = self.ctl.get_hint()
        if hint is not None:
            self.ctl.out(executable.upper(), '-', hint)
        else:
            self.ctl.out(executable.upper())
        copyright = self.ctl.get_copyright()
        if copyright is not None:
            self.ctl.out(copyright)
        self.ctl.out("Usage:", executable, "<routine> [options] [arguments]")
        help = self.ctl.get_help(executable=executable)
        if help is not None:
            self.ctl.out()
            self.ctl.out(help)
        self.ctl.out()
        self.ctl.out("Available routines:")
        for routine_class in self.ctl.routines:
            if not routine_class.name:
                continue
            name = routine_class.name
            if routine_class.aliases:
                name = "%s (%s)" % (name, ", ".join(routine_class.aliases))
            self.ctl.out("  ", end="")
            hint = routine_class.get_hint()
            if hint is not None:
                self.ctl.out("%-24s : %s" % (name, hint))
            else:
                self.ctl.out(name)
        self.ctl.out()

    def describe_routine(self, routine_class):
        # Display the following information:
        # {ROUTINE} - {hint}
        # Usage: {executable} {routine} {arguments}
        #
        # {help}
        #
        # Arguments:
        #   {argument} : {hint}
        #   ...
        #
        # Valid options:
        #   {option} : {hint}
        #   ...
        #
        executable = os.path.basename(self.executable)
        hint = routine_class.get_hint()
        if hint is not None:
            self.ctl.out(routine_class.name.upper(), "-", hint)
        else:
            self.ctl.out(routine_class.name.upper())
        usage = [executable, routine_class.name]
        bracket_depth = 0
        for idx, argument in enumerate(routine_class.arguments):
            name = argument.attribute.replace('_', '-').upper()
            if argument.is_list:
                name = "%s..." % name
            if not argument.is_required:
                name = "[%s" % name
                bracket_depth += 1
            if idx == len(routine_class.arguments)-1 and bracket_depth != 0:
                name = "%s%s" % (name, "]"*bracket_depth)
            usage.append(name)
        self.ctl.out("Usage:", *usage)
        help = routine_class.get_help(executable=executable)
        if help is not None:
            self.ctl.out()
            self.ctl.out(help)
        if routine_class.arguments:
            self.ctl.out()
            self.ctl.out("Arguments:")
            for argument in routine_class.arguments:
                name = argument.attribute.replace('_', '-').upper()
                self.ctl.out("  ", end="")
                hint = argument.get_hint()
                if hint is not None:
                    self.ctl.out("%-24s : %s" % (name, hint))
                else:
                    self.ctl.out(name)
        if routine_class.options:
            self.ctl.out()
            self.ctl.out("Valid options:")
            for option in routine_class.options:
                if option.short_name is not None:
                    name = option.short_name
                    if option.long_name is not None:
                        name = "%s [%s]" % (name, option.long_name)
                else:
                    name = option.long_name
                if option.with_value:
                    if option.value_name is not None:
                        parameter = option.value_name
                    else:
                        parameter = option.attribute
                    parameter = parameter.replace('_', '-').upper()
                    name = "%s %s" % (name, parameter)
                self.ctl.out("  ", end="")
                hint = option.get_hint()
                if hint is not None:
                    self.ctl.out("%-24s : %s" % (name, hint))
                else:
                    self.ctl.out(name)
        self.ctl.out()


