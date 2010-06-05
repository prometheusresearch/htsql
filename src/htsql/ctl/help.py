#
# Copyright (c) 2006-2008, Prometheus Research, LLC
# Authors: Kirill Simonov <xi@gamma.dn.ua>,
#          Clark C. Evans <cce@clarkevans.com>
#


"""
:mod:`htsql.ctl.help`
=====================

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
            Argument('feature', WordVal(), default=None,
                     hint="""the feature to describe""")
    ]
    hint = """describe the usage of the application and its routines"""
    help = """
    Run '%(executable)s help' to describe the usage of the application and
    get the list of available routines.

    Run '%(executable)s help <routine>' to describe the usage of the
    specified routine.

    Some routines may contain separate descriptions of some features.
    Run '%(executable)s help <routine> <feature>' to describe a specific
    feature.
    """

    def run(self):
        # If called without any parameters, describe the application;
        # if called with a routine name, describe the routine; if called
        # with a routine name and a section name, display the section.
        if self.routine is None:
            self.describe_script()
        else:
            if self.routine not in self.ctl.routine_by_name:
                raise ScriptError("unknown routine %r" % self.routine)
            routine_class = self.ctl.routine_by_name[self.routine]
            if self.feature is None:
                self.describe_routine(routine_class)
            else:
                feature_class = routine_class.get_feature(self.feature)
                self.describe_feature(feature_class)

    def describe_script(self):
        # Display the following information:
        # {EXECUTABLE} - {hint}
        # {copyright}
        # Usage: {executable} <routine> [options] [arguments]
        #
        # {help}
        #
        # Available routines:
        #   {routine.name(s)} : {routine.hint}
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
        # {NAME} - {hint}
        # Usage: {executable} {signature}
        #
        # {help}
        #
        # Arguments:
        #   {argument.signature} : {argument.hint}
        #   ...
        #
        # Valid options:
        #   {option.signature} : {option.hint}
        #   ...
        #
        executable = os.path.basename(self.executable)
        hint = routine_class.get_hint()
        if hint is not None:
            self.ctl.out(routine_class.name.upper(), "-", hint)
        else:
            self.ctl.out(routine_class.name.upper())
        signature = routine_class.get_signature()
        self.ctl.out("Usage:", executable, signature)
        help = routine_class.get_help(executable=executable)
        if help is not None:
            self.ctl.out()
            self.ctl.out(help)
        if routine_class.arguments:
            self.ctl.out()
            self.ctl.out("Arguments:")
            for argument in routine_class.arguments:
                signature = argument.get_signature()
                self.ctl.out("  ", end="")
                hint = argument.get_hint()
                if hint is not None:
                    self.ctl.out("%-24s : %s" % (signature, hint))
                else:
                    self.ctl.out(signature)
        if routine_class.options:
            self.ctl.out()
            self.ctl.out("Valid options:")
            for option in routine_class.options:
                signature = option.get_signature()
                self.ctl.out("  ", end="")
                hint = option.get_hint()
                if hint is not None:
                    self.ctl.out("%-24s : %s" % (signature, hint))
                else:
                    self.ctl.out(signature)
        self.ctl.out()

    def describe_feature(self, feature_class):
        # Display the following information:
        # {NAME} - {hint}
        #
        # {help}
        name = feature_class.name.upper()
        hint = feature_class.get_hint()
        help = feature_class.get_help()
        if hint is None:
            self.ctl.out(name)
        else:
            self.ctl.out("%s - %s" % (name, hint))
        if help is not None:
            self.ctl.out()
            self.ctl.out(help)
        self.ctl.out()


