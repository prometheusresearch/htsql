#
# Copyright (c) 2006-2008, Prometheus Research, LLC
# Authors: Kirill Simonov <xi@gamma.dn.ua>,
#          Clark C. Evans <cce@clarkevans.com>
#


from .error import ScriptError
from .routine import Routine, Argument
from ..validator import WordVal


class HelpRoutine(Routine):

    name = 'help'
    aliases = ['h', '?']
    arguments = [
            Argument('routine', WordVal(), default=None),
    ]
    hint = u"""describe the usage of the application and its routines"""
    help = u"""
    Run '%(executable)s help' to describe the usage of the application
    and get the list of available routines.

    Run '%(executable)s help <routine>' to describe the usage of
    the specified routine.
    """

