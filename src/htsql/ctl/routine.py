#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
This module defines basic classes for implementing script routines.
"""


from .error import ScriptError


# Indicates that the argument is required.
ARGUMENT_REQUIRED = object()

class Argument(object):
    """
    Describes an argument of a script routine.

    `attribute` (a string)
        The name of the routine attribute.  When the routine is
        initialized, the value of the argument is assigned to
        the attribute.

    `validator` (:class:`htsql.validator.Val`)
        The validator for the argument value.

    `default`
        The default value of the argument.  If `default` is not
        provided, the argument value is always required.

    `is_list` (Boolean)
        If set, the argument may accept more than one parameter.
        In this case, the argument value is a list of parameters.
    """

    def __init__(self, attribute, validator,
                 default=ARGUMENT_REQUIRED, is_list=False):
        self.attribute = attribute
        self.validator = validator
        self.default = default
        self.is_required = (default is ARGUMENT_REQUIRED)
        self.is_list = is_list


class Routine(object):
    """
    Describes a script routine.

    :class:`Routine` is a base abstract class for implementing
    a script routine.  To create a concrete routine, subclass
    :class:`Routine`, declare the routine name, arguments and
    options, and override :meth:`run`.

    The following class attributes should be overriden.

    `name` (a string)
        The name of the routine.  The name must be unique across
        all routines of the script.  If equal to ``''``, the
        routine is called when the script is executed without
        any parameters.

    `aliases` (a list of strings)
        The list of alternative routine names.

    `arguments` (a list of :class:`Argument`)
        The list of routine arguments.

    `options` (a list of :class:`htsql.ctl.option.Option`)
        The list of routine options.

    `hint` (a string or ``None``)
        A one-line description of the routine.

    `help` (a string or ``None``)
        A long description of the routine.

    The constructor of :class:`Routine` accepts the following arguments:

    `ctl`
        The script; an instance of :class:`htsql.ctl.script.Script` or
        of its subclass.

    `attributes`
        A dictionary mapping attribute names to attribute values.
        The attribute names correspond to the routine arguments and
        options; the values are obtained by parsing the command-line
        parameters.
    """

    name = None
    aliases = []
    arguments = []
    options = []
    hint = None
    help = None

    def __init__(self, ctl, attributes):
        self.ctl = ctl
        for name in attributes:
            setattr(self, name, attributes[name])

    def run(self):
        """
        Executes the routine.  Returns an exit code.
        """
        # Override when subclassing.
        raise ScriptError("routine %r is not implemented" % self.name)


