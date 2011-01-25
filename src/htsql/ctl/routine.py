#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.ctl.routine`
========================

This module defines basic classes for implementing script routines.
"""


from .error import ScriptError
from ..validator import Validator
from ..util import maybe, trim_doc
import re


# Indicates that the argument has no default value and thus cannot be omitted.
MANDATORY_ARGUMENT = object()

class Argument(object):
    """
    Describes an argument of a script routine.

    `attribute` (a string)
        The name of the routine attribute.  When the routine is
        initialized, the value of the argument is assigned to
        the attribute.

    `validator` (:class:`htsql.validator.Validator`)
        The validator for the argument value.

    `default`
        The default value of the argument.  If `default` is not
        provided, the argument value is always required.
        The `is_mandatory` attribute indicates if the default
        value is omitted.

    `is_list` (Boolean)
        If set, the argument may accept more than one parameter.
        In this case, the argument value is a list of parameters.

    `hint` (a string or ``None``)
        A short one-line description of the argument.
    """

    def __init__(self, attribute, validator,
                 default=MANDATORY_ARGUMENT, is_list=False, hint=None):
        # Sanity check on the arguments.
        assert isinstance(attribute, str)
        assert re.match(r'^[a-zA-Z_][0-9a-zA-Z_]*$', attribute)
        assert isinstance(validator, Validator)
        assert isinstance(is_list, bool)
        assert isinstance(hint, maybe(str))

        self.attribute = attribute
        self.validator = validator
        self.default = default
        self.is_mandatory = (default is MANDATORY_ARGUMENT)
        self.is_list = is_list
        self.hint = hint

    def get_hint(self):
        """
        Returns a short one-line description of the option.
        """
        return self.hint

    def get_signature(self):
        """
        Returns the argument signature.
        """
        signature = self.attribute.replace('_', '-').upper()
        if self.is_list:
            signature = "%s..." % signature
        return signature


class Routine(object):
    """
    Describes a script routine.

    :class:`Routine` is a base abstract class for implementing
    a script routine.  To create a concrete routine, subclass
    :class:`Routine`, declare the routine name, arguments and
    options, and override :meth:`run`.

    The following class attributes should be overridden.

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
        A long description of the routine.  Keep the line width
        at 72 characters.

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

    @classmethod
    def get_hint(cls):
        """
        Returns a short one-line description of the routine.
        """
        return cls.hint

    @classmethod
    def get_help(cls, **substitutes):
        """
        Returns a long description of the routine.
        """
        if cls.help is None:
            return None
        return trim_doc(cls.help % substitutes)

    @classmethod
    def get_signature(cls):
        """
        Returns the routine signature.
        """
        # The routine signature has the form:
        # {name} {arg}... [{arg} [...]]
        signature = [cls.name]
        bracket_depth = 0
        for idx, argument in enumerate(cls.arguments):
            argument_signature = argument.get_signature()
            if not argument.is_mandatory:
                argument_signature = "[%s" % argument_signature
                bracket_depth += 1
            if idx == len(cls.arguments)-1 and bracket_depth != 0:
                argument_signature = "%s%s" \
                                     % (argument_signature, "]"*bracket_depth)
            signature.append(argument_signature)
        return " ".join(signature)

    @classmethod
    def get_feature(cls, name):
        """
        Finds some routine feature by name.
        """
        raise ScriptError("routine %r does not support pluggable features"
                          % (cls.name))

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


