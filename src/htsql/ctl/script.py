#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.ctl.script`
=======================

This module implements a command-line application with subcommands.
"""


from .error import ScriptError
from .routine import Argument, Routine
from .option import Option
from ..util import listof, trim_doc


class Script(object):
    """
    Implements a command-line application with the following interface::

        ctl <routine> <options> <arguments>

    ``ctl``
        The name of the script.

    ``<routine>``
        The name of the subcommand.

    ``<options>``
        A list of routine options.  The set of supported options is
        routine-specific.

        Each option may be written using either an abbreviated form
        (a dash + a character) or a full form (two dashes + the option
        name).  For instance, the `quiet` option may be written as
        ``-q`` or as ``--quiet``.
    
        Some options expect a parameter, which should then follow
        the option name.  For instance, the `input` option expects
        a file name parameter, which could be passed using one
        of the following forms:

        * ``-iFILE``
        * ``-i FILE``
        * ``--input=FILE``
        * ``--input FILE``

        Two or more options written using an abbreviated form could
        share the leading dash.  For instance, ``-q -iFILE`` could
        also be written as ``-qiFILE``.  When this form is used, only
        the last option in the list could accept a parameter.

        Note that the order and the position of options in the command
        line is not fixed.  In particular, an option could preceed
        the routine name or follow a routine argument.  Use special
        parameter ``--`` to indicate that there are no more options
        in the list of the remaining parameters.  This is useful when
        a routine argument starts with a dash.

    ``<arguments>``
        A list of parameters of the routine.  The number and the form
        of the arguments are routine-specific.

    :class:`Script` is the base abstract class implementing a command-line
    application with subcommands.  To create a concrete application,
    do the following:

    1. Create a subclass of :class:`Script` overriding the class
       attributes `routines`, `hint`, `help`, and `copyright`.

    2. The attribute `routines` should be a list of
       :class:`htsql.ctl.routine.Routine` subclasses.  Each subclass
       implements an application routine.

    3. The attributes  `hint`, `help`, and `copyright` should provide
       a short one-line description of the application, a longer
       description or the application, and a copyright notice
       respectively.  They are displayed when the application is
       called without any parameters.

    The constructor of :class:`Script` accepts the following arguments:

    `stdin`
        A file or a file-like object representing the standard input
        stream.

    `stdout`
        A file or a file-like object representing the standard output
        stream.

    `stderr`
        A file or a file-like object representing the standard error
        stream.
    """

    # Override to provide a one-line description of the application.
    hint = None
    # Override to provide a long description of the application.
    help = None
    # Override to provide a copyright notice.
    copyright = None
    # Override to provide a list of supported routines.
    routines = []

    def __init__(self, stdin, stdout, stderr):
        # Standard input, output and error streams.
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        # A mapping of routine_class.name -> routine_class.
        self.routine_by_name = {}
        # A mapping of option.short_name | option.long_name -> option.
        self.option_by_name = {}

        # Populate `routine_by_name` and `option_by_name`.
        self.init_routines()
        self.init_options()

    def init_routines(self):
        # Populate `routine_by_name`; also do some sanity checks.
        for routine_class in self.routines:
            # Sanity check on the routine parameters.
            assert issubclass(routine_class, Routine)
            assert isinstance(routine_class.name, str)
            assert isinstance(routine_class.aliases, listof(str))
            assert isinstance(routine_class.arguments, listof(Argument))
            assert isinstance(routine_class.options, listof(Option))
            attributes = set()
            for parameter in routine_class.arguments+routine_class.options:
                assert parameter.attribute not in attributes, \
                       "duplicate attribute name %r in routine %r" \
                       % (parameter.attribute, routine_class.name)
                attributes.add(parameter.attribute)
            has_list_arguments = False
            has_optional_arguments = False
            for argument in routine_class.arguments:
                if has_optional_arguments:
                    assert not argument.is_mandatory, \
                           "required argument %r follows an optional" \
                           " argument in routine %r" % (argument.name,
                                                        routine_class.name)
                    assert not argument.is_list, \
                           "list argument %r follows an optional argument" \
                           " in routine %r" % (argument.name,
                                               routine_class.name)
                if has_list_arguments:
                    assert argument.is_mandatory, \
                           "optional argument %r follows a list argument" \
                           " in routine %r" % (argument.name,
                                               routine_class.name)
                    assert not argument.is_list, \
                           "list argument %r follows another list argument" \
                           " in routine %r" % (argument.name,
                                               routine_class.name)
                if not argument.is_mandatory:
                    has_optional_arguments = True
                if argument.is_list:
                    has_list_arguments = True

            # Populate `self.routine_by_name`; check for duplicates.
            for name in [routine_class.name]+routine_class.aliases:
                assert name not in self.routine_by_name, \
                       "duplicate routine name: %r" % name
                self.routine_by_name[name] = routine_class

    def init_options(self):
        # Populate `option_by_name`; check for duplicates.
        for routine_class in self.routines:
            for option in routine_class.options:
                for name in [option.short_name, option.long_name]:
                    if name is not None:
                        assert (name not in self.option_by_name or
                                option is self.option_by_name[name]), \
                               "duplicate option name: %r" % name
                        self.option_by_name[name] = option

    def main(self, argv):
        """
        Execute the application.

        `argv` (a list of strings)
            The first element of the list is the path to the application.
            The remaining elements of the list are the application
            parameters.

        Returns an exit status suitable for passing to :func:`sys.exit`.
        The exit status could be ``None``, an integer value, or an exception
        object.  Note that when a non-integer value is passed to
        :func:`sys.exit`, the value is printed and the system exit code
        is set to one.
        """
        try:
            # Parse the command-line arguments.
            parameters = self.parse_argv(argv)
            # Validate the arguments and instantiate a routine object.
            routine = self.make_routine(parameters)
            # Execute the routine.
            return routine.run()

        except (ScriptError, IOError, KeyboardInterrupt), exc:
            # Regular exceptions are passed through and produce a traceback.
            # However for a selected list of exceptions, we want to omit
            # the traceback and just show the error message.  These exceptions
            # include:
            # - `ScriptError`: invalid command-line parameter or some other
            #   problem.  We display ``fatal error: <error description>``.
            # - `IOError`: typically, "File Not Found" or a similar error
            #   caused by an incorrect file name.  We display:
            #   ``[Errno XX] <error description>: <filename>``.
            # - `KeyboardInterrupt`: the user pressed `Ctrl-C`.  We display
            #   nothing.
            return exc

    def out(self, *values, **options):
        """
        Print the values to the standard output stream.

        Supported options are:

        `sep`
            A string to print between values, default is ``' '``.

        `end`
            A string to print after the last value, default is ``'\\n'``.

        `file`
            A file or a file-like object, default is `stdout`.
        """
        return self.out_to(self.stdout, *values, **options)

    def err(self, *values, **options):
        """
        Print the values to the standard error stream.

        Supported options are:

        `sep`
            A string to print between values, default is ``' '``.

        `end`
            A string to print after the last value, default is ``'\\n'``.

        `file`
            A file or a file-like object, default is `stderr`.
        """
        return self.out_to(self.stderr, *values, **options)

    def get_hint(self):
        """
        Returns a short one-line description of the application.
        """
        return self.hint

    def get_help(self, **substitutes):
        """
        Returns a long description of the application.
        """
        if self.help is None:
            return None
        return trim_doc(self.help % substitutes)

    def get_copyright(self):
        """
        Returns a copyright notice.
        """
        return self.copyright

    def parse_argv(self, argv):
        # Parses the command-line arguments; returns a triple:
        # `(executable, values, option_values)`.

        # The path to the script.
        executable = argv[0]

        # List of command line parameters that are not options.
        values = []

        # List of tuples `(name, value)`, where `name` is a short
        # or a long option name and `value` is the corresponding
        # value (`None` if the option does not expect a parameter).
        option_values = []

        # Instructs not to try parsing the remaining parameters as options;
        # set by special parameter `--`.
        no_more_options = False

        # The position of the parameter being parsed.
        idx = 1

        while idx < len(argv):
            # Parse the next parameter.
            arg = argv[idx]
            idx += 1

            if no_more_options or arg == '-' or not arg.startswith('-'):
                # A regular argument, not an option.
                values.append(arg)

            elif arg == '--':
                # "No more options" indicator.
                no_more_options = True

            elif arg.startswith('--'):
                # An option in the long form, one of:
                #   `--option`
                #   `--option=value`
                #   `--option value`
                name = arg
                value = None
                if '=' in name:
                    name, value = name.split('=', 1)
                if name not in self.option_by_name:
                    raise ScriptError("unknown option %r" % name)
                option = self.option_by_name[name]
                if option.with_value:
                    if value is None:
                        if idx == len(argv):
                            raise ScriptError("option %r requires a parameter"
                                              % name)
                        value = argv[idx]
                        idx += 1
                else:
                    if value is not None:
                        raise ScriptError("option %r does not expect"
                                          " a parameter" % name)
                option_values.append((name, value))

            elif arg.startswith('-'):
                # An option or a list of options in the short form, one of:
                #   `-XYZ`
                #   `-XYZvalue`
                #   `-XYZ value`
                pos = 1
                while pos < len(arg):
                    name = '-'+arg[pos]
                    pos += 1
                    value = None
                    if name not in self.option_by_name:
                        raise ScriptError("unknown option %r" % name)
                    option = self.option_by_name[name]
                    if option.with_value:
                        if pos < len(arg):
                            value = arg[pos:]
                            pos = len(arg)
                        else:
                            if idx == len(argv):
                                raise ScriptError("option %r requires a"
                                                  " parameter" % name)
                            value = argv[idx]
                            idx += 1
                    option_values.append((name, value))

            else:
                # Not reachable.
                assert False

        # We are done; return the parameters.
        parameters = (executable, values, option_values)
        return parameters

    def make_routine(self, parameters):
        # Takes the parsed command-line parameters, validates them and
        # instantiate a routine.

        # See `parse_argv()` for the description of `parameters`.
        executable, values, option_values = parameters

        # Determine the routine.  The first command-line argument is
        # the routine name; when there are no arguments, we look for
        # a routine with an empty name.
        if values:
            name = values.pop(0)
        else:
            name = ''
        if name not in self.routine_by_name:
            raise ScriptError("unknown routine %r" % name)
        routine_class = self.routine_by_name[name]

        # Values for the routine attributes.
        attributes = {}

        # Populate the attribute values from the command-line parameters.
        common_attributes = self.make_global_attributes(routine_class,
                                                        executable)
        argument_attributes = self.make_argument_attributes(routine_class,
                                                            values)
        option_attributes = self.make_option_attributes(routine_class,
                                                        option_values)
        attributes.update(common_attributes)
        attributes.update(argument_attributes)
        attributes.update(option_attributes)

        # Generate and return a routine.
        routine = routine_class(self, attributes)
        return routine

    def make_global_attributes(self, routine_class, executable):
        # Generate the global attributes; currently the only
        # such attribute is `executable`.
        attributes = {}
        attributes['executable'] = executable
        return attributes

    def make_argument_attributes(self, routine_class, values):
        # Matches the command-line parameters against the routine arguments,
        # validates the values and generates a dictionary of the corresponding
        # routine attributes.
        attributes = {}

        # Check if the number of command-line values matches the number
        # of routine arguments.
        min_bound = 0
        max_bound = 0
        for argument in routine_class.arguments:
            if argument.is_mandatory:
                min_bound += 1
            if max_bound is not None:
                max_bound += 1
            if argument.is_list:
                max_bound = None
        if len(values) < min_bound:
            number = "%s argument" % min_bound
            if min_bound != 1:
                number = "%ss" % number
            if min_bound != max_bound:
                number = "at least %s" % number
            raise ScriptError("expected %s; got %s" % (number, len(values)))
        if max_bound is not None and max_bound < len(values):
            if max_bound == 0:
                number = "no argument"
            else:
                number = "%s argument" % max_bound
            if max_bound != 1:
                number = "%ss" % number
            if min_bound != max_bound:
                number = "at most %s" % max_bound
            raise ScriptError("expected %s; got %s" % (number, len(values)))

        # The number of required arguments.
        reserved = min_bound

        # Go through the list of arguments, fetch their values and assign them
        # to the attributes.
        for argument in routine_class.arguments:
            if argument.is_mandatory:
                reserved -= 1
            is_default = False
            if argument.is_list:
                if reserved:
                    value = values[:-reserved]
                    values = values[-reserved:]
                else:
                    value = values[:]
                    values = []
                is_default = (not value)
            else:
                if values:
                    value = values.pop(0)
                else:
                    is_default = True
            assert not argument.is_mandatory or not is_default
            if is_default:
                value = argument.default
            else:
                try:
                    value = argument.validator(value)
                except ValueError, exc:
                    raise ScriptError("invalid parameter %r: %s"
                                      % (argument.attribute, exc))
            attributes[argument.attribute] = value

        return attributes

    def make_option_attributes(self, routine_class, option_values):
        # Matches the option values from the command-line parameters
        # with the routine options, validates the values and produces
        # a dictionary of the corresponding routine attributes.
        attributes = {}

        # Prefill the dictionary with default values.
        for option in routine_class.options:
            if option.with_value:
                value = option.default
            else:
                value = False
            attributes[option.attribute] = value

        # The set of options we already encountered.
        duplicates = set()

        # Process the option values from the command-line parameters.
        for name, value in option_values:
            option = self.option_by_name[name]
            if option not in routine_class.options:
                raise ScriptError("unexpected option %r" % name)
            if option in duplicates:
                raise ScriptError("duplicate option %r" % name)
            duplicates.add(option)
            if option.with_value:
                try:
                    value = option.validator(value)
                except ValueError, exc:
                    raise ScriptError("invalid parameter %r: %s"
                                      % (option.attribute, exc))
            else:
                value = True
            attributes[option.attribute] = value

        return attributes

    def out_to(self, stream, *values, **options):
        # Print the values to the stream, valid options are
        # `sep`, `end`, and `file` -- the meaning of the arguments
        # is the same as for the `print()` function in Python 3.
        sep = options.pop('sep', ' ')
        end = options.pop('end', '\n')
        stream = options.pop('file', stream)
        assert not options
        stream.write(sep.join(str(value) for value in values) + end)
        stream.flush()


