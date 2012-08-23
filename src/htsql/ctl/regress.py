#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql.ctl.regress`
========================

This module implements the `regress` routine.
"""


from .error import ScriptError
from .routine import Argument, Routine
from .option import (InputOption, TrainOption, PurgeOption,
                     ForceOption, QuietOption)
from .request import Request
from ..core.validator import (Validator, BoolVal, StrVal, WordVal,
                              ChoiceVal, IntVal, UFloatVal, DBVal, SeqVal,
                              MapVal, ClassVal, AnyVal)
from ..core.util import maybe, trim_doc, DB
import traceback
import StringIO
import sys
import os, os.path
import shutil
import re
import difflib
import tempfile
import subprocess
import atexit
import time
import yaml, yaml.constructor


# Indicates that a field has no default value and therefore cannot be omitted.
MANDATORY_FIELD = object()

# Return values for `ask_*` methods indicating the user-chosen action.
DO_CONTINUE = object()
DO_DISCARD = object()
DO_HALT = object()
DO_RECORD = object()
DO_SAVE = object()
DO_SKIP = object()


class TermStringIO(StringIO.StringIO):
    """
    A readable file-like object with an "echo".  Whenever some content is read
    from it, the same content is echoed to the specified `output` stream.

    Use :class:`TermStringIO` to preserve the content of interactive
    sessions with pre-recorded input.  Assign::

        sys.stdout = StringIO.StringIO()
        sys.stdin = TermStringIO(input, sys.stdout)

    where `input` contains the pre-recorded input data.  After the
    session is done, the content of `sys.stdout` will be the same as
    if the session was performed on a real terminal with echo enabled.

    `buf` (a string)
        The content of the stream.

    `output` (a writable file-like object)
        A stream that records data being read.
    """

    def __init__(self, buf, output):
        StringIO.StringIO.__init__(self, buf)
        self.output = output

    def read(self, n=-1):
        data = StringIO.StringIO.read(self, n)
        self.output.write(data)
        return data

    def readline(self, length=None):
        data = StringIO.StringIO.readline(self, length)
        self.output.write(data)
        return data


class Field(object):
    """
    Describes a parameter of test data.

    `attribute` (a string)
        The name of the attribute that contains the field value.

    `val` (:class:`htsql.validator.Validator`)
        The validator for the field values.

    `default`
        The default value of the field.  If not provided, the field
        cannot be omitted.  The `is_mandatory` attribute indicates if
        the `default` value is provided.

    `hint` (a string or ``None``)
        A short one-line description of the field.
    """

    # Use it to filter out `AnyField` instances.
    is_any = False

    def __init__(self, attribute, val,
                 default=MANDATORY_FIELD, hint=None):
        # Sanity check on the arguments.
        assert isinstance(attribute, str)
        assert re.match(r'^[a-zA-Z_][0-9a-zA-Z_]*$', attribute)
        assert isinstance(val, Validator)
        assert isinstance(hint, maybe(str))

        self.attribute = attribute
        self.val = val
        self.default = default
        self.is_mandatory = (default is MANDATORY_FIELD)
        self.hint = hint

    def get_hint(self):
        """
        Returns short one-line description of the field.
        """
        return self.hint

    def get_signature(self):
        """
        Returns the field name.
        """
        signature = self.attribute.replace('_', '-')
        if self.is_mandatory:
            signature += '*'
        return signature


class AnyField(object):
    """
    Indicates that test data may contain extra fields.

    Add ``AnyField()`` to the `fields` list to indicate that YAML
    representation of test data may contain some attributes not
    described by other fields.  These extra attributes will be
    silently ignored.
    """

    # Use it to filter out `AnyField` instances.
    is_any = True


class TestData(object):
    """
    Represents input or output data of a test case.

    This is an abstract class.  Create a subclass of :class:`TestData`
    to describe input or output data for a specific test kind.  You need
    to specify the format of test data using the `fields` class attribute.

    The `fields` attribute is a list of :class:`Field` instances.  Each
    field describes an attribute of test data.

    Instances if :class:`TestData` are YAML-serializable.  A instance of
    a :class:`TestData` subclass is represented as a mapping YAML node.
    The sets of keys and the format of the values come from the `fields`
    list.  Add an :class:`AnyField` instance to `fields` to indicate
    that the mapping node may contain some extra fields (which are to
    be ignored).

    The constructor of :class:`TestData` accepts the following arguments:

    `routine` (:class:`RegressRoutine`)
        The routine that started the testing.

    `case_class` (a subclass of :class:`TestCase`)
        A test type.  The object being constructed is an instance
        of either `case_class.Input` or `case_class.Output`.

    `attributes` (a dictionary)
        A dictionary of attributes and their values.  The set of
        attributes is declared using the `fields` class variable.

    `location` (a string or ``None``)
        When the test data is loaded from a YAML file, `location`
        indicates the location of the corresponding YAML node.
    """

    fields = []

    def __init__(self, routine, case_class, attributes, location=None):
        # Sanity check on the arguments.
        assert isinstance(routine, RegressRoutine)
        assert issubclass(case_class, TestCase)
        assert self.__class__ in [case_class.Input, case_class.Output]
        assert isinstance(attributes, dict)
        assert isinstance(location, maybe(str))

        self.routine = routine
        self.case_class = case_class
        for name in attributes:
            setattr(self, name, attributes[name])
        self.location = location
        self.init_attributes()

    def init_attributes(self):
        """
        Normalize field values.
        """
        # Override in a subclass if you need to massage some field values.

    def __str__(self):
        # Produces the value of the first mandatory field.
        title_attribute = None
        for field in self.fields:
            if field.is_any:
                continue
            if field.is_mandatory:
                title_attribute = field.attribute
        if title_attribute is None:
            return ''
        return repr(getattr(self, title_attribute))

    def __repr__(self):
        return "<%s.%s %s>" % (self.case_class.__name__,
                               self.__class__.__name__, self)


class TestCase(object):
    """
    Describes a test type.

    This an abstract class.  Create a subclass of :class:`TestCase`
    to describe a new type of test case.  When subclassing, define
    the following class attributes:

    `name` (a string)
        The name of the test.

    `hint` (a string)
        Short one-line description of the test.

    `help` (a string)
        Long description of the test.

    `Input` (a subclass of :class:`TestData`)
        The format of the test input.

    `Output` (a subclass of :class:`TestData` or ``None``)
        The format of the test output.

    You also need to override methods :meth:`verify` and :meth:`train`
    to specify how to execute the test case in a normal and in a train mode.

    The constructor of :class:`TestCase` takes the following arguments:

    `routine` (:class:`RegressRoutine`)
        The routine that started the testing.

    `state`
        An object keeping the mutable testing state.

    `input` (an instance of `Input`)
        Input test data.

    `output` (an instance of `Output` or ``None``)
        Expected output test data.
    """

    name = None
    hint = None
    help = None

    # Override to declare the format of input and output test data.
    Input = None
    Output = None

    @classmethod
    def get_hint(cls):
        """
        Returns short one-line description of the test case.
        """
        return cls.hint

    @classmethod
    def get_help(cls):
        """
        Returns long description of the test case.
        """
        # Produce:
        # {help}
        # 
        # Input data:
        #   {field.signature} - {field.hint}
        #   ...
        #
        # Output data:
        #   {field.signature} - {field.hint}
        #   ...
        lines = []
        help = trim_doc(cls.help)
        if help is not None:
            lines.append(help)
        for data_class in [cls.Input, cls.Output]:
            if data_class is None:
                continue
            if lines:
                lines.append("")
            lines.append("%s data:" % data_class.__name__)
            for field in data_class.fields:
                if field.is_any:
                    continue
                signature = field.get_signature()
                hint = field.get_hint()
                if hint is not None:
                    lines.append("  %-24s : %s" % (signature, hint))
                else:
                    lines.append("  %s" % signature)
        return "\n".join(lines)

    def __init__(self, routine, state, input, output):
        # Sanity check on the arguments.
        assert isinstance(routine, RegressRoutine)
        assert isinstance(state, routine.state_class)
        if self.Input is None:
            assert input is None
        else:
            assert isinstance(input, self.Input)
        if self.Output is None:
            assert output is None
        else:
            assert isinstance(output, maybe(self.Output))

        self.routine = routine
        self.state = state
        self.input = input
        self.output = output

        # When the test case is in the quiet mode (indicated by `is_quiet`),
        # all output is redirected to `quiet_buffer`.  If for some reason
        # the test case leaves the quiet mode, all the accumulated data
        # is dumped to the standard output stream.
        self.is_quiet = routine.quiet
        self.quiet_buffer = StringIO.StringIO()

    def make_output(self, **attributes):
        # Generate a new test output record with the given attributes.
        return self.Output(self.routine, self.__class__, attributes)

    @classmethod
    def matches(cls, input, output):
        """
        Checks if the given input and output records belong to the same
        test case.

        Note that we assume that both test input and test output have
        a field with the same attribute name.  This attribute is called
        the key attribute.  Input data matches output data when the
        values of their key attribute are equal.
        """
        # Sanity check on the arguments.
        assert isinstance(input, maybe(TestData))
        assert isinstance(output, maybe(TestData))

        # `input` and `output` must be instances of `Input` and `Output`
        # classes of the test case.
        if cls.Input is None or cls.Output is None:
            return False
        if not isinstance(input, cls.Input):
            return False
        if not isinstance(output, cls.Output):
            return False

        # Find the key attribute: one that is declared both as an input field
        # and as an output field.
        key_attribute = None
        input_attributes = [field.attribute for field in cls.Input.fields
                                            if not field.is_any]
        output_attributes = [field.attribute for field in cls.Output.fields
                                             if not field.is_any]
        for attribute in input_attributes:
            if attribute in output_attributes:
                key_attribute = attribute
                break
        if key_attribute is None:
            return False

        # `input` and `output` are matched when the values of their key
        # attributes are equal.
        if getattr(input, key_attribute) != getattr(output, key_attribute):
            return False
        return True

    def get_suites(self):
        """
        For container test cases, returns a set of test suites that belong
        to the test case; otherwise returns an empty set.
        """
        return set()

    def out(self, *values, **options):
        """
        Print values to the standard output stream.

        :meth:`out` supports the same options as
        :meth:`htsql.ctl.script.Script.out` and an extra option:

        `indent`
            A number of spaces to print before the first value,
            default is ``0``.
        """
        indent = options.pop('indent', 0)
        if indent:
            values = (' '*(indent-1),) + values
        # If the test case is in the quiet mode, redirect the output
        # to `quiet_buffer`.
        if self.is_quiet and 'file' not in options:
            options['file'] = self.quiet_buffer
        self.routine.ctl.out(*values, **options)

    def ask(self, message, choices):
        """
        Asks the user a question; returns the reply.

        `message` (a string)
            The question.

        `choices` (a list of strings)
            The list of valid replies.

        Typically the question has the form::
        
            Press ENTER to perform <the default action>,
                  'x'+ENTER to perform <another action>,
                  'y'+ENTER to perform <another action>,
                  'z'+ENTER to perform <another action>.

        In this case, `choices` should be equal to::

            ['', 'x', 'y', 'z']

        The reply is stripped of leading and trailing whitespaces
        and translated to the lower case.
        """
        # Leave the quiet mode and print the question.
        self.force_out()
        self.out()
        self.out(">>>", message)
        line = None

        # Repeat till we get a valid answer.
        while line not in choices:
            self.out("> ", end='')
            line = self.routine.ctl.stdin.readline().strip().lower()

        return line

    def ask_halt(self):
        """
        Ask if the user wants to halt the tests.

        Returns `DO_HALT` or `DO_CONTINUE`.
        """
        line = self.ask("Press ENTER to halt,"
                        " 'c'+ENTER to continue", ['', 'c'])
        if line == '':
            return DO_HALT
        if line == 'c':
            return DO_CONTINUE

    def ask_record(self):
        """
        Ask if the user wants to remember the new output of a test case.

        Returns `DO_RECORD`, `DO_SKIP`, or `DO_HALT`.
        """
        line = self.ask("Press ENTER to record,"
                        " 's'+ENTER to skip,"
                        " 'h'+ENTER to halt", ['', 's', 'h'])
        if line == '':
            return DO_RECORD
        if line == 's':
            return DO_SKIP
        if line == 'h':
            return DO_HALT

    def ask_save(self):
        """
        Ask if the user wants to save the updated output data.

        Returns `DO_SAVE` or `DO_DISCARD`.
        """
        line = self.ask("Press ENTER to save changes,"
                        " 'd'+ENTER to discard changes", ['', 'd'])
        if line == '':
            return DO_SAVE
        if line == 'd':
            return DO_DISCARD

    def out_exception(self, exc_info):
        """
        Prints an exception traceback.
        """
        # Obey the quiet mode: redirect to `quiet_buffer` if necessary.
        if self.is_quiet:
            file = self.quiet_buffer
        else:
            file = self.routine.ctl.stdout

        exc_type, exc_value, exc_traceback = exc_info
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                                  file=file)

    def out_sep(self, sep="-", length=72):
        """
        Prints a separator: a long line of dashes.
        """
        self.out(sep*length)

    def out_header(self):
        """
        Prints a nice header describing the test case.
        """
        # Print:
        # ---------------- ... -
        #   {NAME} {value}
        #   ({input.location})
        # where {value} is the value of the first field of the input data.
        self.out_sep()
        if not self.input.fields or self.input.fields[0].is_any:
            return
        attribute = self.input.fields[0].attribute
        value = getattr(self.input, attribute)
        if value is not None:
            if isinstance(value, list):
                value = " ".join(str(item) for item in value)
            self.out("%s %s" % (self.name.upper(), value), indent=2)
        if self.input.location is not None:
            self.out("(%s)" % self.input.location, indent=2)

    def halted(self, message=None):
        """
        Indicate that the test case failed and stop the tests.
        """
        self.force_out()
        if message is not None:
            self.out(message)
        self.state.failed += 1
        self.state.is_exiting = True

    def failed(self, message=None):
        """
        Indicate that the test case failed; stop the tests unless
        ``--force`` or ``--train`` flags are set.
        """
        self.force_out()
        if message is not None:
            self.out(message)
        self.state.failed += 1
        if not (self.routine.force or self.routine.train):
            self.state.is_exiting = True

    def updated(self, message=None):
        """
        Indicate that the output of the test case has been updated.
        """
        self.force_out()
        if message is not None:
            self.out(message)
        self.state.updated += 1

    def passed(self, message=None):
        """
        Indicate that the test case passed.
        """
        if message is not None:
            self.out(message)
        self.state.passed += 1

    def force_out(self):
        # Leave the quiet mode; flush the content of `quiet_buffer`
        # to the standard output stream.
        if not self.is_quiet:
            return
        self.is_quiet = False
        buffer = self.quiet_buffer.getvalue()
        self.routine.ctl.stdout.write(buffer)
        self.routine.ctl.stdout.flush()

    def verify(self):
        """
        Executes the test case.

        This method runs the test case with the given input data.
        If the test completed without errors, compare the produced
        output with the given expected output.

        The test case fails if

        - the test failed to complete without errors;
        - or the expected test output is not provided;
        - or the expected test output is not equal to the actual test output.

        Some test cases may not generate output; in this case the test
        passes if it is completed without errors.
        """
        # Override when subclassing.
        raise ScriptError("test %r is not implemented" % self.name)

    def train(self):
        """
        Executes the test case in the training mode; returns the output data.

        In the train mode, when the expected test output is not equal to the
        actual test output, the user is given a choice to update the expected
        test output.

        Note that when the output has not been changed or the user refused
        to update it, the method must return the original output data,
        ``self.output``.
        """
        # Override when subclassing if the test case requires test output data.
        # Otherwise, just run the test case in the normal mode.
        self.verify()
        return None


class SkipTestCase(TestCase):
    """
    Implements a skippable test case.

    This is an abstract mixin class; subclasses should call :meth:`skipped`
    to check if the test case is enabled or not.
    """

    class Input(TestData):
        fields = [
                Field('skip', BoolVal(), False,
                      hint="""do not run the test"""),
                Field('ifdef', SeqVal(StrVal()), None,
                      hint="""run only if a given toggle is active"""),
                Field('ifndef', SeqVal(StrVal()), None,
                      hint="""run only if a given toggle is inactive"""),
        ]

    def skipped(self):
        """
        Checks if the test is disabled.
        """
        # Verify if the test is unconditionally disabled.
        if self.input.skip:
            return True
        # If a positive guard is set, check that at least one of the required
        # toggles is active.
        if self.input.ifdef is not None:
            if not (self.state.toggles & set(self.input.ifdef)):
                return True
        # If a negative guard is set, check that none of the suppressed
        # toggles is active.
        if self.input.ifndef is not None:
            if self.state.toggles & set(self.input.ifndef):
                return True
        # The test is not skipped.
        return False


class DefineTestCase(SkipTestCase):
    """
    Activates a named toggle.
    """

    name = "define"
    hint = """activate a toggle"""
    help = """
    This test case activates a toggle variable.  A toggle allows one
    to conditionally enable or disable some test cases using `ifdef`
    and `ifndef` directives.
    """

    class Input(TestData):
        fields = [
                Field('define', SeqVal(StrVal()),
                      hint="""activate the given toggles"""),
        ] + SkipTestCase.Input.fields

    def verify(self):
        # Check if the test is skipped.
        if self.skipped():
            return
        # Activates the toggles.
        for toggle in self.input.define:
            self.state.toggles.add(toggle)


class RunAndCompareTestCase(SkipTestCase):
    """
    Implements common methods for a broad category of test cases.

    This class implements common scenario: run the test, get the output
    and compare it with the expected output.

    This is an abstract class; create a subclass to implement a concrete
    test case.  The following methods has to be overridden: :meth:`execute`,
    :meth:`render` and :meth:`differs`.
    """

    def out_lines(self, lines, indent=0):
        """
        Prints the lines with the specified identation.
        """
        for line in lines:
            # If `line` is UTF-8 encoded, print it literally;
            # otherwise, replace special and non-ASCII characters
            # with dots.
            try:
                line.decode('utf-8')
            except UnicodeDecodeError:
                line = re.sub(r'[\x00-\x1F\x7E-\xFF]', '.', line)
            self.out(line.rstrip(), indent=indent)

    def out_diff(self, old_output, new_output):
        """
        Prints the delta between two test outputs.
        """
        # Sanity check on the arguments.
        assert isinstance(old_output, maybe(self.Output))
        assert isinstance(new_output, self.Output)

        # Render the outputs to the lists of lines.
        old_lines = self.render(old_output)
        new_lines = self.render(new_output)

        # This function is supposed to be called in two cases:
        # when there is no expected output, but only the actual output,
        # and when the expected output differs from the actual output.
        # However it may also happen that the function is called with
        # two identical outputs, or that the `render` method hides
        # the difference.
        if old_lines is None:
            self.out("=== the test output is new")
        elif old_lines != new_lines:
            self.out("=== the test output is changed")
        else:
            self.out("=== the test output is not changed")
        self.out()

        # Display the actual output if there is no expected output;
        # otherwise display the delta between the expected and the actual
        # output in the unified diff format.
        if old_lines is None or old_lines == new_lines:
            lines = new_lines
        else:
            diff = difflib.unified_diff(old_lines, new_lines,
                                        n=2, lineterm='')
            # Strip the leading `---` and `+++` lines of the unified diff.
            lines = list(diff)[2:]
        self.out_lines(lines, indent=2)

    def render(self, output):
        """
        Converts the output data to a list of lines.
        """
        # Override when subclassing.
        raise NotImplementedError()

    def execute(self):
        """
        Runs the test case; returns the produced output.

        Returns ``None`` if an error occured when running the test case.
        """
        # Override when subclassing.
        raise NotImplementedError()

    def differs(self, old_output, new_output):
        """
        Checks if the actual test output differs from the expected test output.
        """
        # Override when subclassing.
        raise NotImplementedError()

    def verify(self):
        # Check if the test is skipped.
        if self.skipped():
            return

        # Display the header.
        self.out_header()

        # When no expected test output, fail without executing the test.
        if self.output is None:
            return self.failed("*** no output data found")

        # Execute the test; get the actual test output.
        new_output = self.execute()

        # `None` indicates that an error occurred; `execute()` is responsible
        # for displaying an error message, so we just update the status and
        # exit.
        if new_output is None:
            return self.failed()

        # Compare the expected and the actual outputs, fail if they are
        # different.
        if self.differs(self.output, new_output):
            self.out_diff(self.output, new_output)
            return self.failed("*** unexpected test output")

        # The actual output coincides with the expected output; we are good.
        return self.passed()

    def train(self):
        # Check if the test is skipped.
        if self.skipped():
            return self.output

        # Display the header.
        self.out_header()

        # Execute the test; get the actual test output.
        new_output = self.execute()

        # We need to handle three possible outcomes: an error occurred
        # when running the test, the expected output differs from the
        # actual output and the expected output coincides with the actual
        # output.

        # An error occurred while running the test.
        if new_output is None:
            # Ask the user if they want to stop the testing; the expected
            # output is not updated.
            reply = self.ask_halt()
            if reply is DO_HALT:
                self.halted("*** halting")
            else:
                self.failed()
            return self.output

        # The actual output differs from the expected output.
        if self.differs(self.output, new_output):
            # Display the difference.
            self.out_diff(self.output, new_output)
            # Ask the user if they want to record the new output,
            # keep the old output, or halt the testing.
            reply = self.ask_record()
            if reply is DO_HALT:
                self.halted("*** halting")
                return self.output
            if reply is DO_RECORD:
                if self.output is None:
                    self.updated("*** recording new test output")
                else:
                    self.updated("*** recording updated test output")
                return new_output
            self.failed()
            return self.output

        # The actual output coincides with the expected output; note that
        # the caller checks if ``case.train() is case.output`` to learn
        # if the output is updated.
        self.passed()
        return self.output


class AppTestCase(SkipTestCase):
    """
    Configures the HTSQL application.
    """

    name = "app"
    hint = """configure the HTSQL application"""
    help = """
    To run HTSQL requests, the testing engine needs to create an HTSQL
    application.  This test case allows you to configure the application
    parameters.
    """

    class Input(TestData):
        fields = [
                Field('db', DBVal(is_nullable=True),
                      hint="""the connection URI"""),
                Field('extensions', MapVal(StrVal(),
                                           MapVal(StrVal(), AnyVal())),
                      default={},
                      hint="""include extra extensions"""),
                Field('save', StrVal(), default=None,
                      hint="""name of the configuration""")
        ] + SkipTestCase.Input.fields

    def out_header(self):
        # Overriden to avoid printing the password to the database.

        # Clone `input.db`, but omit the password.
        db = self.input.db
        if db is not None:
            sanitized_db = DB(engine=db.engine,
                              username=db.username,
                              password=None,
                              host=db.host,
                              port=db.port,
                              database=db.database,
                              options=db.options)
        else:
            sanitized_db = "-"

        # Print:
        # ---------------- ... -
        #   APP {sanitized_db}
        #   ({input.location})
        self.out_sep()
        self.out("%s %s" % (self.name.upper(), sanitized_db), indent=2)
        if self.input.location is not None:
            self.out("(%s)" % self.input.location, indent=2)

    def verify(self):
        # Check if the test is skipped.
        if self.skipped():
            return

        # Display the header.
        self.out_header()

        # Create an application and update the testing state.  The created
        # application will be in effect for the subsequent tests in the
        # current suite and all the nested suites unless overridden.
        from htsql import HTSQL
        self.state.app = None
        try:
            self.state.app = HTSQL(self.input.db,
                                   self.input.extensions)
        except Exception:
            self.out_exception(sys.exc_info())
            return self.failed("*** an exception occured while"
                               " initializing an HTSQL application")

        # Record the configuration.
        if self.input.save is not None:
            self.state.saves[self.input.save] = (self.input.db,
                                                 self.input.extensions)

        return self.passed()


class LoadAppTestCase(SkipTestCase):
    """
    Loads an existing configuration of an HTSQL application.
    """

    name = "load-app"
    hint = """activate an existing HTSQL application"""
    help = """
    This test case loads a previously saved application configuration.
    """

    class Input(TestData):
        fields = [
                Field('load', StrVal(),
                      hint="""name of the configuration"""),
                Field('extensions', MapVal(StrVal(),
                                           MapVal(StrVal(), AnyVal())),
                      default={},
                      hint="""include extra extensions"""),
                Field('save', StrVal(), default=None,
                      hint="""name of the new configuration""")
        ] + SkipTestCase.Input.fields

    def verify(self):
        # Check if the test is skipped.
        if self.skipped():
            return

        # Display the header.
        self.out_header()

        # Find the configuration data; complain if not found.
        if self.input.load not in self.state.saves:
            return self.failed("*** unknown configuration name %s"
                               % self.input.load)
        configuration = self.state.saves[self.input.load]

        # Add new extensions.
        configuration = configuration+(self.input.extensions,)

        # Create an application and update the testing state.
        from htsql import HTSQL
        self.state.app = None
        try:
            self.state.app = HTSQL(*configuration)
        except Exception:
            self.out_exception(sys.exc_info())
            return self.failed("*** an exception occured while"
                               " initializing an HTSQL application")

        # Record the new configuration.
        if self.input.save is not None:
            self.state.saves[self.input.save] = configuration

        return self.passed()


class IncludeTestCase(SkipTestCase):
    """
    Loads input test data from a file.
    """

    name = "include"
    hint = """load input data from a file"""
    help = """
    This test case allows you to execute a test case or a test suite defined
    in a separate file.
    """

    class Input(TestData):
        fields = [
                Field('include', StrVal(),
                      hint="""file containing input test data"""),
        ] + SkipTestCase.Input.fields

    class Output(TestData):
        fields = [
                Field('include', StrVal(),
                      hint="""file containing input test data"""),
                Field('output', ClassVal(TestData),
                      hint="""the corresponding output test data"""),
        ]

    def __init__(self, routine, state, input, output):
        super(IncludeTestCase, self).__init__(routine, state, input, output)

        # Load the input data and create the corresponding test case.
        self.included_input = routine.load_input(self.input.include)
        case_class = self.included_input.case_class
        self.included_output = None
        if self.output is not None:
            if case_class.matches(self.included_input, self.output.output):
                self.included_output = self.output.output
        self.case = case_class(routine, state,
                               self.included_input,
                               self.included_output)

    def get_suites(self):
        # Get the set of nested suites.
        return self.case.get_suites()

    def verify(self):
        # Check if the test is skipped.
        if self.skipped():
            return
        # Run the included test.
        self.case.verify()

    def train(self):
        # Check if the test is skipped.
        if self.skipped():
            return self.output

        # Run the included test; get the output.
        new_output = self.case.train()

        # Three outcomes are possible: the test generated no output, in this
        # case we don't need to create an output record either; the test
        # generated new or updated output, we have to update our output as
        # well; and finally, the test output didn't change, we could keep
        # ours too.
        if new_output is None:
            output = None
        elif new_output is not self.included_output:
            output = self.make_output(include=self.input.include,
                                      output=new_output)
        else:
            output = self.output
        return output


class SuiteTestCase(SkipTestCase):
    """
    Implements a container of test cases.
    """

    name = "suite"
    hint = """contains other test cases"""
    help = """
    A test suite is a container of test cases.  Typically, it is the
    top-level test case in a test file.
    
    The testing engine allows you to specify what suites to run by their
    ids.
    """

    class Input(TestData):
        fields = [
                Field('title', StrVal(),
                      hint="""the description of the suite"""),
                Field('id', StrVal(), None,
                      hint="""the code name of the suite"""),
                Field('output', StrVal(), None,
                      hint="""file to save the output of the tests"""),
                Field('tests', SeqVal(ClassVal(TestData)),
                      hint="""a list of test inputs"""),
        ] + SkipTestCase.Input.fields

        def init_attributes(self):
            # When `id` is not specified, generate it from the title.
            if self.id is None:
                self.id = self.title.lower().replace(' ', '-')

    class Output(TestData):
        fields = [
                Field('id', StrVal(),
                      hint="""the code name of the suite"""),
                Field('tests', SeqVal(ClassVal(TestData)),
                      hint="""a list of test outputs"""),
        ]

    def __init__(self, routine, state, input, output):
        super(SuiteTestCase, self).__init__(routine, state, input, output)

        # A test suite has an ability to save its test output to a separate
        # file.  In this case, `self.ext_output` contains the test data
        # loaded from the file.
        self.ext_output = None
        if input.output is not None and os.path.exists(input.output):
            ext_output = routine.load_output(input.output)
            if self.matches(input, ext_output):
                self.ext_output = ext_output

        # Generate a list of test cases.
        self.cases = []
        self.cases_state = TestState()
        self.init_cases()

    def init_cases(self):
        # Generate a list of test cases.  We have two independent lists:
        # one containing input test records and the other containing
        # output test records.  Our goal is to find matching pairs and
        # generate the corresponding test cases.

        # The matching pairs of input and output data.
        pairs = []

        # List of available output records.  We need to copy it since
        # it is going to be modified.
        available_outputs = []
        if self.ext_output is not None:
            available_outputs = self.ext_output.tests[:]
        elif self.output is not None:
            available_outputs = self.output.tests[:]

        # For each input record, find the matching output record.
        for input in self.input.tests:
            case_class = input.case_class
            for idx, output in enumerate(available_outputs):
                if case_class.matches(input, output):
                    pairs.append((input, output))
                    del available_outputs[idx]
                    break
            else:
                pairs.append((input, None))

        # Initialize the test cases.
        for input, output in pairs:
            case_class = input.case_class
            case = case_class(self.routine, self.cases_state, input, output)
            self.cases.append(case)

    def get_suites(self):
        # Get a set of (this and) the nested suites.
        suites = set([self.input.id])
        for case in self.cases:
            suites |= case.get_suites()
        return suites

    def out_header(self):
        # Print the header:
        # ================ ... =
        #   {input.title}
        #   ({input.location})
        self.out_sep("=")
        self.out(self.input.title, indent=2)
        if self.input.location is not None:
            self.out("(%s)" % self.input.location, indent=2)

    def skipped(self):
        # Check if the suite should not be executed.

        # Check if the test case was explicitly disabled.
        if super(SuiteTestCase, self).skipped():
            return True

        # The suite is skipped when:
        # - the user specified an explicit list of the suites to run;
        # - and the suite is not one of them;
        # - and the suite does not contain any selected nested suite;
        # - and the suite is not nested in some selected suite.
        if not self.routine.suites:
            return False
        if self.state.with_all_suites:
            return False
        if self.input.id in self.routine.suites:
            self.cases_state.with_all_suites = True
            return False
        if self.get_suites() & set(self.routine.suites):
            return False
        return True

    def verify(self):
        # Run the suite.

        # Push the current state to the cases state.
        self.state.push(self.cases_state)
        # Check if the suite is disabled or if the user specified
        # the suites to run and this one is not among them.
        if self.skipped():
            return
        # Display the headers.
        self.out_header()
        # Run the nested test cases.
        for case in self.cases:
            case.verify()
            # Check if the user asked to halt the testing.
            if self.cases_state.is_exiting:
                break
        # Pull the statistical information from the cases state.
        self.state.pull(self.cases_state)

    def train(self):
        # Run the suite; update the test output if necessary.

        # Push the current state to the cases state.
        self.state.push(self.cases_state)
        # Check if the suite is disabled or if the user specified
        # the suites to run and this one is not among them.
        if self.skipped():
            return self.output
        # A dictionary containing the output (or `None`) generated by test
        # cases when it differs from the existing test output.
        new_output_by_case = {}
        # Display the header.
        self.out_header()
        # Run the nested tests.
        for case in self.cases:
            new_output = case.train()
            # Record modified output data.
            if new_output is not case.output:
                new_output_by_case[case] = new_output
            # Check if the user asked to halt the testing.
            if self.cases_state.is_exiting:
                break
        # Pull the statistical information from the cases state.
        self.state.pull(self.cases_state)
        # Generate a new output record.
        output = self.make_output(new_output_by_case)
        # The output is kept in a separate file.
        if self.input.output is not None:
            # If the output has been updated, ask the user if they want
            # to save it.
            if output is not self.ext_output:
                self.out_sep()
                reply = self.ask_save()
                if reply is DO_DISCARD:
                    # `self.output` may still be not ``None`` if the `output`
                    # field was recently added.  In that case, we don't want
                    # to delete the regular output data until it is saved
                    # to a separate file.
                    return self.output
                self.out("*** saving test output data to %r"
                         % self.input.output)
                self.routine.save_output(self.input.output, output)
            # Returning `None` since the output is saved to a separate file.
            return None
        return output

    def make_output(self, new_output_by_case):
        # Generate the output test data.

        # Here we update the list of output test records.  Note that the list
        # may contain some inactive output records.  These output records
        # do not correspond to any input records and thus have no respective
        # test case.  It may happen if the user removed or modified the input
        # data.  Since a test case may be only temporarily disabled, we never
        # remove inactive output records unless the `--purge` option is enabled.
        
        # The list of the output records.
        tests = []

        # Start with the original list of output records.
        if self.output is not None:
            tests = self.output.tests[:]
        if self.ext_output is not None:
            tests = self.ext_output.tests[:]

        # `--purge` is enabled, we don't have to keep inactive records,
        # so simply generate the list from scratch.
        if self.routine.purge and not self.state.is_exiting:
            tests = []
            for case in self.cases:
                output = case.output
                if case in new_output_by_case:
                    output = new_output_by_case[case]
                if output is not None:
                    tests.append(output)

        # Some test cases generated new output, so we need to update the list.
        elif new_output_by_case:

            # Here we take the original list of records and replace those
            # that have been updated.  We may also encounter a new output
            # record, which has no corresponding old record in the list.
            # For that new record, we need to find a position in the list.
            # We want the order of the output records to match the order
            # of their respective input records, so to ensure this, we
            # put any new record immediately after all other records processed
            # so far.

            # Position to put new records.
            next_idx = 0
            for case in self.cases:
                # The record has been added, removed or updated.
                if case in new_output_by_case:
                    new_output = new_output_by_case[case]
                    # The record is rarely entirely removed so we should almost
                    # never get ``None`` here.  If we do, do nothing.
                    if new_output is not None:
                        # This is an updated record: replace the old record
                        # and update the position for the following new
                        # records.
                        if case.output in tests:
                            idx = tests.index(case.output)
                            tests[idx] = new_output
                            if idx >= next_idx:
                                next_idx = idx+1
                        # This is a new record: place it to the designated
                        # position.
                        else:
                            tests.insert(next_idx, new_output)
                            next_idx += 1

                # The record has not been changed.
                else:
                    # Make sure any new record will go after this one.
                    if case.output in tests:
                        idx = tests.index(case.output)
                        if idx >= next_idx:
                            next_idx = idx+1

        # When there are no test output data, skip creating the output record.
        if not tests:
            return None

        # Now we need to check if the new output list coincides with the old
        # one, in which case we don't want to create a new output record.
        if self.input.output is not None:
            if self.ext_output is not None and self.ext_output.tests == tests:
                return self.ext_output
        else:
            if self.output is not None and self.output.tests == tests:
                return self.output

        # Generate and return new output data.
        output = super(SuiteTestCase, self).make_output(id=self.input.id,
                                                        tests=tests)
        return output


class QueryTestCase(RunAndCompareTestCase):
    """
    Performs an HTSQL query.
    """

    name = "query"
    hint = """execute an HTSQL query"""
    help = """
    This test case executes an HTSQL query.
    """

    class Input(TestData):
        fields = [
                Field('uri', StrVal(),
                      hint="""the HTSQL query"""),
                Field('method', ChoiceVal(['GET', 'POST']), 'GET',
                      hint="""the HTTP method (GET or POST)"""),
                Field('remote_user', StrVal(), None,
                      hint="""the HTTP remote user"""),
                Field('headers', MapVal(StrVal(), StrVal()), None,
                      hint="""the HTTP headers"""),
                Field('content_type', StrVal(), None,
                      hint="""the content type of HTTP POST data"""),
                Field('content_body', StrVal(), None,
                      hint="""the HTTP POST data"""),
                Field('expect', IntVal(), 200,
                      hint="""the HTTP status code to expect"""),
                Field('ignore', BoolVal(), False,
                      hint="""ignore the response body"""),
                Field('ignore_headers', BoolVal(), False,
                      hint="""ignore the response headers"""),
        ] + SkipTestCase.Input.fields

        def init_attributes(self):
            # Check that `content-type` and `content-body` are set only if
            # the HTTP method is `POST`.
            if self.method == 'GET':
                if self.content_type is not None:
                    raise ValueError("unexpected content-type parameter"
                                     " for a GET request")
                if self.content_body is not None:
                    raise ValueError("unexpected content-body parameter"
                                     " for a GET request")
            if self.method == 'POST':
                if self.content_body is None:
                    raise ValueError("no expected content-body parameter"
                                     " for a POST request")

    class Output(TestData):
        fields = [
                Field('uri', StrVal(),
                      hint="""the HTSQL query"""),
                Field('status', StrVal(),
                      hint="""the response status line"""),
                Field('headers', SeqVal(SeqVal(StrVal(), length=2)),
                      hint="""the response headers"""),
                Field('body', StrVal(),
                      hint="""the response body"""),
        ]

        def init_attributes(self):
            # Convert the list of two-element lists to a list of pairs.
            self.headers = [(key, value) for key, value in self.headers]

    def out_header(self):
        # Display the header:
        # ---------------- ... -
        #   {method} {uri}
        #   ({input.location})
        #   Remote-User: {remote_user}
        #   {header}: value
        #   ...
        #   Content-Type: {content_type}
        #
        #   {content_body}
        self.out_sep()
        self.out("%s %s" % (self.input.method, self.input.uri), indent=2)
        self.out("(%s)" % self.input.location, indent=2)
        if self.input.remote_user is not None:
            self.out("Remote-User: %s" % self.input.remote_user, indent=2)
        if self.input.headers:
            for key in sorted(self.input.headers):
                value = self.input.headers[key]
                self.out("%s: %s" % (key, value), indent=2)
        if self.input.content_type is not None:
            self.out("Content-Type: %s" % self.input.content_type, indent=2)
        self.out()
        if self.input.content_body:
            self.out_lines(self.input.content_body.splitlines(), indent=2)

    def differs(self, old_output, new_output):
        # Check if the actual output differs from the expected output.
        if old_output is None or new_output is None:
            return True
        if old_output.status != new_output.status:
            return True
        if not self.input.ignore_headers:
            if old_output.headers != new_output.headers:
                return True
        if not self.input.ignore:
            if old_output.body != new_output.body:
                return True
        return False

    def render(self, output):
        # Convert the output record to a list of lines.
        if output is None:
            return None
        lines = []
        lines.append(output.status)
        for header, value in output.headers:
            lines.append("%s: %s" % (header, value))
        lines.append("")
        lines.extend(output.body.splitlines())
        return lines

    def execute(self):
        # Execute the query; return the output.

        # Prepare the HTSQL application.
        app = self.state.app
        if app is None:
            return self.failed("*** no HTSQL application is defined")

        # Prepare and execute the query.
        request = Request.prepare(method=self.input.method,
                                  query=self.input.uri,
                                  remote_user=self.input.remote_user,
                                  content_type=self.input.content_type,
                                  content_body=self.input.content_body,
                                  extra_headers=self.input.headers)
        response = request.execute(app)

        # Check if the response is valid.
        if response.exc_info is not None:
            self.out_exception(response.exc_info)
            return self.out("*** an exception occured"
                            " while executing the query")
        if not response.complete():
            return self.out("*** the response is not complete")

        # Generate the output record.
        new_output = self.make_output(uri=self.input.uri,
                                      status=response.status,
                                      headers=response.headers,
                                      body=response.body)

        # Check if we get the expected status code (200, by default).
        # If not, display the response and discard the output.
        if not response.status.startswith(str(self.input.expect)):
            self.out_diff(self.output, new_output)
            return self.out("*** unexpected status code: %s"
                            % response.status)

        return new_output


class CtlTestCase(RunAndCompareTestCase):
    """
    Executes a script routine.
    """

    name = "ctl"
    hint = """execute a routine"""
    help = """
    This test case simulates a run of the HTSQL command-line application.
    """

    class Input(TestData):
        fields = [
                Field('ctl', SeqVal(StrVal()),
                      hint="""a list of command-line parameters"""),
                Field('stdin', StrVal(), '',
                      hint="""the content of the standard input"""),
                Field('expect', IntVal(), 0,
                      hint="""the exit code to expect"""),
                Field('ignore', BoolVal(), False,
                      hint="""ignore the exit code and the standard output"""),
        ] + SkipTestCase.Input.fields

    class Output(TestData):
        fields = [
                Field('ctl', SeqVal(StrVal()),
                      hint="""a list of command-line parameters"""),
                Field('stdout', StrVal(),
                      hint="""the content of the standard output"""),
                Field('exit', IntVal(),
                      hint="""the exit code"""),
        ]

    def out_header(self):
        # Display the header:
        # ---------------- ... -
        #   {EXECUTABLE} {ctl}
        #   ({input.location})
        self.out_sep()
        executable = os.path.basename(self.routine.executable)
        command_line = " ".join([executable.upper()]+self.input.ctl)
        self.out(command_line, indent=2)
        self.out("(%s)" % self.input.location, indent=2)

    def differs(self, old_output, new_output):
        # Check if the actual output differs from the expected output.
        if old_output is None or new_output is None:
            return True
        if not self.input.ignore:
            if old_output.exit != new_output.exit:
                return True
            if old_output.stdout != new_output.stdout:
                return True
        return False

    def render(self, output):
        # Convert the output to a list of lines.
        if output is None:
            return None
        return output.stdout.splitlines()

    def execute(self):
        # Run the routine; return the output

        # Prepare the standard streams and the script instance.
        stdout = StringIO.StringIO()
        stderr = stdout
        stdin = TermStringIO(self.input.stdin, stdout)
        command_line = [self.routine.executable]+self.input.ctl

        # The script class.
        ctl_class = self.routine.ctl.__class__

        # Initialize and execute the script; check for exceptions.
        try:
            ctl = ctl_class(stdin, stdout, stderr)
            exit = ctl.main(command_line)
        except:
            self.out_exception(sys.exc_info())
            return self.out("*** an exception occured"
                            " while running the application")

        # Normalize the exit code.
        if exit is None:
            exit = 0
        elif not isinstance(exit, int):
            stderr.write(str(exit))
            exit = 1

        # Generate a new output record.
        new_output = self.make_output(ctl=self.input.ctl,
                                      stdout=stdout.getvalue(),
                                      exit=exit)

        # Check if we get the expected exit code; if not, display
        # the content of stdout and discard the output record.
        if not self.input.ignore:
            if new_output.exit != self.input.expect:
                self.out_diff(self.output, new_output)
                return self.out("*** unexpected exit code: %s" % exit)

        return new_output


class Fork(object):
    """
    Keeps information on the started processes.

    Class attributes:

    `active_forks`
        The global list of active processes.

    `is_atexit_registered`
        Indicates whether an :func:`atexit.atexit` callable was registered.
        The callable is called when the script is about to finish and kills
        any remaining active processes.

    Attributes:

    `process` (an instance of :class:`subprocess.Popen`)
        The wrapped process.

    `temp_path` (a string)
        A directory containing two files: `input` and `output`, which
        keeps the content of the standard input and the standard output
        respectively.
    """

    active_forks = []
    is_atexit_registered = False

    @classmethod
    def start(cls, executable, arguments, input):
        """
        Starts a new process.

        `executable`
            The path to the executable.

        `arguments`
            The list of arguments (not including the executable).

        `input`
            The content of the standard input.

        Returns a new :class:`Fork` instance.
        """
        # Create a temporary directory with the files 'input' and 'output'.
        temp_path = tempfile.mkdtemp()
        stream = open("%s/input" % temp_path, 'wb')
        stream.write(input)
        stream.close()
        # Prepare the standard input and the standard output streams.
        stdin = open("%s/input" % temp_path, 'rb')
        stdout = open("%s/output" % temp_path, 'wb')
        # Start the process.
        try:
            process = subprocess.Popen([executable]+arguments,
                                       stdin=stdin,
                                       stdout=stdout,
                                       stderr=subprocess.STDOUT)
        except:
            shutil.rmtree(temp_path)
            raise
        # Return a new `Fork` instance.
        return cls(process, temp_path)

    @classmethod
    def atexit(cls):
        # Finalize any remaining active processes.
        for fork in cls.active_forks:
            fork.end()

    @classmethod
    def atexit_register(cls):
        # Register the `atexit` callable if not done already.
        if not cls.is_atexit_registered:
            atexit.register(cls.atexit)
            cls.is_atexit_registered = True

    def __init__(self, process, temp_path):
        # Sanity check on the arguments.
        assert isinstance(process, subprocess.Popen)
        assert isinstance(temp_path, str) and os.path.isdir(temp_path)

        self.process = process
        self.temp_path = temp_path

        # Save themselves in the global list of active processes.
        self.active_forks.append(self)
        # Register the `atexit` callback.
        self.atexit_register()

    def end(self):
        """
        Ends the process.

        Returns the content of the standard output.
        """
        # Terminate the process if it is still alive.
        if self.process.poll() is None:
            self.process.terminate()
            time.sleep(1.0)
        # Read the standard output.
        stream = open("%s/output" % self.temp_path, 'rb')
        output = stream.read()
        stream.close()
        # Remove the temporary directory.
        shutil.rmtree(self.temp_path)
        # Remove it from the list of active processes.
        self.active_forks.remove(self)
        return output


class StartCtlTestCase(SkipTestCase):
    """
    Starts a long-running routine.
    """

    name = "start-ctl"
    hint = """execute a long-running routine"""
    help = """
    This test case starts a long-running the HTSQL command-line
    application.  Use the `end-ctl` test case to finalize the application
    and check the output.
    """

    class Input(TestData):
        fields = [
                Field('start_ctl', SeqVal(StrVal()),
                      hint="""a list of command-line parameters"""),
                Field('stdin', StrVal(), '',
                      hint="""the content of the standard output"""),
                Field('sleep', UFloatVal(), 0,
                      hint="""sleep for the specified number of seconds"""),
        ] + SkipTestCase.Input.fields

    def verify(self):
        # Execute the test.

        # Check if the test case is skipped.
        if self.skipped():
            return

        # Check if an application with the same command-line parameters
        # has already been started.
        key = tuple(self.input.start_ctl)
        if key in self.state.forks:
            return self.fork("*** the application is already started")

        # Start and save the process.
        fork = Fork.start(self.routine.executable,
                          self.input.start_ctl,
                          self.input.stdin)
        self.state.forks[key] = fork


class EndCtlTestCase(RunAndCompareTestCase):
    """
    Terminates a long-running routine.
    """

    name = "end-ctl"
    hint = """terminate a long-running routine"""
    help = """
    This test case allows you to terminate a long-running routine started
    with `start-ctl`.
    """

    class Input(TestData):
        fields = [
                Field('end_ctl', SeqVal(StrVal()),
                      hint="""a list of command-line parameters"""),
                Field('ignore', BoolVal(), False,
                      hint="""ignore the exit code and the standard output"""),
        ] + SkipTestCase.Input.fields

    class Output(TestData):
        fields = [
                Field('end_ctl', SeqVal(StrVal()),
                      hint="""a list of command-line parameters"""),
                Field('stdout', StrVal(),
                      hint="""the standard output"""),
        ]

    def differs(self, old_output, new_output):
        # Check if the actual output differs from the expected output.
        if old_output is None or new_output is None:
            return True
        if not self.input.ignore:
            if old_output.stdout != new_output.stdout:
                return True
        return False

    def render(self, output):
        # Convert the output record to a list of lines.
        if output is None:
            return None
        return output.stdout.splitlines()

    def execute(self):
        # Execute the test case.

        # Find the active process with the same command-line artguments.
        key = tuple(self.input.end_ctl)
        if key not in self.state.forks:
            return self.out("*** the application has not been started")
        fork = self.state.forks.pop(key)

        # Terminate the process; get the standard output.
        stdout = fork.end()

        # Create and return the output record.
        new_output = self.make_output(end_ctl=self.input.end_ctl,
                                      stdout=stdout)
        return new_output


class PythonCodeTestCase(RunAndCompareTestCase):
    """
    Executes arbitrary Python code.
    """

    name = "python"
    hint = """execute Python code"""
    help = """
    This test case allows you to execute arbitrary Python code.
    """

    class Input(TestData):
        fields = [
                Field('py', WordVal(),
                      hint="""the code name"""),
                Field('code', StrVal(),
                      hint="""Python code"""),
                Field('stdin', StrVal(), '',
                      hint="""the content of the standard input"""),
                Field('expect', StrVal(), None,
                      hint="""the name of an exception to expect"""),
                Field('ignore', BoolVal(), False,
                      hint="""ignore the standard output"""),
        ] + SkipTestCase.Input.fields

    class Output(TestData):
        fields = [
                Field('py', WordVal(),
                      hint="""the code name"""),
                Field('stdout', StrVal(),
                      hint="""the content of the standard output"""),
        ]

    def differs(self, old_output, new_output):
        # Check if the actual output differs from the expected output.
        if old_output is None or new_output is None:
            return True
        if not self.input.ignore:
            if old_output.stdout != new_output.stdout:
                return True
        return False

    def render(self, output):
        # Convert the output record to a list of lines.
        if output is None:
            return None
        return output.stdout.splitlines()

    def execute(self):
        # Execute the test case.

        # Prepare new standard streams.
        old_stdin = sys.stdin
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.stdin = StringIO.StringIO(self.input.stdin)
        sys.stdout = StringIO.StringIO()
        sys.stderr = sys.stdout
        # Prepare the code.
        code = self.load()
        context = {'state': self.state}
        # Execute the code.
        exc_info = None
        try:
            exec code in context
        except:
            exc_info = sys.exc_info()
        # Make new output record.
        key = self.input.fields[0].attribute
        new_output = self.make_output(stdout=sys.stdout.getvalue(),
                                      **{key: getattr(self.input, key)})
        # Restore old standard streams.
        sys.stdin = old_stdin
        sys.stdout = old_stdout
        sys.stderr = old_stderr
        # An exception occured while running the code.
        if exc_info is not None:
            # Display the output and the exception
            self.out_diff(self.output, new_output)
            self.out_exception(exc_info)
            exc_name = exc_info[0].__name__
            # The exception was unexpected: discard the output.
            if self.input.expect is None or self.input.expect != exc_name:
                return self.out("*** an unexpected exception occured")
        else:
            # We didn't get the expected exception: discard the output.
            if self.input.expect is not None:
                return self.out("*** an expected exception did not occur")
        return new_output

    def load(self):
        # Get the script source code.
        return self.input.code


class PythonCodeIncludeTestCase(PythonCodeTestCase):
    """
    Executes arbitrary Python code loaded from a file.
    """

    name = "python-include"
    hint = """load and execute Python code"""
    help = """
    This test case allows you to execute arbitrary Python code
    loaded from a file.
    """

    class Input(TestData):
        fields = [
                Field('py_include', StrVal(),
                      hint="""the file containing Python code"""),
                Field('stdin', StrVal(), '',
                      hint="""the content of the standard input"""),
                Field('expect', StrVal(), None,
                      hint="""the name of an exception to expect"""),
                Field('ignore', BoolVal(), False,
                      hint="""ignore the standard output"""),
        ] + SkipTestCase.Input.fields

    class Output(TestData):
        fields = [
                Field('py_include', StrVal(),
                      hint="""the file containing Python code"""),
                Field('stdout', StrVal(),
                      hint="""the content of the standard output"""),
        ]

    def load(self):
        # Get the script code from the given file
        stream = open(self.input.py_include, 'rb')
        code = stream.read()
        stream.close()
        return code


class SQLTestCase(SkipTestCase):
    """
    Executes a SQL query.
    """

    name = "sql"
    hint = """execute a SQL statement"""
    help = """
    This test case executes one or multiple SQL statements.
    """

    class Input(TestData):
        fields = [
                Field('connect', DBVal(),
                      hint="""the connection URI"""),
                Field('sql', StrVal(),
                      hint="""the statements to execute"""),
                Field('autocommit', BoolVal(), False,
                      hint="""use the auto-commit mode"""),
                Field('ignore', BoolVal(), False,
                      hint="""ignore any errors"""),
        ] + SkipTestCase.Input.fields

    def out_header(self):
        # Print:
        # ---------------- ... -
        #   {first line of input.sql}
        #   ({input.location})
        self.out_sep()
        first_line = self.input.sql.split('\n', 1)[0]
        self.out(first_line, indent=2)
        if self.input.location is not None:
            self.out("(%s)" % self.input.location, indent=2)

    def verify(self):
        # Check if the test is skipped.
        if self.skipped():
            return

        # Display the header.
        self.out_header()

        # Load the SQL input data.
        sql = self.load()

        # Generate an HTSQL application.  We need an application instance
        # to split the SQL data and to connect to the database, but we
        # never use it for executing HTSQL queries.
        from htsql import HTSQL
        from htsql.core.connect import connect, DBError
        from htsql.core.split_sql import split_sql
        try:
            app = HTSQL(self.input.connect)
        except Exception, exc:
            self.out_exception(sys.exc_info())
            return self.failed("*** an exception occured while"
                               " initializing an HTSQL application")

        # Activate the application so that we could use the splitter
        # and the connection adapters.
        with app:
            # Realize a splitter and split the input data to individual
            # SQL statements.
            try:
                statements = list(split_sql(sql))
            except ValueError, exc:
                return self.failed("*** invalid SQL: %s" % exc)

            # Realize the connector and connect to the database.
            try:
                connection = connect(with_autocommit=self.input.autocommit)
                cursor = connection.cursor()
            except DBError, exc:
                return self.failed("*** failed to connect to the database:"
                                   " %s" % exc)

            # Execute the given SQL statements.
            for statement in statements:
                try:
                    # Execute the statement in the current connection.
                    cursor.execute(statement)
                except DBError, exc:
                    # Display the statement that caused a problem.
                    for line in statement.splitlines():
                        self.out(line, indent=4)
                    # Normally, we end the test case when an error occurs,
                    # but if `ignore` is set, we just break the loop.
                    if not self.input.ignore:
                        return self.failed("*** failed to execute SQL:"
                                           " %s" % exc)
                    break

            # No error occurred while executing the SQL statements.
            else:
                # Commit the transaction unless `autocommit` mode is set.
                # Again, respect the `ignore` flag.
                if not self.input.autocommit:
                    try:
                        connection.commit()
                    except DBError, exc:
                        if not self.input.ignore:
                            return self.failed("*** failed to commit"
                                               " a transaction: %s" % exc)

            # Close the connection.  Note that we insist that connection
            # is opened and closed successfully regardless of the value
            # of the `ignore` flag.
            try:
                connection.close()
            except DBError, exc:
                return self.failed("*** failed to close the connection:"
                                   " %s" % exc)

        # If we reached that far, we passed the test.
        return self.passed()

    def load(self):
        """
        Returns the SQL data to execute.
        """
        # Override when subclassing.
        return self.input.sql


class SQLIncludeTestCase(SQLTestCase):
    """
    Loads SQL queries from a file and executes them.
    """

    name = "sql-include"
    hint = """load and execute SQL statements"""
    help = """
    This test case loads SQL statements from a file and execute them.
    """

    class Input(TestData):
        fields = [
                Field('connect', DBVal(),
                      hint="""the connection URI"""),
                Field('sql_include', StrVal(),
                      hint="""the file containing SQL statements"""),
                Field('autocommit', BoolVal(), False,
                      hint="""use the auto-commit mode"""),
                Field('ignore', BoolVal(), False,
                      hint="""ignore any errors"""),
        ] + SkipTestCase.Input.fields

    def out_header(self):
        # Print:
        # ---------------- ... -
        #   SQL-INCLUDE {input.sql_include}
        #   ({input.location})
        self.out_sep()
        self.out("%s %s" % (self.name.upper(), self.input.sql_include),
                 indent=2)
        if self.input.location is not None:
            self.out("(%s)" % self.input.location, indent=2)

    def load(self):
        # Load SQL from the given file.
        stream = open(self.input.sql_include, 'rb')
        sql = stream.read()
        stream.close()
        return sql


class WriteToFileTestCase(SkipTestCase):
    """
    Writes some data to a file.
    """

    name = "write-to-file"
    hint = """write some data to a file"""
    help = None

    class Input(TestData):
        fields = [
                Field('write', StrVal(),
                      hint="""the file name"""),
                Field('data', StrVal(),
                      hint="""the data to write"""),
        ] + SkipTestCase.Input.fields

    def verify(self):
        # Check if the test is skipped.
        if self.skipped():
            return
        # Display the header.
        self.out_header()
        # Write the data to the file.
        stream = open(self.input.write, 'wb')
        stream.write(self.input.data)
        stream.close()


class ReadFromFileTestCase(RunAndCompareTestCase):
    """
    Reads the file content.
    """

    name = "read-from-file"
    hint = """read the content of a file"""
    help = None

    class Input(TestData):
        fields = [
                Field('read', StrVal(),
                      hint="""the file name"""),
        ] + SkipTestCase.Input.fields

    class Output(TestData):
        fields = [
                Field('read', StrVal(),
                      hint="""the file name"""),
                Field('data', StrVal(),
                      hint="""the content of the file"""),
        ]

    def differs(self, old_output, new_output):
        # Check if the actual output differs from the expected output.
        if old_output is None or new_output is None:
            return True
        return (old_output.data != new_output.data)

    def render(self, output):
        # Convert the output record to a list of lines.
        if output is None:
            return None
        return output.data.splitlines()

    def execute(self):
        # Execute the test.

        # Check if the file exists.
        if not os.path.exists(self.input.read):
            return self.out("*** file %r does not exist" % self.input.read)

        # Read the data and create the output record.
        stream = open(self.input.read, 'rb')
        data = stream.read()
        stream.close()
        new_output = self.make_output(read=self.input.read, data=data)
        return new_output


class RemoveFilesTestCase(SkipTestCase):
    """
    Removes the specified files.
    """

    name = "remove-files"
    hint = """remove the specified files"""
    help = """
    Remove a list of files.  It is not an error if some of the files do not
    exist.
    """

    class Input(TestData):
        fields = [
                Field('remove', SeqVal(StrVal()),
                      hint="""a list of files to remove"""),
        ] + SkipTestCase.Input.fields

    def verify(self):
        # Check if the test is skipped.
        if self.skipped():
            return
        # Display the header.
        self.out_header()
        # Remove the given files.
        for path in self.input.remove:
            if os.path.exists(path):
                os.unlink(path)


class MakeDirTestCase(SkipTestCase):
    """
    Creates a directory.
    """

    name = "make-dir"
    hint = """create a directory"""
    help = """
    Create a directory.  If necessary, all intermediate directories are also
    created.
    """

    class Input(TestData):
        fields = [
                Field('mkdir', StrVal(),
                      hint="""the directory name"""),
        ] + SkipTestCase.Input.fields

    def verify(self):
        # Check if the test is skipped.
        if self.skipped():
            return
        # Display the header.
        self.out_header()
        # Create the directory if it does not already exist.
        if not os.path.isdir(self.input.mkdir):
            os.makedirs(self.input.mkdir)


class RemoveDirTestCase(SkipTestCase):
    """
    Removes a directory.
    """

    name = "remove-dir"
    hint = """remove a directory"""
    help = """
    Removes a directory with all its content.  It is not an error if the
    directory does not exist.
    """

    class Input(TestData):
        fields = [
                Field('rmdir', StrVal(),
                      hint="""the directory name"""),
        ] + SkipTestCase.Input.fields

    def verify(self):
        # Check if the test is skipped.
        if self.skipped():
            return
        # Display the header.
        self.out_header()
        # Remove the directory with all its content (DANGEROUS!).
        if os.path.exists(self.input.rmdir):
            shutil.rmtree(self.input.rmdir)


class TestState(object):
    """
    Keeps the mutable state of the testing process.

    `app`
        The current HTSQL application.

    `forks`
        A mapping from command-line parameters to :class:`Fork`
        instances; contains long-running applications.

    `toggles`
        A set of active named toggles.

    `saves`
        A mapping of named application configurations.

    `with_all_suites`
        Indicates that the current suite or one of its ancestors
        was explicitly selected by the user.

    `passed`
        The current number of passed tests.

    `failed`
        The current number of failed tests.

    `updated`
        The current number of updated tests.

    `is_exiting`
        Indicates whether the user asked to halt the testing.
    """

    def __init__(self, app=None, forks=None, toggles=None, saves=None,
                 with_all_suites=False, passed=0, failed=0, updated=0,
                 is_exiting=False):
        self.app = app
        self.forks = forks or {}
        self.toggles = toggles or set()
        self.saves = saves or {}
        self.with_all_suites = with_all_suites
        self.passed = passed
        self.failed = failed
        self.updated = updated
        self.is_exiting = is_exiting

    def push(self, other):
        """
        Push the state data to a derived state.

        `other` (:class:`TestState`)
            A derived state, the state created by a suite for
            the suite test cases.
        """
        other.app = self.app
        other.forks = self.forks.copy()
        other.toggles = self.toggles.copy()
        other.saves = self.saves.copy()
        other.with_all_suites = self.with_all_suites
        other.passed = self.passed
        other.failed = self.failed
        other.updated = self.updated
        other.is_exiting = self.is_exiting

    def pull(self, other):
        """
        Pull the state from a derived state.

        Note that only statistical information is pulled from
        the derived state.

        `other` (:class:`TestState`)
            A derived state, the state created by a suite for
            the suite test cases.
        """
        self.passed = other.passed
        self.failed = other.failed
        self.updated = other.updated
        self.is_exiting = other.is_exiting


# The base classes for the YAML loaders and dumpers.  When available,
# use the fast, LibYAML-based variants, if not, use the slow pure-Python
# versions.
BaseYAMLLoader = yaml.SafeLoader
if hasattr(yaml, 'CSafeLoader'):
    BaseYAMLLoader = yaml.CSafeLoader
BaseYAMLDumper = yaml.SafeDumper
if hasattr(yaml, 'CSafeDumper'):
    BaseYAMLDumper = yaml.CSafeDumper


class RegressYAMLLoader(BaseYAMLLoader):
    """
    Loads test data from a YAML file.

    `routine` (:class:`RegressRoutine`)
        The testing engine.

    `with_input` (Boolean)
        Indicates that the YAML file contains input records.

    `with_output` (Boolean)
        Indicates that the YAML file contains output records.

    `stream` (a file or a file-like object)
        The YAML stream.
    """

    # A pattern to match substitution variables in `!environ` nodes.
    environ_pattern = r"""
        \$ \{
            (?P<name> [a-zA-Z_][0-9a-zA-Z_.-]*)
            (?: : (?P<default> [0-9A-Za-z~@#^&*_;:,./?=+-]*) )?
        \}
    """
    environ_regexp = re.compile(environ_pattern, re.X)
    # A pattern for valid values of substitution variables.
    environ_value_pattern = r"""^ [0-9A-Za-z~@#^&*_;:,./?=+-]* $"""
    environ_value_regexp = re.compile(environ_value_pattern, re.X)


    def __init__(self, routine, with_input, with_output, stream):
        super(RegressYAMLLoader, self).__init__(stream)
        self.routine = routine
        # The list of permitted record classes.
        self.records = []
        # A mapping of record_class -> case_class.
        self.case_by_record = {}
        # A mapping of record_class -> the set of all attributes.
        self.all_keys_by_record = {}
        # A mapping of record_class -> the set of mandatory attributes.
        self.mandatory_keys_by_record = {}
        # Generate a list of permitted record classes.
        self.init_records(with_input, with_output)

    def init_records(self, with_input, with_output):
        # Gather the record classes from the available test cases.
        for case_class in self.routine.cases:
            if with_input and case_class.Input is not None:
                self.records.append(case_class.Input)
                self.case_by_record[case_class.Input] = case_class
            if with_output and case_class.Output is not None:
                self.records.append(case_class.Output)
                self.case_by_record[case_class.Output] = case_class

        # For each record class, prepare the set of all attributes and
        # the set of mandatory attributes.
        for record_class in self.records:
            all_keys = set()
            for field in record_class.fields:
                if field.is_any:
                    all_keys = None
                    break
                all_keys.add(field.attribute.replace('_', '-'))
            self.all_keys_by_record[record_class] = all_keys
            mandatory_keys = set()
            for field in record_class.fields:
                if field.is_any or not field.is_mandatory:
                    continue
                mandatory_keys.add(field.attribute.replace('_', '-'))
            if not mandatory_keys:
                mandatory_keys = None
            self.mandatory_keys_by_record[record_class] = mandatory_keys

    def load(self):
        """
        Loads test data from the YAML stream.
        """
        # That ensures the stream contains one document, parses it and
        # returns the corresponding object.
        return self.get_single_data()

    def construct_document(self, node):
        # We override this to ensure that any produced document is
        # a test record of expected type.
        data = super(RegressYAMLLoader, self).construct_document(node)
        if type(data) not in self.records:
            raise yaml.constructor.ConstructorError(None, None,
                    "unexpected document type",
                    node.start_mark)
        return data

    def construct_yaml_str(self, node):
        # Always convert a `!!str` scalar node to a byte string.
        # By default, PyYAML converts an `!!str`` node containing non-ASCII
        # characters to a Unicode string.
        value = self.construct_scalar(node)
        value = value.encode('utf-8')
        return value

    def construct_yaml_map(self, node):
        # Detect if a node represent test data and convert it to a test record.

        # We assume that the node represents a test record if it contains
        # all mandatory keys of the record class.  Otherwise, we assume it
        # is a regular dictionary.
        #
        # It would be much better to perform this detection on the tag
        # resolution phase.  However this phase does not give us access
        # to the mapping keys, so we have no choice but do it during the
        # construction phase.

        # Check if we got a mapping node.
        if not isinstance(node, yaml.MappingNode):
            raise yaml.constructor.ConstructorError(None, None,
                    "expected a mapping node, but found %s" % node.id,
                    node.start_mark)

        # Objects corresponding to the key nodes.
        keys = []
        # Objects corresponding to the value nodes.
        values = []
        # The mapping of key object -> value object.
        value_by_key = {}
        # The mapping of key object -> the mark of the key node.
        key_mark_by_key = {}
        # The mapping of key object -> the mark of the value node.
        value_mark_by_key = {}

        # Convert the key and the value nodes.
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=True)
            try:
                hash(key)
            except TypeError, exc:
                raise yaml.constructor.ConstructorError(
                        "while constructing a mapping",
                        node.start_mark,
                        "found unacceptable key (%s)" % exc,
                        key_node.start_mark)
            keys.append(key)
            value = self.construct_object(value_node, deep=True)
            values.append(value)
            value_by_key[key] = value
            key_mark_by_key[key] = key_node.start_mark
            value_mark_by_key[key] = value_node.start_mark

        # Find a record class such that the node contains all
        # the mandatory record fields.
        detected_record_class = None
        key_set = set(keys)
        for record_class in self.records:
            mandatory_keys = self.mandatory_keys_by_record[record_class]
            if mandatory_keys is None:
                continue
            if key_set.issuperset(mandatory_keys):
                detected_record_class = record_class
                break

        # If we can't find a suitable record class, it must be a regular
        # dictionary.
        if detected_record_class is None:
            return dict(zip(keys, values))

        # Check that the node does not contain any keys other than
        # the record fields.
        all_keys = self.all_keys_by_record[detected_record_class]
        if all_keys is not None:
            for key in keys:
                if key not in all_keys:
                    raise yaml.constructor.ConstructorError(None, None,
                            "unexpected key %r; expected one of %s"
                            % (key, ", ".join(sorted(all_keys))),
                            key_mark_by_key[key])

        # Generate the record attributes: validate and normalize
        # the field values.
        attributes = {}
        for field in detected_record_class.fields:
            if field.is_any:
                continue
            key = field.attribute.replace('_', '-')
            if key in value_by_key:
                value = value_by_key[key]
                try:
                    value = field.val(value)
                except ValueError, exc:
                    raise yaml.constructor.ConstructorError(None, None,
                            "invalid field %r (%s)" % (key, exc),
                            value_mark_by_key[key])
            else:
                value = field.default
            attributes[field.attribute] = value

        # Record where the node was found.
        location = "\"%s\", line %s" \
                   % (node.start_mark.name, node.start_mark.line+1)

        # Instantiate and return the test record.
        case_class = self.case_by_record[detected_record_class]
        try:
            record = detected_record_class(self.routine, case_class,
                                           attributes, location)
        except ValueError, exc:
            raise yaml.constructor.ConstructorError(None, None,
                    "invalid test data (%s)" % exc,
                    node.start_mark)
        return record

    def construct_environ(self, node):
        # Substitute environment variables in `!environ` scalars.

        def replace(match):
            # Substitute environment variables with values.
            name = match.group('name')
            default = match.group('default') or ''
            value = os.environ.get(name, default)
            if not self.environ_value_regexp.match(value):
                raise yaml.constructor.ConstructorError(None, None,
                        "invalid value of environment variable %s: %r"
                        % (name, value), node.start_mark)
            return value

        # Get the scalar value and replace all ${...} occurences with
        # values of respective environment variables.
        value = self.construct_scalar(node)
        value = value.encode('utf-8')
        value = self.environ_regexp.sub(replace, value)

        # Blank values are returned as `None`.
        if not value:
            return None
        return value


# Register custom constructors for `!!str``, `!!map`` and ``!environ``.
RegressYAMLLoader.add_constructor(
        u'tag:yaml.org,2002:str',
        RegressYAMLLoader.construct_yaml_str)
RegressYAMLLoader.add_constructor(
        u'tag:yaml.org,2002:map',
        RegressYAMLLoader.construct_yaml_map)
RegressYAMLLoader.add_constructor(
        u'!environ',
        RegressYAMLLoader.construct_environ)


# Register a resolver for ``!environ``.
RegressYAMLLoader.add_implicit_resolver(
        u'!environ', RegressYAMLLoader.environ_regexp, [u'$'])


class RegressYAMLDumper(BaseYAMLDumper):
    """
    Dumps test data to a YAML file.

    `routine` (:class:`RegressRoutine`)
        The testing engine.

    `with_input` (Boolean)
        Indicates that the YAML file will contain input records.

    `with_output` (Boolean)
        Indicates that the YAML file will contain output records.

    `stream` (a file or a file-like object)
        The stream where the YAML document is written.
    """

    def __init__(self, routine, with_input, with_output, stream, **keywords):
        # FIXME: we don't really need extra `with_*` parameters, this
        # constructor is always called with with_input=False, with_output=True.
        super(RegressYAMLDumper, self).__init__(stream, **keywords)
        self.routine = routine
        # The set of permitted record classes.
        self.records = set()
        # Gather the permitted record classes.
        self.init_records(with_input, with_output)
        # Check if the PyYAML version is suitable for dumping.
        self.check_version()

    def init_records(self, with_input, with_output):
        # Gather permitted record classes.
        for case_class in self.routine.cases:
            if with_input and case_class.Input is not None:
                self.records.add(case_class.Input)
            if with_output and case_class.Output is not None:
                self.records.add(case_class.Output)

    def check_version(self):
        # We require PyYAML >= 3.07 built with LibYAML >= 0.1.2 to dump
        # YAML data.  Other versions may produce slightly different output.
        # Since the YAML files may be kept in a VCS repository, we don't
        # want minor formatting changes generate unnecessarily large diffs.
        try:
            pyyaml_version = yaml.__version__
        except AttributeError:
            pyyaml_version = '3.05'
        try:
            import _yaml
            libyaml_version = _yaml.get_version_string()
        except ImportError:
            libyaml_version = None
        if pyyaml_version < '3.07':
            raise ScriptError("PyYAML >= 3.07 is required"
                              " to dump test output")
        if libyaml_version is None:
            raise ScriptError("PyYAML built with LibYAML bindings"
                              " is required to dump test output")
        if libyaml_version < '0.1.2':
            raise ScriptError("LibYAML >= 0.1.2 is required"
                              " to dump test output")

    def dump(self, data):
        """
        Dumps the data to the YAML stream.
        """
        self.open()
        self.represent(data)
        self.close()

    def represent_str(self, data):
        # Serialize a string.  We override the default string serializer
        # to use the literal block style for multi-line strings.
        tag = None
        style = None
        if data.endswith('\n'):
            style = '|'
        try:
            data = data.decode('utf-8')
            tag = u'tag:yaml.org,2002:str'
        except UnicodeDecodeError:
            data = data.encode('base64')
            tag = u'tag:yaml.org,2002:binary'
            style = '|'
        return self.represent_scalar(tag, data, style=style)

    def represent_record(self, data):
        # Complain when given a record of unexpected type.
        if type(data) not in self.records:
            return super(RegressYAMLDumper, self).represent_undefined(data)
        # Extract the fields skipping those with the default value.
        mapping = []
        for field in data.fields:
            if field.is_any:
                continue
            name = field.attribute.replace('_', '-')
            value = getattr(data, field.attribute)
            if value == field.default:
                continue
            mapping.append((name, value))
        # Generate a mapping node.
        return self.represent_mapping(u'tag:yaml.org,2002:map', mapping,
                                      flow_style=False)


# Register custom representers for `str` and `TestData`.
RegressYAMLDumper.add_representer(
        str, RegressYAMLDumper.represent_str)
RegressYAMLDumper.add_multi_representer(
        TestData, RegressYAMLDumper.represent_record)


class RegressRoutine(Routine):
    """
    Implements the `regress` routine.
    """

    name = 'regress'
    aliases = ['test']
    arguments = [
            Argument('suites', SeqVal(WordVal()), None, is_list=True),
    ]
    options = [
            InputOption,
            TrainOption,
            PurgeOption,
            ForceOption,
            QuietOption,
    ]
    hint = """run regression tests"""
    help = """
    This routine runs a series of test cases.
    
    A test case takes input data and produces output data.  The test
    succeeds if it runs without errors and its output data coincides with
    the expected output.

    Input and output test data are stored in the YAML format.  Run
    '%(executable)s help regress <case>' to get the description of the
    format for a specific test type.

    Test cases are organized into suites.  A test suite is a special type of
    a test case that contains other test cases.

    By default, the routine executes all tests in the given YAML file.  To
    run only specific test suites, list their identifiers in the command
    line.

    Unless option `--force` is used, the testing process will halt on the
    first test failure.

    The routine reads the input data from the standard input stream.  Use
    option `--input FILE` to read the input data from a file instead.

    The routine supports training mode, in which it allows you to add
    expected output for new tests and updated expected output for existing
    tests.  Use option `--train` to run the routine in the training mode.

    When a test case is removed, the routine does not remove obsolete
    expected output records automatically.  Use option `--purge` to remove
    stale output records.

    By default, the routine prints the header of every executed tests.  Use
    option `--quiet` to print only errors and final statistics.
    """

    # This text is written to YAML files generated by the routine.
    output_help = """
    #
    # This file contains expected test output data for regression tests.
    # It was generated automatically by the `regress` routine.
    #
    """

    # List of supported types of test cases.
    cases = [
            AppTestCase,
            LoadAppTestCase,
            DefineTestCase,
            IncludeTestCase,
            SuiteTestCase,
            QueryTestCase,
            CtlTestCase,
            StartCtlTestCase,
            EndCtlTestCase,
            PythonCodeTestCase,
            PythonCodeIncludeTestCase,
            SQLTestCase,
            SQLIncludeTestCase,
            WriteToFileTestCase,
            ReadFromFileTestCase,
            RemoveFilesTestCase,
            MakeDirTestCase,
            RemoveDirTestCase,
    ]

    # Represents the mutable state of the testing process.
    state_class = TestState

    @classmethod
    def get_help(cls, **substitutes):
        """
        Returns a long description of the routine.
        """
        # Produce routine description of the form:
        # {help}
        # 
        # Test cases: (run ... for more help)
        #   {case.name} : {case.hint}
        #   ...
        lines = []
        help = super(RegressRoutine, cls).get_help(**substitutes)
        if help is not None:
            lines.append(help)
        if cls.cases:
            if lines:
                lines.append("")
            lines.append("Test cases:"
                         " (run '%(executable)s help regress <case>'"
                         " for more help)" % substitutes)
            for case_class in cls.cases:
                case_name = case_class.name
                case_hint = case_class.get_hint()
                if case_hint is not None:
                    lines.append("  %-24s : %s" % (case_name, case_hint))
                else:
                    lines.append("  %s" % case_name)
        return "\n".join(lines)

    @classmethod
    def get_feature(cls, name):
        """
        Finds the test case by name.
        """
        for case_class in cls.cases:
            if case_class.name == name:
                return case_class
        raise ScriptError("unknown test case %r" % name)

    def run(self):
        # Get the test input data.
        input = self.load_input(self.input)
        # Initialize the testing state.
        state = self.state_class()
        # Create a test case.
        case = input.case_class(self, state, input, None)

        # Check if all test suites specified by the user exist.
        if self.suites:
            available_suites = case.get_suites()
            for suite in self.suites:
                if suite not in available_suites:
                    raise ScriptError("unknown suite %r" % suite)

        # Start the testing in the selected mode.
        if self.train:
            case.train()
        else:
            case.verify()

        # Display the statistics.
        self.ctl.out("="*72)
        if state.passed:
            self.ctl.out("TESTS PASSED: %s" % state.passed)
        if state.failed:
            self.ctl.out("TESTS FAILED: %s" % state.failed)
        if state.updated:
            self.ctl.out("TESTS UPDATED: %s" % state.updated)
        self.ctl.out()

        # Produce a fatal error if at least one test failed.
        if state.failed:
            if state.failed == 1:
                message = "a test failed"
            else:
                message = "%s tests failed" % state.failed
            raise ScriptError(message)

    def load_input(self, path):
        # Load test input data from a file.  If `path` is `None`,
        # load from the standard input.
        assert isinstance(path, maybe(str))
        if path is not None:
            stream = open(path, 'rb')
        else:
            stream = self.ctl.stdin
        loader = RegressYAMLLoader(self, True, False, stream)
        try:
            input = loader.load()
        except yaml.YAMLError, exc:
            raise ScriptError("failed to load test input data: %s" % exc)
        return input

    def load_output(self, path):
        # Load test output data from a file.
        assert isinstance(path, str)
        stream = open(path, 'rb')
        loader = RegressYAMLLoader(self, False, True, stream)
        try:
            input = loader.load()
        except yaml.YAMLError, exc:
            raise ScriptError("failed to load test output data: %s" % exc)
        return input

    def save_output(self, path, output):
        # Serialize and write test output data to a file.
        assert isinstance(path, str)
        assert isinstance(output, TestData)
        stream = open(path, 'wb')
        if self.output_help is not None:
            self.ctl.out(trim_doc(self.output_help), file=stream)
            self.ctl.out(file=stream)
        dumper = RegressYAMLDumper(self, False, True, stream)
        try:
            dumper.dump(output)
        except yaml.YAMLError, exc:
            raise ScriptError("failed to write test output data: %s" % exc)


