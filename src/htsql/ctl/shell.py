#
# Copyright (c) 2006-2008, Prometheus Research, LLC
# Authors: Kirill Simonov <xi@gamma.dn.ua>,
#          Clark C. Evans <cce@clarkevans.com>
#


"""
:mod:`htsql.ctl.shell`
======================

This module implements the `shell` routine.
"""


from .error import ScriptError
from .routine import Argument, Routine
from .option import ExtensionsOption
from .request import Request
from ..validator import DBVal
from ..util import listof, trim_doc
import traceback
import StringIO
import mimetypes
import sys
import os, os.path
import re
import subprocess
try:
    import readline
except ImportError:
    readline = None


class Cmd(object):
    """
    Describes a shell command.

    This is the base abtract class for all shell commands.  To create
    a concrete command, subclass :class:`Cmd`, declare the command
    name and other parameters, and override :meth:`execute`.

    The following class attributes could be overridden.

    `name` (a string)
        The name of the command.  Name equal to an empty string (``''``)
        means this command is executed when no explicit command name
        is provided.  The name must be unique across all commands.

    `aliases` (a list of strings)
        The list of alternative command names.

    `signature` (a string or ``None``)
        Declaration of the command name and the arguments.

    `hint` (a string or ``None``)
        A short one-line description of the command.

    `help` (a string or ``None``)
        A long description of the command.  Keep the line width
        at 72 characters.

    Instances of :class:`Cmd` have the following attributes.

    `routine` (:class:`ShellRoutine`)
        The routine that executed the command.

    `ctl` (:class:`htsql.ctl.script.Script`)
        The script that started the shell routine.

    `state` (:class:`ShellState`)
        The shell state.

    `argument` (a string)
        The argument of the command.
    """

    name = None
    aliases = []
    signature = None
    hint = None
    help = None

    @classmethod
    def get_signature(cls):
        """
        Returns an (informal) signature of the command.
        """
        if cls.signature is not None:
            return cls.signature
        return cls.name

    @classmethod
    def get_hint(cls):
        """
        Returns a short one-line description of the command.
        """
        return cls.hint

    @classmethod
    def get_help(cls):
        """
        Returns a long description of the command.
        """
        return trim_doc(cls.help)

    def __init__(self, routine, argument):
        assert isinstance(routine, ShellRoutine)
        assert isinstance(argument, str)
        self.routine = routine
        self.ctl = routine.ctl
        self.state = routine.state
        self.argument = argument

    def execute(self, app):
        """
        Executes the command.

        `app`
            A WSGI application.

        The normal return value is ``None``; any other value causes
        the shell to exit.
        """
        # Override in a subclass.
        self.ctl.out("** not implemented")


class HelpCmd(Cmd):
    """
    Implements the `help` command.
    """

    name = 'help'
    aliases = ['?']
    signature = """help [command]"""
    hint = """describe the shell or a shell command"""
    help = """
    Type `help` to learn how to use the shell and to get a list of available
    commands.

    Type `help <command>` to learn how to use the specified command.
    """

    def execute(self, app):
        # If called without arguments, describe the shell.
        # If called with an argument, assume it is the name of the command
        # to describe.
        if not self.argument:
            self.describe_routine()
        else:
            if self.argument not in self.routine.command_by_name:
                self.ctl.out("** unknown command %r" % self.argument)
                return
            command_class = self.routine.command_by_name[self.argument]
            self.describe_command(command_class)

    def describe_routine(self):
        # Display:
        # {usage}
        #
        # Available commands:
        #   {command} : {hint}
        #   ...
        #
        usage = self.routine.get_usage()
        if usage is not None:
            self.ctl.out(usage)
        if self.routine.commands:
            self.ctl.out()
            self.ctl.out("Available commands:")
            for command_class in self.routine.commands:
                signature = command_class.get_signature()
                hint = command_class.get_hint()
                self.ctl.out("  ", end="")
                if hint is not None:
                    self.ctl.out("%-24s : %s" % (signature, hint))
                else:
                    self.ctl.out(signature)
        self.ctl.out()

    def describe_command(self, command_class):
        # Display:
        # {NAME} - {hint}
        #
        # Usage: {signature}
        #
        # {help}
        #
        name = command_class.name
        hint = command_class.get_hint()
        if hint is not None:
            self.ctl.out(name.upper(), "-", hint)
        else:
            self.ctl.out(name.upper())
        signature = command_class.get_signature()
        self.ctl.out("Usage:", signature)
        help = command_class.get_help()
        if help is not None:
            self.ctl.out()
            self.ctl.out(help)
        self.ctl.out()


class ExitCmd(Cmd):
    """
    Implements the `exit` command.
    """

    name = 'exit'
    aliases = ['quit', 'q']
    hint = """quit the shell"""
    help = """
    Type `exit` or Ctrl-D to quit the shell.
    """

    def execute(self, app):
        # Returning any non-`None` value exits the shell.
        return True


class UserCmd(Cmd):
    """
    Implements the `user` command.
    """

    name = 'user'
    signature = """user [remote_user]"""
    hint = """set the remote user for HTTP requests"""
    help = """
    To set the remote user for HTTP requests, type `user <name>`.

    To unset the remote user for HTTP requests, type `user`.
    """

    def execute(self, app):
        # If set, `state.remote_user` is passed to the WSGI application
        # as `environ['REMOTE_USER']`.
        if self.argument:
            self.state.remote_user = self.argument
            self.ctl.out("** remote user is set to %r" % self.argument)
        else:
            self.state.remote_user = None
            self.ctl.out("** remote user is unset")


class HeadersCmd(Cmd):
    """
    Implements the `headers` command.
    """

    name = 'headers'
    signature = """headers on|off"""
    hint = """display HTTP status line and headers"""
    help = """
    Type `headers on` to enable output of HTTP status line and headers along
    with any HTTP response body.

    Type `headers off` to disable output of HTTP status line and headers.
    Only HTTP response body will be displayed.
    """

    def execute(self, app):
        # `state.with_headers` indicates whether or not to display
        # the status line and headers of the WSGI response.
        if not self.argument:
            self.ctl.out("** expected 'on' or 'off'")
            return
        if self.argument not in ['on', 'off']:
            self.ctl.out("** expected 'on' or 'off'; got %r" % self.argument)
            return
        if self.argument == 'on':
            self.state.with_headers = True
            self.ctl.out("** headers are turned on")
        if self.argument == 'off':
            self.state.with_headers = False
            self.ctl.out("** headers are turned off")


class PagerCmd(Cmd):
    """
    Implements the `pager` command.
    """

    name = 'pager'
    signature = """pager on|off"""
    hint = """pipe long output to a pager"""
    help = """
    Type `pager on` or `pager off` to enable or disable the pager
    respectively.

    If the pager is enabled, and the number of lines in the HTTP response
    exceeds the height of the terminal, the response is displayed via the
    pager.

    The pager is a command which allows you to scroll and search in the
    output.  The pager application is determined by the environment variable
    $PAGER.  When $PAGER is not set, one of the common pagers such as
    `/usr/bin/more` is used if available.  The pager could only be enabled
    when the shell is running in a terminal.
    """

    def execute(self, app):
        if not self.argument:
            self.ctl.out("** expected 'on' or 'off'")
            return
        if self.argument not in ['on', 'off']:
            self.ctl.out("** expected 'on' or 'off'; got %r" % self.argument)
            return
        if self.argument == 'on':
            # `stdin` and `stdout` must come from a terminal.
            if not self.routine.is_interactive:
                self.ctl.out("** pager cannot be enabled"
                             " in the non-interactive mode")
                return
            # The pager application must be present.
            if self.routine.pager is None:
                self.ctl.out("** no pager is found")
                return
            self.state.with_pager = True
            self.ctl.out("** pager %r is enabled" % self.routine.pager)
        if self.argument == 'off':
            self.state.with_pager = False
            self.ctl.out("** pager is disabled")


class GetPostBaseCmd(Cmd):
    """
    Implements the common methods of `get` and `post` commands.
    """

    # The HTTP method implemented by the command.
    method = None

    def execute(self, app):
        # Check if the argument of the command looks like an HTSQL query.
        if not self.argument:
            self.ctl.out("** a query is expected")
        if self.argument[0] != '/':
            self.ctl.out("** a query is expected; got %r" % self.argument)

        # Prepare the WSGI `environ` for a GET request.
        if self.method == 'GET':
            request = Request.prepare('GET', query=self.argument,
                                      remote_user=self.state.remote_user)

        # Prepare the WSGI `environ` for a POST request.
        if self.method == 'POST':
            # Get the name of the file containing POST data of the request.
            if self.routine.is_interactive:
                self.ctl.out("File with POST data:", end=" ")
            content_path = self.ctl.stdin.readline().strip()
            if not content_path:
                self.ctl.out("** a file name is expected")
                return
            if not os.path.exists(content_path):
                self.ctl.out("** file %r does not exist" % content_path)
                return
            content_body = open(content_path, 'rb').read()

            # Determine the content type of the POST data.
            default_content_type = mimetypes.guess_type(content_path)[0]
            if default_content_type is None:
                default_content_type = 'application/octet-stream'
            if self.routine.is_interactive:
                self.ctl.out("Content type [%s]:" % default_content_type,
                             end=" ")
            content_type = self.ctl.stdin.readline().strip()
            if not content_type:
                content_type = default_content_type

            request = Request.prepare('POST', query=self.argument,
                                      remote_user=self.state.remote_user,
                                      content_type=content_type,
                                      content_body=content_body)

        # Execute the WSGI request.
        response = request.execute(app)

        # Check for exceptions and incomplete responses.
        if response.exc_info is not None:
            exc_type, exc_value, exc_traceback = response.exc_info
            traceback.print_exception(exc_type, exc_value, exc_traceback,
                                      file=self.ctl.stderr)
            return
        if not response.complete():
            self.ctl.out("** incomplete response")
            return

        # Check if we need to use the pager.
        length = response.body.count('\n')
        if (self.state.with_pager
                and length > self.routine.pager_line_threshold):
            # Pipe the response to the pager.
            stream = StringIO.StringIO()
            response.dump(stream, self.state.with_headers)
            output = stream.getvalue()
            process = subprocess.Popen(self.routine.pager,
                                       stdin=subprocess.PIPE)
            try:
                process.communicate(output)
            except IOError, exc:
                self.ctl.out(exc)
        else:
            # Dump the response.
            response.dump(self.ctl.stdout, self.state.with_headers)


class GetCmd(GetPostBaseCmd):
    """
    Implements the `get` command.
    """

    name = 'get'
    aliases = ['']
    signature = """[get] /query"""
    hint = """execute an HTSQL query"""
    help = """
    Type `get /query` or just `/query` to execute an HTSQL query.

    The output of the query is dumped to the console.  When the pager is
    enabled and the number of lines in the response body exceeds the height
    of the terminal, the output is displayed via the pager.  Use `pager off`
    to disable the pager.

    By default, the command does not dump the response status line and the
    headers.  To enable displaying the status line and the headers along
    with the response body, use `headers on`.
    """
    method = 'GET'


class PostCmd(GetPostBaseCmd):
    """
    Implements the `post` command.
    """

    name = 'post'
    signature = """post /query"""
    hint = """execute an HTSQL query with POST data"""
    help = """
    Type `post /query` to execute an HTSQL query with POST data.

    You will be asked to provide a file containing the POST data and to
    indicate the content type of the data.

    The output of the query is dumped to the console.  When the pager is
    enabled and the number of lines in the response body exceeds the height
    of the terminal, the output is displayed via the pager.  Use `pager off`
    to disable the pager.

    By default, the command does not dump the response status line and the
    headers.  To enable displaying the status line and the headers along
    with the response body, use `headers on`.
    """
    method = 'POST'


class ShellState(object):
    """
    Holds mutable shell parameters.

    `with_headers` (Boolean)
        Indicates whether to display the status line and the headers of
        an HTTP response.

    `remote_user` (a string or ``None``)
        The WSGI remote user.

    `with_pager` (Boolean)
        Indicates whether the pager is enabled.
    """

    def __init__(self, with_headers=False,
                 remote_user=None, with_pager=True):
        self.with_headers = with_headers
        self.remote_user = remote_user
        self.with_pager = with_pager


class ShellRoutine(Routine):
    """
    Implements the `shell` routine.
    """

    name = 'shell'
    aliases = ['sh']
    arguments = [
            Argument('db', DBVal(),
                     hint="""the connection URI"""),
    ]
    options = [
            ExtensionsOption,
    ]
    hint = """start an HTSQL shell"""
    help = """
    The routine starts an interactive HTSQL shell over the specified database.

    The DB argument specifies database connection parameters; must have the
    form:
    
        engine://username:password@host:port/database

    Here,
    
      - ENGINE is the type of the database server; supported values are
        `pgsql` and `sqlite`.
      - The parameters USERNAME:PASSWORD are used for authentication.
      - The parameters HOST:PORT indicate the address of the database
        server.
      - DATABASE is the name of the database; for SQLite, the path to the
        database file.

    All parameters except ENGINE and DATABASE are optional.

    When the shell is started, you will see the shell prompt with the
    database name followed by `$`.  Type an HTSQL query and press ENTER to
    execute it.  Type a command with any arguments and press ENTER to
    execute the command.  In particular, use `help` to get a list of shell
    commands, `help <command>` to describe of a command.

    Type `exit` or Ctrl-D to exit the shell.
    """

    # List of supported shell commands.
    commands = [
            HelpCmd,
            ExitCmd,
            UserCmd,
            HeadersCmd,
            PagerCmd,
            GetCmd,
            PostCmd,
    ]

    # A notice displayed when the shell is started.
    intro = """
    Interactive HTSQL Shell
    Type 'help' for more information, 'exit' to quit the shell.
    """

    # The description of the shell displayed by the `help` command.
    usage = """
    To execute an HTSQL query, type

        /query

    To execute a shell command, type

        command [arguments...]
    """

    # Path to the file keeping the `readline` history.
    history_path = '~/.htsql_shell_history'

    # Potential pagers, used when $PAGER is not set.
    default_pager_paths = ['/usr/bin/pager', '/usr/bin/more']

    # The default value of the pager line threshold, used
    # when $LINES is not set
    default_pager_line_threshold = 25

    # The pattern to check for commands.
    command_name_pattern = re.compile(r'^[a-zA-Z.?]')

    @classmethod
    def get_intro(cls):
        """
        Returns a notice to display when the shell is started.
        """
        return trim_doc(cls.intro)

    @classmethod
    def get_usage(cls):
        """
        Returns the shell description used by the `help` command.
        """
        return trim_doc(cls.usage)

    @classmethod
    def get_help(cls, **substitutes):
        """
        Returns a long description of the routine.
        """
        # The description of the shell routine has the form:
        # {help}
        #
        # Shell commands: (run ... for more help)
        #   {command.signature} : {command.hint}
        #   ...
        help = super(ShellRoutine, cls).get_help(**substitutes)
        if cls.commands:
            lines = []
            lines.append("Shell commands:"
                         " (run '%(executable)s help shell <command>'"
                         " for more help)" % substitutes)
            for command_class in cls.commands:
                signature = command_class.get_signature()
                hint = command_class.get_hint()
                if hint is not None:
                    line = "  %-24s : %s" % (signature, hint)
                else:
                    line = "  %s" % signature
                lines.append(line)
            if help is not None:
                help = "%s\n\n%s" % (help, "\n".join(lines))
            else:
                help = "\n".join(lines)
        return help

    @classmethod
    def get_feature(cls, name):
        """
        Returns the shell command by name.
        """
        for command_class in cls.commands:
            if name == command_class.name or name in command_class.aliases:
                return command_class
        raise ScriptError("unknown shell command %r" % name)

    def __init__(self, ctl, attributes):
        super(ShellRoutine, self).__init__(ctl, attributes)
        self.is_interactive = (hasattr(ctl.stdin, 'isatty') and
                               ctl.stdin.isatty() and
                               ctl.stdin is sys.stdin and
                               hasattr(ctl.stdout, 'isatty') and
                               ctl.stdout.isatty() and
                               ctl.stdout is sys.stdout)
        # A mapping of command_class.name -> command_class
        self.command_by_name = {}
        # Path to the pager.
        self.pager = None
        # The pager will be activated when the number of lines in the response
        # exceeds this value.
        self.pager_line_threshold = self.default_pager_line_threshold
        # Populate `command_by_name`.
        self.init_commands()
        # Set `pager` and `pager_line_threshold`.
        self.init_pager()
        # The mutable shell state.
        self.state = ShellState(with_pager=(self.is_interactive and
                                            self.pager is not None))

    def init_commands(self):
        # Populate `command_by_name`; also, sanity check on the commands.
        for command_class in self.commands:
            assert issubclass(command_class, Cmd)
            assert isinstance(command_class.name, str)
            assert isinstance(command_class.aliases, listof(str))
            for name in [command_class.name]+command_class.aliases:
                assert name not in self.command_by_name, \
                       "duplicate command name: %r" % name
                self.command_by_name[name] = command_class

    def init_pager(self):
        # Initialize the attributes `pager` and `pager_line_threshold`.

        # Use the environment variable $PAGER; if not set, check for
        # some common pagers.
        if 'PAGER' in os.environ:
            self.pager = os.environ['PAGER']
        else:
            for path in self.default_pager_paths:
                if os.path.exists(path):
                    self.pager = path
                    break

        # $LINES indicates the number of lines in the terminal.
        if 'LINES' in os.environ:
            try:
                self.pager_line_threshold = int(os.environ['LINES'])
            except ValueError:
                pass

    def run(self):
        # Create the HTSQL application.
        from htsql.application import Application
        app = Application(self.db, *self.extensions)

        # Display the welcome notice; load the history.
        self.setup(app)
        try:
            # Read and execute commands until instructed to exit.
            while self.loop(app) is None:
                pass
        finally:
            # Save the history.
            self.shutdown(app)

    def setup(self, app):
        # Load the `readline` history.
        if self.is_interactive and readline is not None:
            path = os.path.abspath(os.path.expanduser(self.history_path))
            if os.path.exists(path):
                readline.read_history_file(path)

        # Display the welcome notice.
        if self.is_interactive:
            intro = self.get_intro()
            if intro:
                self.ctl.out(intro)

    def shutdown(self, app):
        # Save the `readline` history.
        if self.is_interactive and readline is not None:
            path = os.path.abspath(os.path.expanduser(self.history_path))
            readline.write_history_file(path)

    def loop(self, app):
        # Display the prompt and read the command from the console.
        # On EOF, exit the loop.
        if self.is_interactive:
            prompt = "%s$ " % self.db.database
            try:
                line = raw_input(prompt)
            except EOFError:
                self.ctl.out()
                return True
        else:
            line = self.ctl.stdin.readline()
            if not line:
                return True

        # Skip empty lines.
        line = line.strip()
        if not line:
            return

        # Determine the command name and the command argument.
        name = line.split()[0]
        if self.command_name_pattern.match(name):
            argument = line[len(name):].strip()
        else:
            name = ''
            argument = line

        # Complain if the command is not found.
        if name not in self.command_by_name:
            self.ctl.out("** unknown command %r" % name)
            return

        # Instantiate and execute the command.
        command_class = self.command_by_name[name]
        command = command_class(self, argument)
        return command.execute(app)


