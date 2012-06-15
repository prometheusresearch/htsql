#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql.ctl.shell`
======================

This module implements the `shell` routine.
"""


from .error import ScriptError
from .request import Request, DBRoutine
from ..core.util import listof, trim_doc
from ..core.model import (HomeNode, InvalidNode, InvalidArc, TableArc, 
                          ColumnArc, ChainArc, AmbiguousArc)
from ..core.classify import classify, normalize
import traceback
import StringIO
import mimetypes
import sys
import os, os.path
import re
import subprocess
import struct
try:
    import readline
except ImportError:
    readline = None
try:
    import termios
except:
    termios = None
try:
    import fcntl
except:
    fcntl = None


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

    @classmethod
    def complete(cls, routine, argument):
        return None

    def __init__(self, routine, argument):
        assert isinstance(routine, ShellRoutine)
        assert isinstance(argument, str)
        self.routine = routine
        self.ctl = routine.ctl
        self.state = routine.state
        self.argument = argument

    def execute(self):
        """
        Executes the command.

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

    @classmethod
    def complete(cls, routine, argument):
        if argument:
            return None
        return sorted(routine.command_by_name)

    def execute(self):
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

    def execute(self):
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

    def execute(self):
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

    @classmethod
    def complete(cls, routine, argument):
        if argument:
            return None
        return ['on', 'off']

    def execute(self):
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

    @classmethod
    def complete(cls, routine, argument):
        if argument:
            return None
        return ['on', 'off']

    def execute(self):
        if not self.argument:
            self.ctl.out("** expected 'on' or 'off'")
            return
        if self.argument not in ['on', 'off']:
            self.ctl.out("** expected 'on' or 'off'; got %r" % self.argument)
            return
        if self.argument == 'on':
            # `stdin` and `stdout` must come from a terminal.
            if not self.ctl.is_interactive:
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


class ScanState(object):

    def __init__(self):
        self.indicator = '/'
        self.identifiers = []
        self.stack = []

    def push(self, indicator, identifiers=None):
        self.stack.append((self.indicator, self.identifiers))
        self.indicator = indicator
        if identifiers is not None:
            self.identifiers = identifiers

    def drop(self, indicators):
        if self.indicator in indicators:
            self.indicator, self.identifiers = self.stack.pop()
            return True
        return False

    def drop_all(self, indicators=None):
        if self.indicator in indicators:
            while self.indicator in indicators:
                self.indicator, self.identifiers = self.stack.pop()
            return True
        return False

    def clone(self):
        copy = ScanState()
        copy.indicator = self.indicator
        copy.identifiers = self.identifiers
        copy.stack = self.stack[:]
        return copy


class NodeChain(object):

    def __init__(self, app):
        self.app = app
        self.nodes = []
        self.labels_by_node = {}
        self.names_by_node = {}

    def push(self, node):
        self.nodes.append(node)
        if node not in self.labels_by_node:
            with self.app:
                labels = classify(node)
            labels = [label for label in labels
                            if not isinstance(label.arc, InvalidArc)]
            names = dict((label.name, label) for label in labels)
            self.labels_by_node[node] = labels
            self.names_by_node[node] = names

    def drop(self):
        self.nodes.pop()

    def labels(self):
        if not self.nodes:
            return []
        return self.labels_by_node[self.nodes[-1]]

    def label(self, name):
        if not self.nodes:
            return None
        return self.names_by_node[self.nodes[-1]].get(name)

    def __nonzero__(self):
        return bool(self.nodes)

    def clone(self):
        copy = NodeChain(self.app)
        copy.nodes = self.nodes[:]
        copy.labels_by_node = self.labels_by_node
        copy.names_by_node = self.names_by_node
        return copy


class GetPostBaseCmd(Cmd):
    """
    Implements the common methods of `get` and `post` commands.
    """

    # The HTTP method implemented by the command.
    method = None

    pattern = r"""
    ~ | < | > | = | ! | & | \| | -> | \. | , | \? | \^ |
    / | \* | \+ | - | \( | \) | \{ | \} | := | : | \$ | @ |
    \[ | \] |
    ' (?: [^'] | '')* ' | \d+ [.eE]?|
    (?! \d) \w+
    """
    letter_pattern = r"""(?! \d) \w"""
    regexp = re.compile(pattern, re.X|re.U)
    letter_regexp = re.compile(letter_pattern, re.X|re.U)

    @classmethod
    def complete(cls, routine, argument):
        if routine.state.app is None:
            return
        argument = argument.decode('utf-8', 'replace')
        tokens = cls.regexp.findall(argument)
        tokens = [''] + [token.encode('utf-8') for token in tokens] + ['']
        state = ScanState()
        for idx in range(1, len(tokens)-1):
            token = tokens[idx]
            prev_token = tokens[idx-1]
            next_token = tokens[idx+1]
            if state.indicator == '[':
                if token == '[' or token == '(':
                    state.push('[', [])
                elif token == ']' or token == ')':
                    state.drop('[')
                continue
            if cls.letter_regexp.match(token) is not None:
                if not (prev_token == ':' or prev_token == '$'
                        or next_token == '('):
                    state.push('_', state.identifiers+[token])
            elif token == '.':
                pass
            elif token == '->' or token == '@':
                state.push('_', [])
            elif token == '?' or token == '^':
                state_copy = state.clone()
                state.drop_all('_')
                if not state.drop('?^'):
                    state = state_copy
                state.push(token)
            elif token == '(':
                state.push(token)
            elif token == ')':
                state_copy = state.clone()
                state.drop_all('_?^')
                if not state.drop('('):
                    state = state_copy
            elif token == '{':
                state_copy = state.clone()
                state.drop_all('_')
                if not state.drop('?^'):
                    state = state_copy
                state.push(token)
            elif token == '}':
                state_copy = state.clone()
                state.drop_all('_?^')
                if not state.drop('{'):
                    state = state_copy
            elif token == ':=':
                state.drop('_')
            elif token == ':':
                state.drop_all('_?^')
            elif token == '$':
                state.push('_', [])
            elif token == '[':
                state.push('[', [])
            else:
                state.drop_all('_')
        if state.indicator == '[':
            return []
        identifiers = state.identifiers
        chain = NodeChain(routine.state.app)
        chain.push(HomeNode())
        for identifier in identifiers:
            identifier = normalize(identifier.decode('utf-8'))
            chain_copy = chain.clone()
            while chain:
                label = chain.label(identifier)
                if label is not None:
                    break
                chain.drop()
            node = label.target if label is not None else InvalidNode()
            chain = chain_copy
            chain.push(node)
        labels = chain.labels()
        names = [label.name.encode('utf-8') for label in labels]
        return names

    def execute(self):
        # Check if the argument of the command looks like an HTSQL query.
        if not self.argument:
            self.ctl.out("** a query is expected")
            return
        if self.argument[0] != '/':
            self.ctl.out("** a query is expected; got %r" % self.argument)
            return

        # Prepare the WSGI `environ` for a GET request.
        if self.method == 'GET':
            request = Request.prepare('GET', query=self.argument,
                                      remote_user=self.state.remote_user)

        # Prepare the WSGI `environ` for a POST request.
        if self.method == 'POST':
            # Get the name of the file containing POST data of the request.
            if self.ctl.is_interactive:
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
            if self.ctl.is_interactive:
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
        response = request.execute(self.state.app)

        # Display the output using a pager when necessary.
        self.dump(response)

    def dump(self, response):
        # Display the response.

        # Check for exceptions and incomplete responses.
        if response.exc_info is not None:
            exc_type, exc_value, exc_traceback = response.exc_info
            traceback.print_exception(exc_type, exc_value, exc_traceback,
                                      file=self.ctl.stderr)
            return
        if not response.complete():
            self.ctl.out("** incomplete response")
            return

        # Determine the output dimensions.
        lines = response.body.splitlines()
        length = len(lines)
        if self.state.with_headers:
            length += len(response.headers)+2
        width = max(len(line.decode('utf-8', 'replace'))
                    for line in lines) if lines else 0

        # Check if the output fits the terminal screen.
        max_lines, max_columns = self.routine.get_screen_size()
        if (self.state.with_pager
                and (length >= max_lines or width >= max_columns)):
            # Pipe the response to the pager.
            stream = StringIO.StringIO()
            response.dump(stream, self.state.with_headers)
            output = stream.getvalue()
            process = subprocess.Popen(self.routine.pager.split(),
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


class RunCmd(GetPostBaseCmd):
    """
    Implements the `run` command.
    """

    name = 'run'
    signature = """run filename.htsql"""
    hint = """run an HTSQL query from a file"""
    help = """
    Type `run filename.htsql` to load and execute an HTSQL query from a file.

    The command reads an HTSQL query from the given file and executes it as
    a GET request.

    The output of the query is dumped to the console.  When the pager is
    enabled and the number of lines in the response body exceeds the height
    of the terminal, the output is displayed via the pager.  Use `pager off`
    to disable the pager.

    By default, the command does not dump the response status line and the
    headers.  To enable displaying the status line and the headers along
    with the response body, use `headers on`.
    """

    @classmethod
    def complete(cls, routine, argument):
        return None

    def execute(self):
        # Check if the argument is suppied and is a valid filename.
        if not self.argument:
            self.ctl.out("** a file name is expected")
            return
        if not os.path.isfile(self.argument):
            self.ctl.out("** file %r does not exist" % self.argument)
            return

        # Read the file, check if it looks like valid HTSQL.
        stream = open(self.argument)
        query = stream.read().strip()
        stream.close()
        if not query or query[0] != '/':
            self.ctl.out("** file %r does not contain a valid HTSQL query"
                         % self.argument)

        # Prepare and execute a WSGI request.
        request = Request.prepare('GET', query=query,
                                  remote_user=self.state.remote_user)
        response = request.execute(self.state.app)

        # Display the response using a pager when necessary.
        self.dump(response)


class ShellState(object):
    """
    Holds mutable shell parameters.

    `app`
        The current HTSQL application.

    `with_headers` (Boolean)
        Indicates whether to display the status line and the headers of
        an HTTP response.

    `remote_user` (a string or ``None``)
        The WSGI remote user.

    `with_pager` (Boolean)
        Indicates whether the pager is enabled.
    """

    def __init__(self, app=None, with_headers=False,
                 remote_user=None, with_pager=True,
                 completer=None, completer_delims=None,
                 completions=()):
        self.app = app
        self.with_headers = with_headers
        self.remote_user = remote_user
        self.with_pager = with_pager
        self.completer = completer
        self.completer_delims = completer_delims
        self.completions = completions

class VersionCmd(Cmd):
    """
    Implements the `version` command.
    """

    name = 'version'
    signature = """version"""
    hint = """print version and license information"""
    help = """
    Type `version` to list the current software version and
    license information.
    """

    def execute(self):
        self.ctl.out(self.ctl.get_legal())
        self.ctl.out()


class DescribeCmd(Cmd):
    """
    Implements the `describe` command.
    """

    name = 'describe'
    signature = """describe [table]"""
    hint = """list tables, or slots for a given table"""
    help = """
    Type `describe` to list all tables or `describe <table>` to list
    all columns and links for a given table.
    """

    @classmethod
    def complete(cls, routine, argument):
        if argument:
            return None
        with routine.state.app:
             return [label.name for label in classify(HomeNode())]

    def execute(self):
        with self.state.app:
             root_labels = classify(HomeNode())

        if not self.argument:
            #
            # Enumerate introspected tables if one isn't provided
            #
            if not(root_labels):
                 self.ctl.out("No tables introspected or configured.")
                 return
            self.ctl.out("Tables introspected for this database are:")
            for label in root_labels:
                if not isinstance(label.arc, TableArc):
                     continue
                if label.name == label.arc.target.table.name:
                    self.ctl.out("\t%s" % label.name)
                else:
                    self.ctl.out("\t%s (%s)" % (label.name,
                                                label.arc.target.table.name))
            self.ctl.out()
            return
        #
        # Dump table attributes if a specific table is requested.
        #
        slots = None
        with self.state.app:
            for label in root_labels:
                if label.name == self.argument:
                    slots = classify(label.arc.target)
        if slots is None:
            self.ctl.out("Unable to find table: %s" % self.argument)
            self.ctl.out()
            return

        max_width = 0
        for slot in slots:
            if isinstance(slot.arc, AmbiguousArc):
                continue
            if len(slot.name) > max_width:
                max_width = len(slot.name)
        if not max_width:
            self.ctl.out("Table `%s` has no slots." % self.argument)
            self.ctl.out()
            return

        self.ctl.out("Slots for `%s` are:" % self.argument)
        for slot in slots:
            name = slot.name.ljust(max_width)
            post = str(slot.arc.target)
            if isinstance(slot.arc, ChainArc):
                if slot.arc.is_contracting:
                    post = "SINGULAR(%s)" % post
                else:
                    post = "PLURAL(%s)" % post
            if isinstance(slot.arc, AmbiguousArc):
                continue
            self.ctl.out("\t %s %s" % (name, post))
        self.ctl.out()


class ShellRoutine(DBRoutine):
    """
    Implements the `shell` routine.
    """

    name = 'shell'
    aliases = ['sh']
    hint = """start an HTSQL shell"""
    help = """
    The routine starts an interactive HTSQL shell over the specified database.

    The DB argument specifies database connection parameters; must have the
    form:
    
        engine://username:password@host:port/database

    Here,
    
      - ENGINE is the type of the database server; supported values are
        `sqlite`, `pgsql`, `mysql`, `mssql` and `oracle`.
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
    commands, `help <command>` to describe a command.

    Type `exit` or Ctrl-D to exit the shell.
    """

    # List of supported shell commands.
    commands = [
            HelpCmd,
            VersionCmd,
            ExitCmd,
            UserCmd,
            DescribeCmd,
            HeadersCmd,
            PagerCmd,
            GetCmd,
            PostCmd,
            RunCmd,
    ]

    # A notice displayed when the shell is started.
    intro = """
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
    history_path = '~/.htsql/shell.history'

    # Potential pagers, used when $PAGER is not set.
    default_pager_paths = [
            '/bin/less -S',
            '/usr/bin/less -S',
            '/usr/local/bin/less -S',
            '/bin/more',
    ]

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
        # A mapping of command_class.name -> command_class
        self.command_by_name = {}
        # Path to the pager.
        self.pager = None
        # Populate `command_by_name`.
        self.init_commands()
        # Set the pager and the pager thresholds.
        self.init_pager()
        # The mutable shell state.
        self.state = ShellState(with_pager=(self.ctl.is_interactive and
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
        # Detect the pager.

        # Use the environment variable $PAGER; if not set, check for
        # some common pagers.
        if 'PAGER' in os.environ:
            self.pager = os.environ['PAGER']
        else:
            for path in self.default_pager_paths:
                # Ignore parameters if any.
                if os.path.exists(path.split()[0]):
                    self.pager = path
                    break

    def get_screen_size(self):
        # Determine the terminal dimensions.

        # No dimensions in a non-interactive environment.
        if not self.ctl.is_interactive:
            return 0, 0

        lines = None
        columns = None

        # Use TIOCGWINSZ ioctl call to determine the terminal dimensions.
        if (fcntl is not None and
                termios is not None and hasattr(termios, 'TIOCGWINSZ')):
            # struct winsize {
            #   unsigned short ws_row;
            #   unsigned short ws_col;
            #   unsigned short ws_xpixel;   /* unused */
            #   unsigned short ws_ypixel;   /* unused */
            # };
            winsize = struct.pack('HHHH', 0, 0, 0, 0)
            try:
                winsize = fcntl.ioctl(self.ctl.stdout,
                                      termios.TIOCGWINSZ, winsize)
                lines, columns = struct.unpack('HHHH', winsize)[:2]
            except IOError:
                pass

        # Try $LINES and $COLUMNS environment variables (usually not set).
        if lines is None:
            if 'LINES' in os.environ:
                try:
                    lines = int(os.environ['LINES'])
                except ValueError:
                    pass
        if columns is None:
            if 'COLUMNS' in os.environ:
                try:
                    columns = int(os.environ['COLUMNS'])
                except ValueError:
                    pass

        # The default values.
        if lines is None:
            lines = 25
        if columns is None:
            columns = 80

        return lines, columns

    def start(self, app):
        # Set the active HTSQL application.
        self.state.app = app

        # Display the welcome notice; load the history.
        self.setup()
        try:
            # Read and execute commands until instructed to exit.
            while self.loop() is None:
                pass
        finally:
            # Save the history.
            self.shutdown()

    def setup(self):
        # Load the `readline` history; initialize completion.
        if self.ctl.is_interactive and readline is not None:
            path = os.path.abspath(os.path.expanduser(self.history_path))
            if os.path.exists(path):
                readline.read_history_file(path)
            self.state.completer = readline.get_completer()
            self.state.completer_delims = readline.get_completer_delims()
            readline.set_completer(self.completer)
            readline.set_completer_delims(
                    " \t\n`~!@#$%^&*()-=+[{]}\\|;:\'\",<.>/?")
            readline.parse_and_bind("tab: complete")

        # Display the welcome notice.
        if self.ctl.is_interactive:
            intro = self.get_intro()
            if intro:
                self.ctl.out(intro)

    def shutdown(self):
        # Save the `readline` history; restore the original state of completion.
        if self.ctl.is_interactive and readline is not None:
            path = os.path.abspath(os.path.expanduser(self.history_path))
            directory = os.path.dirname(path)
            if not os.path.exists(directory):
                try:
                    os.mkdir(directory, 0700)
                except OSError:
                    return
            readline.write_history_file(path)
            readline.set_completer(self.state.completer)
            readline.set_completer_delims(self.state.completer_delims)

    def loop(self):
        # Display the prompt and read the command from the console.
        # On EOF, exit the loop.
        if self.ctl.is_interactive:
            prompt = "$ "
            app = self.state.app
            if app is not None and app.htsql.db is not None:
                # When the database is a file, strip the dirname and
                # the extension.
                database = os.path.basename(app.htsql.db.database)
                database = os.path.splitext(database)[0]
                prompt = "%s$ " % database
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
        return command.execute()

    def completer(self, text, index):
        try:
            if index == 0:
                line = readline.get_line_buffer()
                begidx = readline.get_begidx()
                endidx = readline.get_endidx()
                delta = len(line)
                line = line.lstrip()
                delta -= len(line)
                begidx -= delta
                endidx -= delta
                prefix = line[:begidx]
                if not prefix:
                    completions = sorted(self.command_by_name)
                else:
                    name = prefix.split()[0]
                    if self.command_name_pattern.match(name):
                        prefix = prefix[len(name):].strip()
                    else:
                        name = ''
                    if name in self.command_by_name:
                        command_class = self.command_by_name[name]
                        completions = command_class.complete(self, prefix)
                        if completions is None:
                            completions = []
                    else:
                        completions = []
                completions = [word for word in completions
                                    if word and word.startswith(text)]
                self.state.completions = tuple(completions)
            if index < len(self.state.completions):
                return self.state.completions[index]
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback,
                                      file=self.ctl.stderr)


