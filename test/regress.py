#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from pbbt import Test, Field, BaseCase, MatchCase
from pbbt.check import choiceof, maybe, oneof, listof, tupleof, dictof
from pbbt.std import is_filename, to_identifier
import re
import io
import traceback
import tempfile
import shutil
import atexit
import subprocess
import os
import time


class TermStringIO(io.StringIO):

    def __init__(self, buf, output):
        io.StringIO.__init__(self, buf)
        self.output = output

    def read(self, n=-1):
        data = io.StringIO.read(self, n)
        self.output.write(data)
        return data

    def readline(self, length=None):
        data = io.StringIO.readline(self, length)
        self.output.write(data)
        return data


@Test
class SQLCase(BaseCase):

    class Input:
        connect = Field(maybe(oneof(str, dictof(str, object))))
        sql = Field(str)
        autocommit = Field(bool, default=False)
        ignore = Field(bool, default=False)

        @property
        def sql_key(self):
            if is_filename(self.sql):
                return self.sql
            else:
                return to_identifier(self.sql)

        @property
        def sql_as_filename(self):
            if is_filename(self.sql):
                return self.sql

        @property
        def sql_as_source(self):
            if not is_filename(self.sql):
                return self.sql

        def __str__(self):
            return "SQL: %s" % self.sql_key

    def check(self):
        filename = self.input.sql_as_filename
        if filename is not None:
            stream = open(self.input.sql)
            source = stream.read()
            stream.close()
        else:
            source = self.input.sql
        from htsql import HTSQL
        from htsql.core.error import Error
        from htsql.core.connect import connect
        from htsql.core.split_sql import split_sql
        try:
            app = HTSQL(self.input.connect)
        except Exception:
            self.ui.literal(traceback.format_exc())
            self.ctl.failed("exception occured while"
                            " initializing an HTSQL application")
            return

        with app:
            try:
                statements = list(split_sql(source))
            except ValueError as exc:
                self.ctl.failed("cannot parse SQL: %s" % exc)
                return

            try:
                connection = connect(with_autocommit=self.input.autocommit)
                cursor = connection.cursor()
            except Error as exc:
                self.ui.literal(str(exc))
                self.ctl.failed("failed to connect to the database")
                return

            for statement in statements:
                try:
                    cursor.execute(statement)
                except Error as exc:
                    self.ui.literal(statement)
                    self.ui.literal(str(exc))
                    if not self.input.ignore:
                        self.ctl.failed("failed to execute SQL")
                        return
                    break

            else:
                if not self.input.autocommit:
                    try:
                        connection.commit()
                    except Error as exc:
                        self.ui.literal(str(exc))
                        if not self.input.ignore:
                            self.ctl.failed("failed to commit the transaction")
                            return

            try:
                connection.close()
            except Error as exc:
                self.ui.literal(str(exc))
                self.ctl.failed("failed to close the connection")
                return

        return self.ctl.passed()


class SavedDB(object):

    def __init__(self, db, *extensions):
        self.db = db
        self.extensions = extensions


@Test
class DBCase(BaseCase):

    class Input:
        db = Field(maybe(oneof(str, dictof(str, object))))
        extensions = Field(dictof(str, dictof(str, object)), default={})
        save = Field(str, default=None)

        def __str__(self):
            if not self.db:
                return "DB: -"
            try:
                from htsql.core.util import DB
            except ImportError:
                return "DB: -"
            try:
                db = DB.parse(self.db)
            except ValueError:
                return "DB: -"
            db = db.clone(password=None)
            return "DB: %s" % db

    def check(self):
        from htsql import HTSQL
        self.ctl.state['htsql'] = None
        try:
            self.ctl.state['htsql'] = HTSQL(self.input.db,
                                            self.input.extensions)
        except Exception:
            self.ui.literal(traceback.format_exc())
            self.ctl.failed("exception occured while"
                            " initializing the HTSQL application")
            return

        # Record the configuration.
        if self.input.save is not None:
            self.ctl.state[self.input.save] = SavedDB(self.input.db,
                                                      self.input.extensions)

        self.ctl.passed()
        return


@Test
class DBLoadCase(BaseCase):

    class Input:
        load = Field(str)
        extensions = Field(dictof(str, dictof(str, object)), default={})
        save = Field(str, default=None)

    def check(self):
        if not isinstance(self.ctl.state.get(self.input.load), SavedDB):
            self.ctl.failed("unknown configuration %r" % self.input.load)
            return
        configuration = self.ctl.state[self.input.load]
        db = configuration.db
        extensions = configuration.extensions+(self.input.extensions,)
        from htsql import HTSQL
        self.ctl.state['htsql'] = None
        try:
            self.ctl.state['htsql'] = HTSQL(db, *extensions)
        except Exception:
            self.ui.literal(traceback.format_exc())
            self.ctl.failed("exception occured while"
                            " initializing the HTSQL application")
            return

        # Record the configuration.
        if self.input.save is not None:
            self.ctl.state[self.input.save] = SavedDB(db, *extensions)

        self.ctl.passed()


@Test
class QueryCase(MatchCase):

    class Input:
        uri = Field(str)
        method = Field(choiceof(['GET', 'POST']), default='GET')
        remote_user = Field(str, default=None)
        headers = Field(dictof(str, str), default=None)
        content_type = Field(str, default=None)
        content_body = Field(str, default=None)
        expect = Field(int, default=200)

    class Output:
        uri = Field(str)
        status = Field(str)
        headers = Field(listof(tupleof(str, str)))
        body = Field(str)

        @classmethod
        def __load__(cls, mapping):
            if 'headers' in mapping and \
                    isinstance(mapping['headers'], listof(listof(str, length=2))):
                mapping['headers'] = [tuple(header)
                                      for header in mapping['headers']]
            return super(QueryCase.Output, cls).__load__(mapping)

        def __dump__(self):
            return [
                    ('uri', self.uri),
                    ('status', self.status),
                    ('headers', [list(header) for header in self.headers]),
                    ('body', self.body)]

    def run(self):
        app = self.ctl.state.get('htsql')
        if app is None:
            self.ui.warning("HTSQL application is not defined")
            return

        from htsql.ctl.request import Request

        request = Request.prepare(method=self.input.method,
                                  query=self.input.uri,
                                  remote_user=self.input.remote_user,
                                  content_type=self.input.content_type,
                                  content_body=self.input.content_body,
                                  extra_headers=self.input.headers)
        response = request.execute(app)

        if response.exc_info is not None:
            lines = traceback.format_exception(*response.exc_info)
            self.ui.literal("".join(lines))
            self.ui.warning("exception occured while executing the query")
            return
        if not response.complete():
            self.ui.warning("incomplete response")
            return

        new_output = self.Output(uri=self.input.uri,
                                 status=response.status,
                                 headers=response.headers,
                                 body=response.body.decode('utf-8', 'replace'))

        if not response.status.startswith(str(self.input.expect)):
            text = self.render(self.output)
            new_text = self.render(new_output)
            self.compare(text, new_text)
            self.ui.warning("unexpected status code: %s" % response.status)
            return

        return new_output

    def render(self, output):
        if output is None:
            return None
        lines = []
        lines.append(output.status)
        for header, value in output.headers:
            lines.append("%s: %s" % (header, value))
        lines.append("")
        lines.extend(output.body.splitlines())
        return "\n".join(lines)+"\n"


@Test
class CtlCase(MatchCase):

    class Input:
        ctl = Field(listof(str))
        stdin = Field(str, default='')
        expect = Field(int, default=0)

        def __str__(self):
            return "CTL: %s" % " ".join(self.ctl)

    class Output:
        ctl = Field(listof(str))
        stdout = Field(str)

    def run(self):
        stdout = io.StringIO()
        stderr = stdout
        stdin = TermStringIO(self.input.stdin, stdout)
        command_line = ['htsql-ctl']+self.input.ctl

        # The script class.
        from htsql.ctl import HTSQL_CTL

        # Initialize and execute the script; check for exceptions.
        try:
            ctl = HTSQL_CTL(stdin, stdout, stderr)
            exit = ctl.main(command_line)
        except:
            self.ui.literal(stdout.getvalue())
            self.ui.literal(traceback.format_exc())
            self.ui.warning("exception occured"
                            " while running the application")
            return

        # Normalize the exit code.
        if exit is None:
            exit = 0
        elif not isinstance(exit, int):
            stderr.write(str(exit))
            exit = 1

        if exit != self.input.expect and self.input.ignore is not True:
            self.ui.literal(stdout.getvalue())
            self.ui.warning("unexpected exit code: %s" % exit)
            return

        new_output = self.Output(ctl=self.input.ctl,
                                 stdout=stdout.getvalue())
        return new_output

    def render(self, output):
        return output.stdout


class Fork(object):

    active_forks = []
    active_fork_map = {}
    is_atexit_registered = False

    @classmethod
    def push(cls, key, fork):
        cls.active_fork_map[key] = fork

    @classmethod
    def pop(cls, key):
        return cls.active_fork_map.pop(key)

    @classmethod
    def start(cls, executable, arguments, stdin):
        # Create a temporary directory with the files 'input' and 'output'.
        temp_path = tempfile.mkdtemp()
        stream = open("%s/input" % temp_path, 'wb')
        stream.write(stdin)
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


@Test
class StartCtlCase(BaseCase):

    class Input:
        start_ctl = Field(listof(str))
        stdin = Field(str, default='')
        sleep = Field(oneof(int, float), default=0)

        def __str__(self):
            return "START-CTL: %s" % " ".join(self.start_ctl)

    def check(self):
        key = tuple(self.input.start_ctl)
        ctl_path = self.state.get('HTSQL_CTL', 'htsql-ctl')
        fork = Fork.start(ctl_path,
                          self.input.start_ctl,
                          self.input.stdin)
        Fork.push(key, fork)


@Test
class EndCtlCase(MatchCase):

    class Input:
        end_ctl = Field(listof(str))

        def __str__(self):
            return "END-CTL: %s" % " ".join(self.end_ctl)

    class Output:
        end_ctl = Field(listof(str))
        stdout = Field(str)

    def run(self):
        key = tuple(self.input.end_ctl)
        fork = Fork.pop(key)
        stdout = fork.end()

        new_output = self.Output(end_ctl=self.input.end_ctl,
                                 stdout=stdout)
        return new_output

    def render(self, output):
        return output.stdout


