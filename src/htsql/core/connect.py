#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


"""
:mod:`htsql.core.connect`
=========================

This module declares the database connection adapter.
"""


from .adapter import Adapter, Utility, adapt
from .domain import Domain, Record
from .error import Error, EngineError
from .context import context


class DBErrorGuard(object):
    """
    Guards against DBAPI exception.

    This class converts DBAPI exceptions to :exc:`EngineError`.  It is designed
    to be used in a ``with`` clause.

    Usage::

        connect = Connect()
        try:
            with DBErrorGuard():
                connection = connect.open_connection()
                cursor = connection.cursor()
                ...
        except EngineError:
            ...
    """

    def __enter__(self):
        # Enters the `with` clause.
        pass

    def __exit__(self, exc_type, exc_value, exc_traceback):
        # Exits from the `with` clause.

        # If no exception occurred, we are done.
        if exc_type is None:
            return

        # Get an exception instance.  Depending on the form of
        # the `raise` command, `exc_value` could be either an
        # exception object or its argument.  If latter, we need
        # to create an exception instance.
        if isinstance(exc_value, Exception):
            exception = exc_value
        elif exc_value is None:
            exception = exc_type()
        elif isinstance(exc_value, tuple):
            exception = exc_type(*exc_value)
        else:
            exception = exc_type(exc_value)

        # Convert the exception.
        error = unscramble_error(exception)

        # If we got a new exception, raise it.
        if error is not None:
            raise EngineError("Got an error from the database driver", error)


class ConnectionProxy(object):
    """
    Wraps a DBAPI connection object.

    The proxy supports common DBAPI methods by passing them to
    the connection object.  Any exceptions raised when the executing
    DBAPI methods are converted to :exc:`Error`.

    `connection`
        A raw DBAPI connection object.

    `guard` (:class:`DBErrorGuard`)
        A DBAPI exception guard.
    """

    def __init__(self, connection, guard):
        self.connection = connection
        self.guard = guard
        self.is_busy = True
        self.is_valid = True

    def cursor(self):
        """
        Returns a database cursor.

        This method actually produces a :class:`CursorProxy` instance.
        """
        with self.guard:
            cursor = self.connection.cursor()
            return CursorProxy(cursor, self.guard)

    def commit(self):
        """
        Commit the current transaction.
        """
        with self.guard:
            return self.connection.commit()

    def rollback(self):
        """
        Rollback the current transaction.
        """
        with self.guard:
            return self.connection.rollback()

    def close(self):
        """
        Close the connection.
        """
        self.is_valid = False
        with self.guard:
            return self.connection.close()

    def invalidate(self):
        self.is_valid = False

    def acquire(self):
        assert not self.is_busy
        self.is_busy = True

    def release(self):
        assert self.is_busy
        self.is_busy = False


class CursorProxy(object):
    """
    Wraps a DBAPI cursor object.

    The proxy supports common DBAPI methods by passing them to
    the cursor object.  Any exceptions raised when the executing
    DBAPI methods are converted to :exc:`Error`.

    `cursor`
        A raw DBAPI cursor object.

    `guard` (:class:`DBErrorGuard`)
        A DBAPI exception guard.
    """

    def __init__(self, cursor, guard):
        self.cursor = cursor
        self.guard = guard

    @property
    def description(self):
        """
        The format of the result rows.
        """
        return self.cursor.description

    @property
    def rowcount(self):
        """
        The number of rows produced or affected by the last statement.
        """
        return self.cursor.rowcount

    def execute(self, statement, *parameters):
        """
        Execute one SQL statement.
        """
        addon = context.app.htsql
        if addon.debug:
            try:
                with self.guard:
                    return self.cursor.execute(statement, *parameters)
            except Error as exc:
                exc.wrap("While executing SQL", statement)
                if parameters:
                    parameters = parameters[0]
                    exc.wrap("With parameters", repr(parameters))
                raise
        else:
            with self.guard:
                return self.cursor.execute(statement, *parameters)

    def executemany(self, statement, parameters_set):
        """
        Execute an SQL statement against all parameters.
        """
        addon = context.app.htsql
        if addon.debug:
            try:
                with self.guard:
                    return self.cursor.executemany(statement, parameters_set)
            except Error as exc:
                exc.wrap("While executing SQL", statement)
                if not parameters_set:
                    exc.wrap("With no parameters")
                else:
                    exc.wrap("With a set of parameters",
                             "\n".join(repr(list(group))
                                       for group in parameters_set))
                raise
        else:
            with self.guard:
                return self.cursor.executemany(statement, parameters_set)

    def fetchone(self):
        """
        Fetch the next row of the result.
        """
        with self.guard:
            return self.cursor.fetchone()

    def fetchmany(self, *size):
        """
        Fetch the next set of rows of the result.
        """
        with self.guard:
            return self.cursor.fetchmany(*size)

    def fetchall(self):
        """
        Fetch all remaining rows of the result.
        """
        with self.guard:
            return self.cursor.fetchall()

    def fetchnamed(self):
        with self.guard:
            rows = self.fetchall()
            if not rows:
                return rows
            fields = [kind[0].lower() for kind in self.description]
        Row = Record.make(None, fields)
        return [Row(row) for row in rows]

    def __iter__(self):
        """
        Iterates over the rows of the result.
        """
        iterator = iter(self.cursor)
        while True:
            with self.guard:
                try:
                    row = next(iterator)
                except StopIteration:
                    row = None
            if row is None:
                break
            yield row

    def close(self):
        """
        Close the cursor.
        """
        with self.guard:
            return self.cursor.close()


class Connect(Utility):
    """
    Declares the connection interface.

    The connection interface is a utility to open database connections.

    Usage::

        connect = Connect()
        try:
            connection = connect()
            cursor = connection.cursor()
            cursor.execute(...)
            cursor.fetchall()
            ...
        except Error:
            ...
    """

    def __init__(self, with_autocommit=False):
        assert isinstance(with_autocommit, bool)
        self.with_autocommit = with_autocommit

    def __call__(self):
        """
        Returns a connection object.

        The connection object is an instance of :class:`ConnectionProxy`
        and supports common DBAPI methods.

        If the database parameters for the application are invalid,
        the method may raise :exc:`Error`.
        """
        # Create a guard for DBAPI exceptions.
        guard = DBErrorGuard()
        # Open a raw connection while intercepting DBAPI exceptions.
        with guard:
            connection = self.open()
        # Return a proxy object.
        proxy = ConnectionProxy(connection, guard)
        return proxy

    def open(self):
        """
        Returns a raw DBAPI connection object.

        `with_autocommit` (Boolean)
            If set, the connection is opened in the autocommit mode.
        """
        # Override when subclassing.
        raise NotImplementedError()


class Scramble(Adapter):

    adapt(Domain)

    @staticmethod
    def convert(value):
        return value

    def __init__(self, domain):
        self.domain = domain

    def __call__(self):
        return self.convert


class Unscramble(Adapter):

    adapt(Domain)

    @staticmethod
    def convert(value):
        return value

    def __init__(self, domain):
        self.domain = domain

    def __call__(self):
        return self.convert


class UnscrambleError(Utility):

    def __init__(self, error):
        assert isinstance(error, Exception)
        self.error = error

    def __call__(self):
        return None


class Transact(Utility):

    def __call__(self):
        return TransactionGuard()


class TransactionGuard(object):

    def __init__(self):
        self.connection = context.env.connection

    def __enter__(self):
        if self.connection is None:
            connection = connect()
            context.env.push(connection=connection)
            return connection
        return self.connection

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if self.connection is None:
            connection = context.env.connection
            context.env.pop()
            if exc_type is None:
                connection.commit()
            else:
                # FIXME: ?
                # To avoid issues with pymssql driver, we do not issue
                # rollback manually, but instead let it rollback the
                # transaction automatically when the connection is garbage
                # collected.
                #connection.rollback()
                connection.invalidate()
            connection.release()


connect = Connect.__invoke__
scramble = Scramble.__invoke__
unscramble = Unscramble.__invoke__
unscramble_error = UnscrambleError.__invoke__
transaction = Transact.__invoke__


