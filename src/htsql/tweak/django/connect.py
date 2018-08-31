#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#

from ...core.connect import Connect, Transact, connect
from ...core.adapter import rank
from ...core.context import context


class DjangoConnect(Connect):

    rank(2.0) # ensure connections here are not pooled

    def open(self):
        from django.db import connections
        from django.db.utils import DEFAULT_DB_ALIAS
        return connections[DEFAULT_DB_ALIAS]


class DjangoTransact(Transact):

    def __call__(self):
        return DjangoTransactionGuard()


class DjangoTransactionGuard:

    def __init__(self):
        self.is_nested = None
        self.is_managed = None

    def __enter__(self):
        self.is_nested = True
        self.is_managed = True
        if context.env.connection is None:
            connection = connect()
            context.env.push(connection=connection)
            self.is_nested = False
            if not connection.connection.is_managed():
                connection.connection.enter_transaction_management()
                self.is_managed = False
        return context.env.connection

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if not self.is_nested:
            connection = context.env.connection
            context.env.pop()
            if exc_type is None:
                if not self.is_managed:
                    connection.commit()
                    connection.connection.leave_transaction_management()
                else:
                    connection.connection.commit_unless_managed()
            else:
                if not self.is_managed:
                    connection.connection.leave_transaction_management()
                connection.invalidate()
            connection.release()
        self.is_nested = None
        self.is_forced = None


