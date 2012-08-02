#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql.core.context`
=========================

This module keeps the active HTSQL application in a thread-local variable.

This module exports one global variable:

`context` (:class:`ThreadContext`)
    A thread-local variable that holds the active application.
"""


import threading


class ThreadContext(threading.local):
    """
    Keeps the active HTSQL application and environment.
    """

    def __init__(self):
        self.active_app = None
        self.active_env = None
        self.stack = []

    def push(self, app, env):
        self.stack.append((self.active_app, self.active_env))
        self.active_app = app
        self.active_env = env

    def pop(self, app):
        assert app is self.active_app
        assert self.stack
        self.active_app, self.active_env = self.stack.pop()

    @property
    def app(self):
        """
        Returns the active HTSQL application.

        This property never returns ``None``; when there is no active
        application, it raises an exception.
        """
        if self.active_app is None:
            raise RuntimeError("HTSQL application is not activated")
        return self.active_app

    @property
    def env(self):
        if self.active_env is None:
            raise RuntimeError("HTSQL environment is not activated")
        return self.active_env


context = ThreadContext()


