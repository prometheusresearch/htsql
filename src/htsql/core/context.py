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
    Keeps the active HTSQL application.
    """

    def __init__(self):
        self.active_app = None
        self.app_stack = []

    def push(self, app):
        self.app_stack.append(self.active_app)
        self.active_app = app

    def pop(self, app):
        assert app is self.active_app
        assert self.app_stack
        self.active_app = self.app_stack.pop()

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


context = ThreadContext()


