#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.context`
====================

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

    def switch(self, old_app, app):
        """
        Activate/inactivate an HTSQL application.

        `old_app` (:class:`htsql.application.Application` or ``None``)
            The current active application or ``None`` if there is no one.

        `app` (:class:`htsql.application.Application` or ``None``)
            The new active application or ``None`` to just inactivate
            the current active application.
        """
        assert self.active_app is old_app
        self.active_app = app

    @property
    def app(self):
        """
        Returns the active HTSQL application.

        This property never returns ``None``; when there is no active
        application, it raises an exception.
        """
        assert self.active_app is not None
        return self.active_app


context = ThreadContext()


