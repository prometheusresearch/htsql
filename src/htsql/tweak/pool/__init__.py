#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from . import connect
from ...core.addon import Addon
import threading


class TweakPoolAddon(Addon):

    name = 'tweak.pool'
    hint = """cache database connections"""
    help = """
    This addon caches database connections so that a single
    connection could be used to execute more than one query.
    """

    def __init__(self, app, attributes):
        super(TweakPoolAddon, self).__init__(app, attributes)
        self.lock = threading.Lock()
        self.items = []


