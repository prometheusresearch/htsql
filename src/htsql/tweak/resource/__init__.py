#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from . import wsgi, locate
from ...core.addon import Addon, Parameter
from ...core.validator import StrVal
import threading


class TweakResourceAddon(Addon):

    name = 'tweak.resource'
    hint = """serve static files"""
    help = """
    This addon adds a mechanism for serving static files via HTTP.
    This mechanism is used by other addons to provide access to static
    resources such as Javascript and CSS files.

    Normally, static files are served under HTTP prefix `/-/`.  Use
    parameter `indicator` to change the prefix.
    """

    parameters = [
            Parameter('indicator', StrVal(r'^[^/]+$'), default='-',
                      value_name="STR",
                      hint="""location for static files (default: `-`)"""),
    ]

    def __init__(self, app, attributes):
        super(TweakResourceAddon, self).__init__(app, attributes)
        self.lock = threading.Lock()


