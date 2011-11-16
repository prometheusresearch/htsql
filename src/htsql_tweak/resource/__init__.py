#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from . import wsgi, locate
from htsql.addon import Addon, Parameter
from htsql.validator import StrVal
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


