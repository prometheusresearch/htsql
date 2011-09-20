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
    hint = """static resource support"""
    help = """
      This plugin creates a mechanism for other plugins, such as
      the ``tweak.shell`` to provide access to static resources
      such as Javascript and CSS files. 
    """

    parameters = [
            Parameter('indicator', StrVal(r'^[/]+$'), default='-'),
    ]

    def __init__(self, app, attributes):
        super(TweakResourceAddon, self).__init__(app, attributes)
        self.lock = threading.Lock()


