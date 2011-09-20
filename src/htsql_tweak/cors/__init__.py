#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from . import wsgi
from htsql.addon import Addon


class TweakCORSAddon(Addon):

    name = 'tweak.cors'
    hint = """permit cross site scripting"""
    help = """
      This plugin adds CORS headers in order to enable cross
      site scripting for public data servers.  This permits
      modern browsers to bypass JSONP and other hacks used
      to work around XSS protection.
    """


