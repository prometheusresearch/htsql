#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from . import wsgi
from ...core.addon import Addon, Parameter
from ...core.validator import StrVal


class TweakCORSAddon(Addon):

    name = 'tweak.cors'
    hint = """enable cross-origin resource sharing"""
    help = """
    This addon adds CORS headers to HTTP output to allow HTSQL queries
    by a web page from a different domain.

    For security reasons, browsers do not allow AJAX requests to cross
    the domain boundaries.  The CORS (Cross-Origin Resource Sharing)
    specification defines a way for a server to provide a list of
    domains which are permitted to make AJAX requests. For more details
    on CORS, see `http://www.w3.org/TR/cors/`.

    The `origin` parameter lists domains which are allowed to access
    the server.  The value of `origin` must be either `*` (default) or
    a space-separated list of host names of the form:

        http[s]://domain[:port]

    The default value of `origin` permits HTSQL queries from any
    domain.  Do not use the default settings with non-public data.
    """

    parameters = [
            Parameter('origin', StrVal(r'^[^\r\n]+$', is_nullable=True),
                      default="*",
                      hint="""URI that may access the server (default: *)"""),
    ]


