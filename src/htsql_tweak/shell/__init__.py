#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from . import command, locate
from htsql.addon import Addon, Parameter
from htsql.validator import StrVal, PIntVal


class TweakShellAddon(Addon):

    name = 'tweak.shell'
    prerequisites = ['tweak.resource']
    hint = """add in-browser editor for HTSQL"""
    help = """
    This addon adds command `/shell()` that creates an in-browser
    HTSQL editor called HTSQL shell.

    The HTSQL shell provides an edit area with support for syntax
    hightlighting and code completion.

    Parameter `server_root` specifies the root URL of the HTSQL
    server.  Use this parameter when HTSQL is unable to determine
    its root automatically.

    Parameter `limit` specifies the maximum number of output rows
    displayed by the shell.
    """

    parameters = [
            Parameter('server_root', StrVal(r'^https?://.+$'),
                      value_name="URL",
                      hint="""root of HTSQL server"""),
            Parameter('limit', PIntVal(is_nullable=True), default=1000,
                      hint="""max. number of output rows (default: 1000)"""),
    ]


