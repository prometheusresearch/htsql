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
    hint = """registers visual /shell() command"""
    help = """
      This command creates a command line editor in Javascript
      that can let users edit multi-line queries using a web-browser.
    """

    parameters = [
            Parameter('server_root', StrVal(r'^https?://.+$')),
            Parameter('limit', PIntVal(is_nullable=True), default=1000),
    ]


