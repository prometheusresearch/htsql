#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from . import act
from htsql.addon import Addon, Parameter
from htsql.validator import BoolVal


class TweakShellDefaultAddon(Addon):

    name = 'tweak.shell.default'
    hint = """makes /shell() be default command"""
    help = """
      This plugin makes the default formatter be the command shell,
      rather than plain HTML.  Of course ``/:html`` format is still
      available, but must be provided explicitly.
    """

    parameters = [
            Parameter('on_root', BoolVal(), default=True),
            Parameter('on_default', BoolVal(), default=True),
            Parameter('on_error', BoolVal(), default=True),
    ]


