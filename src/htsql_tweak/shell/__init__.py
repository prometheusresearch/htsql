#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from . import command, locate
from htsql.addon import Addon, Parameter
from htsql.validator import StrVal


class TweakShellAddon(Addon):

    name = 'tweak.shell'
    prerequisites = ['tweak.resource']

    parameters = [
            Parameter('server_root', StrVal(r'^https?://.+$')),
    ]


