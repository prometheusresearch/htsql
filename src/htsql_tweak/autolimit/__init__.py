#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from . import encode
from htsql.addon import Addon, Parameter
from htsql.validator import PIntVal


class TweakAutolimitAddon(Addon):

    name = 'tweak.autolimit'

    parameters = [
            Parameter('limit', PIntVal(is_nullable=True),
                      default=10000),
    ]


