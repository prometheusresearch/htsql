#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from . import encode
from htsql.addon import Addon, Parameter
from htsql.validator import PIntVal


class TweakAutolimitAddon(Addon):

    name = 'tweak.autolimit'
    hint = """limit # of rows returned by queries"""
    help = """
      To help deployments ensure against accidental denial of
      service, this plugin automatically truncates output from
      a query to a given number of rows (10k default).  The 
      ``limit`` parameter can be customized to change the 
      truncation limit.
    """

    parameters = [
            Parameter('limit', PIntVal(is_nullable=True),
                      default=10000),
    ]


