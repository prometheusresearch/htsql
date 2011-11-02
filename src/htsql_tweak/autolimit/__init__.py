#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from . import encode
from htsql.addon import Addon, Parameter
from htsql.validator import PIntVal


class TweakAutolimitAddon(Addon):

    name = 'tweak.autolimit'
    hint = """limit number of rows returned by queries"""
    help = """
    This addon automatically truncates output from a query to
    a given number of rows (10,000, by default).

    Use this addon to prevent accidental denial of service
    caused by large query output.

    The `limit` parameter sets the truncation threshold.
    """

    parameters = [
            Parameter('limit', PIntVal(is_nullable=True), default=10000,
                      hint="""max. number of rows (default: 10000)"""),
    ]


