#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.validator import PIntVal
from htsql.addon import Addon, Parameter


class TweakTimeoutAddon(Addon):

    name = 'tweak.timeout'

    parameters = [
            Parameter('timeout', PIntVal(is_nullable=True),
                      default=60),
    ]

    @classmethod
    def get_extension(cls, app, attributes):
        return 'tweak.timeout.%s' % app.htsql.db.engine


