#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from . import command
from htsql.addon import Addon


class TweakMetaAddon(Addon):

    name = 'tweak.meta'

    prerequisites = []

    def __init__(self, app, attributes):
        super(TweakMetaAddon, self).__init__(app, attributes)
        self.cached_slave_app = None


