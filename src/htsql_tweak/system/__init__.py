#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.addon import Addon


class TweakSystemAddon(Addon):

    name = 'tweak.system'

    @classmethod
    def get_extension(cls, app, attributes):
        return 'tweak.system.%s' % app.htsql.db.engine


