#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.addon import Addon


class TweakINetAddon(Addon):

    name = 'tweak.inet'

    @classmethod
    def get_extension(cls, app, attributes):
        return 'tweak.inet.%s' % app.htsql.db.engine


