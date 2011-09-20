#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.addon import Addon


class TweakINetAddon(Addon):

    name = 'tweak.inet'
    hint = """adds support for inet data types"""
    help = """
      This plugin adds support for various internet data
      types for PostgreSQL.
    """

    @classmethod
    def get_extension(cls, app, attributes):
        return 'tweak.inet.%s' % app.htsql.db.engine


