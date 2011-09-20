#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.addon import Addon


class TweakSystemAddon(Addon):

    name = 'tweak.system'
    hint = """direct access to system catalog"""
    help = """
      This plugin adds the system catalog tables and links 
      for the database's native system catalog.  This is 
      supported only for PostgreSQL.
    """

    @classmethod
    def get_extension(cls, app, attributes):
        return 'tweak.system.%s' % app.htsql.db.engine


