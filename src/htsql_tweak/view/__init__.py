#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.addon import Addon


class TweakViewAddon(Addon):

    name = 'tweak.view'
    hint = """guesses links for views"""
    help = """
      This plugin attempts to guess at various links 
      between views and tables (where foreign keys are
      not defined).  This is only supported in PostgreSQL.
    """

    @classmethod
    def get_extension(cls, app, attributes):
        return 'tweak.view.%s' % app.htsql.db.engine


