#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from . import command
from htsql.addon import Addon


class TweakMetaAddon(Addon):

    name = 'tweak.meta'
    hint = """meta-data introspection database"""
    help = """
      This plugin provides a command ``/meta()`` that is an
      in-memory database providing introspection ability.
      The introspection permits listing of tables, columns,
      and links.  For example, ``/meta(/table)`` lists all
      of the tables in the current database.
    """

    prerequisites = []

    def __init__(self, app, attributes):
        super(TweakMetaAddon, self).__init__(app, attributes)
        self.cached_slave_app = None


