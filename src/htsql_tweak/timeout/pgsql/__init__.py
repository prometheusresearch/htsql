#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from . import connect
from htsql.addon import Addon


class TweakTimeoutPGSQLAddon(Addon):

    name = 'tweak.timeout.pgsql'
    hint = """implements query timeout for PostgreSQL"""
    help = """
      This plugin is used to set the query timeout using PostgreSQL
      specific connection parameter.
    """
    prerequisites = ['engine.pgsql']


