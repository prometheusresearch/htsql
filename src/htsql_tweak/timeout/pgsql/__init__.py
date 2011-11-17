#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from . import connect
from htsql.addon import Addon


class TweakTimeoutPGSQLAddon(Addon):

    name = 'tweak.timeout.pgsql'
    hint = """implement `tweak.timeout` for PostgreSQL"""
    prerequisites = ['engine.pgsql']


