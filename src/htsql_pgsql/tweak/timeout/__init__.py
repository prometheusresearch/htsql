#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from . import connect
from htsql.core.addon import Addon


class TweakTimeoutPGSQLAddon(Addon):

    name = 'tweak.timeout.pgsql'
    hint = """implement `tweak.timeout` for PostgreSQL"""
    prerequisites = ['engine.pgsql']


