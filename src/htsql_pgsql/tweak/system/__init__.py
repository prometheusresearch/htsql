#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from . import introspect
from htsql.core.addon import Addon


class TweakSystemPGSQLAddon(Addon):

    name = 'tweak.system.pgsql'
    prerequisites = ['engine.pgsql']
    hint = """implement `tweak.system` for PostgreSQL"""


