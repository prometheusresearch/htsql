#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from . import introspect, dump
from htsql.addon import Addon


class TweakINetPGSQLAddon(Addon):

    name = 'tweak.inet.pgsql'
    prerequisites = ['engine.pgsql']
    hint = """implement `tweak.inet` for PostgreSQL"""


