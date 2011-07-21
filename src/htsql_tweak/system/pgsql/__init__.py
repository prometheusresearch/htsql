#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from . import introspect
from htsql.addon import Addon


class TweakSystemPGSQLAddon(Addon):

    name = 'tweak.system.pgsql'
    prerequisites = ['engine.pgsql']


