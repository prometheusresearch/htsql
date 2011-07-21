#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from . import introspect, rulesparser
from htsql.addon import Addon


class TweakViewPGSQLAddon(Addon):

    name = 'tweak.view.pgsql'
    prerequisites = ['engine.pgsql']


