#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from . import wsgi
from htsql.addon import Addon


class TweakCORSAddon(Addon):

    name = 'tweak.cors'


