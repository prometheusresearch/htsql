#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


from htsql.util import autoimport
from htsql.addon import Addon


autoimport('htsql_tweak.pgsql_timeout')


class TWEAK_PGSQL_TIMEOUT(Addon):
    pass


