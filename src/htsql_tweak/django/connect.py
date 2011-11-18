#
# Copyright (c) 2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#

from htsql.context import context
from htsql.connect import Connect
from htsql.adapter import weigh


class DjangoConnect(Connect):

    weigh(2.0) # ensure connections here are not pooled

    def open(self):
        from django.db import connections
        from django.db.utils import DEFAULT_DB_ALIAS
        return connections[DEFAULT_DB_ALIAS]


