#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#

from ...core.connect import Connect
from ...core.adapter import rank


class DjangoConnect(Connect):

    rank(2.0) # ensure connections here are not pooled

    def open(self):
        from django.db import connections
        from django.db.utils import DEFAULT_DB_ALIAS
        return connections[DEFAULT_DB_ALIAS]


