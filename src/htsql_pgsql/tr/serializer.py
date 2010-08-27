#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_pgsql.tr.serializer`
================================

This module adapts the SQL serializer for PostgreSQL.
"""


from htsql.adapter import adapts
from htsql.tr.serializer import Format, SerializeBranch


class PGSQLFormat(Format):

    def float(self, value):
        return "%s::float8" % value


class PGSQLSerializeBranch(SerializeBranch):

    def serialize_from_clause(self):
        if (len(self.frame.linkage) == 1 and
            self.frame.linkage[0].frame.is_scalar):
            return None
        return super(PGSQLSerializeBranch, self).serialize_from_clause()


