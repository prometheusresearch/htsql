#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.produce`
====================

This module implements the produce utility.
"""


from .adapter import Utility, find_adapters
from .connect import DBError, Connect, Normalize


class Profile(object):

    def __init__(self, plan):
        self.plan = plan
        self.frame = plan.frame
        self.sketch = plan.sketch
        self.term = plan.term
        self.code = plan.code
        self.binding = plan.binding
        self.syntax = plan.syntax


class Product(object):

    def __init__(self, profile, records=None):
        self.profile = profile
        self.records = records

    def __iter__(self):
        if self.records is not None:
            return iter(self.records)
        else:
            return iter([])

    def __nonzero__(self):
        return (self.records is not None)


class Produce(Utility):

    def __call__(self, plan):
        profile = Profile(plan)
        records = None
        if plan.sql:
            try:
                connect = Connect()
                connection = connect()
                cursor = connection.cursor()
                cursor.execute(plan.sql)
                rows = cursor.fetchall()
                connection.close()
            except DBError, exc:
                raise EngineError(str(exc))
            records = []
            select = plan.frame.segment.select
            normalizers = []
            for phrase in plan.frame.segment.select:
                normalize = Normalize(phrase.domain)
                normalizers.append(normalize)
            for row in rows:
                values = []
                for item, normalize in zip(row, normalizers):
                    value = normalize(item)
                    values.append(value)
                records.append((values))
        return Product(profile, records)


produce_adapters = find_adapters()


