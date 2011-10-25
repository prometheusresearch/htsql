#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from . import introspect, pattern
from htsql.addon import Addon, Parameter
from htsql.validator import SeqVal
from htsql.introspect import introspect
from .pattern import (SchemaPatternVal, TablePatternVal, ColumnPatternVal,
                      UniqueKeyPatternVal, ForeignKeyPatternVal)
from .introspect import UnusedPatternCache


class TweakOverrideAddon(Addon):

    name = 'tweak.override'

    parameters = [
            Parameter('include_schemas', SeqVal(SchemaPatternVal()),
                      default=[]),
            Parameter('exclude_schemas', SeqVal(SchemaPatternVal()),
                      default=[]),
            Parameter('include_tables', SeqVal(TablePatternVal()),
                      default=[]),
            Parameter('exclude_tables', SeqVal(TablePatternVal()),
                      default=[]),
            Parameter('include_columns', SeqVal(ColumnPatternVal()),
                      default=[]),
            Parameter('exclude_columns', SeqVal(ColumnPatternVal()),
                      default=[]),
            Parameter('not_nulls', SeqVal(ColumnPatternVal()),
                      default=[]),
            Parameter('unique_keys', SeqVal(UniqueKeyPatternVal()),
                      default=[]),
            Parameter('foreign_keys', SeqVal(ForeignKeyPatternVal()),
                      default=[]),
    ]

    def __init__(self, app, attributes):
        super(TweakOverrideAddon, self).__init__(app, attributes)
        self.unused_pattern_cache = UnusedPatternCache()

    def validate(self):
        catalog = introspect()
        unused_patterns = self.unused_pattern_cache.patterns
        if unused_patterns:
            raise ValueError("unused override patterns: %s"
                             % ", ".join(str(pattern)
                                         for pattern in unused_patterns))


