#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from . import introspect, pattern
from htsql.addon import Addon, Parameter
from htsql.validator import SeqVal
from .pattern import SchemaPatternVal, TablePatternVal, ColumnPatternVal


class TweakOverrideAddon(Addon):

    name = 'tweak.override'

    parameters = [
            Parameter('include_schema', SeqVal(SchemaPatternVal())),
            Parameter('exclude_schema', SeqVal(SchemaPatternVal())),
            Parameter('include_table', SeqVal(TablePatternVal())),
            Parameter('exclude_table', SeqVal(TablePatternVal())),
            Parameter('include_column', SeqVal(ColumnPatternVal())),
            Parameter('exclude_column', SeqVal(ColumnPatternVal())),
#            Parameter('not-null', SeqVal(NotNullConstraintVal())),
#            Parameter('unique-key', SeqVal(UniqueKeyConstraintVal())),
#            Parameter('foreign-key', SeqVal(ForeignKeyConstraintVal())),
    ]


