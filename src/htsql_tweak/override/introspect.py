#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.context import context
from htsql.adapter import weigh
from htsql.introspect import Introspect
import threading


class UnusedPatternCache(object):

    def __init__(self):
        self.patterns = None
        self.lock = threading.Lock()

    def update(self, patterns):
        with self.lock:
            if self.patterns is None:
                self.patterns = patterns


class OverrideIntrospect(Introspect):

    weigh(1.0)

    def __call__(self):
        addon = context.app.tweak.override
        catalog = super(OverrideIntrospect, self).__call__()
        unused = set()

        if addon.include_schemas or addon.exclude_schemas:
            include = addon.include_schemas
            exclude = addon.exclude_schemas
            unused.update(include)
            unused.update(exclude)
            for schema in reversed(list(catalog.schemas)):
                include_matches = [pattern
                                   for pattern in include
                                   if pattern.matches(schema)]
                exclude_matches = [pattern
                                   for pattern in exclude
                                   if pattern.matches(schema)]
                if exclude_matches or (include and not include_matches):
                    schema.remove()
                unused.difference_update(include_matches)
                unused.difference_update(exclude_matches)

        if addon.include_tables or addon.exclude_tables:
            include = addon.include_tables
            exclude = addon.exclude_tables
            unused.update(include)
            unused.update(exclude)
            for schema in catalog.schemas:
                schema_exclude = [pattern
                                  for pattern in exclude
                                  if pattern.matches(schema)]
                if not (include or schema_exclude):
                    continue
                for table in reversed(list(schema.tables)):
                    include_matches = [pattern
                                       for pattern in include
                                       if pattern.matches(table)]
                    exclude_matches = [pattern
                                       for pattern in schema_exclude
                                       if pattern.matches(table)]
                    if exclude_matches or (include and not include_matches):
                        table.remove()
                    unused.difference_update(include_matches)
                    unused.difference_update(exclude_matches)

        if addon.include_columns or addon.exclude_columns:
            include = addon.include_columns
            exclude = addon.exclude_columns
            unused.update(include)
            unused.update(exclude)
            for schema in catalog.schemas:
                schema_exclude = [pattern
                                  for pattern in exclude
                                  if pattern.matches(schema)]
                if not (include or schema_exclude):
                    continue
                for table in schema.tables:
                    table_exclude = [pattern
                                     for pattern in schema_exclude
                                     if pattern.matches(table)]
                    if not (include or table_exclude):
                        continue
                    for column in reversed(list(table.columns)):
                        include_matches = [pattern
                                           for pattern in include
                                           if pattern.matches(column)]
                        exclude_matches = [pattern
                                           for pattern in table_exclude
                                           if pattern.matches(column)]
                        if exclude_matches or (include and not include_matches):
                            column.remove()
                        unused.difference_update(include_matches)
                        unused.difference_update(exclude_matches)

        if addon.not_nulls:
            unused.update(addon.not_nulls)
            for schema in catalog.schemas:
                schema_patterns = [pattern
                                   for pattern in addon.not_nulls
                                   if pattern.matches(schema)]
                if not schema_patterns:
                    continue
                for table in schema.tables:
                    table_patterns = [pattern
                                      for pattern in schema_patterns
                                      if pattern.matches(table)]
                    if not table_patterns:
                        continue
                    for column in reversed(list(table.columns)):
                        matches = [pattern
                                   for pattern in table_patterns
                                   if pattern.matches(column)]
                        if matches:
                            column.set_is_nullable(False)
                            unused.difference_update(matches)

        if addon.unique_keys:
            unused.update(addon.unique_keys)
            for schema in catalog.schemas:
                schema_keys = [pattern
                               for pattern in addon.unique_keys
                               if pattern.matches(schema)]
                if not schema_keys:
                    continue
                for table in schema.tables:
                    table_keys = [pattern
                                  for pattern in schema_keys
                                  if pattern.matches(table)]
                    for pattern in table_keys:
                        columns = pattern.extract(table)
                        if columns is None:
                            continue
                        if pattern.is_primary:
                            for column in columns:
                                column.set_is_nullable(False)
                            if table.primary_key is not None:
                                table.primary_key.set_is_primary(False)
                        table.add_unique_key(columns, pattern.is_primary,
                                             pattern.is_partial)
                        unused.discard(pattern)

        if addon.foreign_keys:
            unused.update(addon.foreign_keys)
            for schema in catalog.schemas:
                schema_keys = [pattern
                               for pattern in addon.foreign_keys
                               if pattern.matches(schema)]
                if not schema_keys:
                    continue
                for table in schema.tables:
                    table_keys = [pattern
                                  for pattern in schema_keys
                                  if pattern.matches(table)]
                    for pattern in table_keys:
                        columns = pattern.extract(table)
                        if columns is None:
                            continue
                        targets = [target_table
                                   for target_schema in catalog.schemas
                                   if pattern.matches_target(target_schema)
                                   for target_table in target_schema.tables
                                   if pattern.matches_target(target_table)
                                   and pattern.extract_target(target_table)]
                        if len(targets) != 1:
                            continue
                        [target_table] = targets
                        target_columns = pattern.extract_target(target_table)
                        table.add_foreign_key(columns, target_table,
                                              target_columns,
                                              pattern.is_partial)
                        unused.discard(pattern)

        unused_patterns = []
        for pattern in (addon.include_schemas + addon.exclude_schemas +
                        addon.include_tables + addon.exclude_tables +
                        addon.include_columns + addon.exclude_columns +
                        addon.not_nulls + addon.unique_keys +
                        addon.foreign_keys):
            if pattern in unused:
                unused_patterns.append(pattern)

        #for pattern in sorted(addon.labels, key=(lambda node: str(node))):
        #    node = pattern.extract(catalog)
        #    if node is None:
        #        unused_patterns.append(pattern)
        #    for name in sorted(addon.labels[pattern]):
        #        arc_pattern = addon.labels[pattern][name]
        #        arc = arc_pattern.extract(node)
        #        if arc is None:
        #            unused_patterns.append(arc_pattern)

        addon.unused_pattern_cache.update(unused_patterns)

        return catalog


