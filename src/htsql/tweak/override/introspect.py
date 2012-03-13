#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ...core.context import context
from ...core.adapter import rank
from ...core.introspect import Introspect
import threading


class UnusedPatternCache(object):

    def __init__(self):
        self.patterns = []
        self.lock = threading.Lock()

    def add(self, pattern):
        with self.lock:
            self.patterns.append(pattern)


class OverrideIntrospect(Introspect):

    rank(2.0)

    def __call__(self):
        addon = context.app.tweak.override
        catalog = super(OverrideIntrospect, self).__call__()
        unused = set()

        if addon.included_tables or addon.excluded_tables:
            include = addon.included_tables
            exclude = addon.excluded_tables
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

        if addon.included_columns or addon.excluded_columns:
            include = addon.included_columns
            exclude = addon.excluded_columns
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

        if addon.unlabeled_tables:
            unused.update(addon.unlabeled_tables)
            for schema in catalog.schemas:
                schema_matches = [pattern
                                  for pattern in addon.unlabeled_tables
                                  if pattern.matches(schema)]
                if not schema_matches:
                    continue
                for table in schema.tables:
                    matches = [pattern
                               for pattern in schema_matches
                               if pattern.matches(table)]
                    unused.difference_update(matches)

        if addon.unlabeled_columns:
            unused.update(addon.unlabeled_columns)
            for schema in catalog.schemas:
                schema_matches = [pattern
                                  for pattern in addon.unlabeled_columns
                                  if pattern.matches(schema)]
                if not schema_matches:
                    continue
                for table in schema.tables:
                    table_matches = [pattern
                                     for pattern in schema_matches
                                     if pattern.matches(table)]
                    if not table_matches:
                        continue
                    for column in table.columns:
                        matches = [pattern
                                   for pattern in table_matches
                                   if pattern.matches(column)]
                        unused.difference_update(matches)

        for pattern in (addon.included_tables + addon.excluded_tables +
                        addon.included_columns + addon.excluded_columns +
                        addon.not_nulls + addon.unique_keys +
                        addon.foreign_keys +
                        addon.unlabeled_tables + addon.unlabeled_columns):
            if pattern in unused:
                addon.unused_pattern_cache.add(str(pattern))

        return catalog


