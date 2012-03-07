#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from htsql_pgsql.core.introspect import IntrospectPGSQL
from htsql.core.entity import PrimaryKeyEntity, ForeignKeyEntity, CatalogEntity
from . import rulesparser


class PGViewIntrospectPGSQL(IntrospectPGSQL):

    def introspect_catalog(self):
        schemas = self.introspect_schemas()
        self.introspect_views()
        return CatalogEntity(schemas)

    def introspect_views(self):
        for oid in self.meta.pg_rewrite:
            rule = self.meta.pg_rewrite[oid]
            if rule.ev_type != '1' \
                    or rule.ev_attr >= 0 \
                    or not rule.is_instead \
                    or rule.ev_qual != '<>':
                # not a view
                continue

            if not rule.ev_class in self.table_by_oid:
                # not introspected view
                continue

            view = self.table_by_oid[rule.ev_class]

            ruletree = rulesparser.RuleTreeParser().parse(rule.ev_action)
            for scenario in rulesparser.scenario_list:
                if scenario.accepts(ruletree):
                    keyset = scenario.find_keys(ruletree, view, self.table_by_oid)
                    for key in keyset:
                        if isinstance(key, PrimaryKeyEntity):
                            view.unique_keys.append(key)
                            view.primary_key = key
                        if isinstance(key, ForeignKeyEntity):
                            view.foreign_keys.append(key)
                    if len(keyset) > 0:
                        break


