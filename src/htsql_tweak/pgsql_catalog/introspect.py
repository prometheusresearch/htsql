#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


from htsql_pgsql.introspect import IntrospectPGSQL
from htsql.entity import PrimaryKeyEntity, ForeignKeyEntity


class PGCatalogIntrospectPGSQL(IntrospectPGSQL):

    primary_keys = {
            ('pg_catalog', 'pg_aggregate'): ['aggfnoid'],
            ('pg_catalog', 'pg_am'): ['oid'],
            ('pg_catalog', 'pg_amop'): ['oid'],
            ('pg_catalog', 'pg_amproc'): ['oid'],
            ('pg_catalog', 'pg_attrdef'): ['oid'],
            ('pg_catalog', 'pg_attribute'): ['attrelid', 'attnum'],
            ('pg_catalog', 'pg_authid'): ['oid'],
            ('pg_catalog', 'pg_auth_members'): ['roleid', 'member'],
            ('pg_catalog', 'pg_cast'): ['oid'],
            ('pg_catalog', 'pg_class'): ['oid'],
            ('pg_catalog', 'pg_constraint'): ['oid'],
            ('pg_catalog', 'pg_conversion'): ['oid'],
            ('pg_catalog', 'pg_database'): ['oid'],
            ('pg_catalog', 'pg_depend'):
                    ['classid', 'objid', 'objsubid',
                     'refclassid', 'refobjid', 'refobjsubid'],
            ('pg_catalog', 'pg_description'):
                    ['objoid', 'classoid', 'objsubid'],
            ('pg_catalog', 'pg_enum'): ['oid'],
            ('pg_catalog', 'pg_foreign_data_wrapper'): ['oid'],
            ('pg_catalog', 'pg_foreign_server'): ['oid'],
            ('pg_catalog', 'pg_index'): ['indexrelid'],
            ('pg_catalog', 'pg_inherits'): ['inhrelid', 'inhparent'],
            ('pg_catalog', 'pg_language'): ['oid'],
            ('pg_catalog', 'pg_largeobject'): ['loid', 'pageno'],
            ('pg_catalog', 'pg_listener'): ['relname', 'listenerpid'],
            ('pg_catalog', 'pg_namespace'): ['oid'],
            ('pg_catalog', 'pg_opclass'): ['oid'],
            ('pg_catalog', 'pg_operator'): ['oid'],
            ('pg_catalog', 'pg_opfamily'): ['oid'],
            ('pg_catalog', 'pg_pltemplate'): ['tmplname'],
            ('pg_catalog', 'pg_proc'): ['oid'],
            ('pg_catalog', 'pg_rewrite'): ['oid'],
            ('pg_catalog', 'pg_shdepend'):
                    ['dbid', 'classid', 'objid', 'objsubid',
                     'refclassid', 'refobjid'],
            ('pg_catalog', 'pg_shdescription'): ['objoid', 'classoid'],
            ('pg_catalog', 'pg_statistic'): ['starelid', 'staattnum'],
            ('pg_catalog', 'pg_tablespace'): ['oid'],
            ('pg_catalog', 'pg_trigger'): ['oid'],
            ('pg_catalog', 'pg_ts_config'): ['oid'],
            ('pg_catalog', 'pg_ts_config_map'):
                    ['mapcfg', 'maptokentype', 'mapseqno'],
            ('pg_catalog', 'pg_ts_dict'): ['oid'],
            ('pg_catalog', 'pg_ts_parser'): ['oid'],
            ('pg_catalog', 'pg_ts_template'): ['oid'],
            ('pg_catalog', 'pg_type'): ['oid'],
            ('pg_catalog', 'pg_user_mapping'): ['oid'],
    }

    foreign_keys = {
            ('pg_catalog', 'pg_aggregate'): [
                (['aggfnoid'], ('pg_catalog', 'pg_proc'), ['oid']),
                (['aggtransfn'], ('pg_catalog', 'pg_proc'), ['oid']),
                (['aggfinalfn'], ('pg_catalog', 'pg_proc'), ['oid']),
                (['aggsortop'], ('pg_catalog', 'pg_operator'), ['oid']),
                (['aggtranstype'], ('pg_catalog', 'pg_type'), ['oid']),
            ],
            ('pg_catalog', 'pg_am'): [
                (['amkeytype'], ('pg_catalog', 'pg_type'), ['oid']),
                (['aminsert'], ('pg_catalog', 'pg_proc'), ['oid']),
                (['ambeginscan'], ('pg_catalog', 'pg_proc'), ['oid']),
                (['amgettuple'], ('pg_catalog', 'pg_proc'), ['oid']),
                (['amgetbitmap'], ('pg_catalog', 'pg_proc'), ['oid']),
                (['amrescan'], ('pg_catalog', 'pg_proc'), ['oid']),
                (['amendscan'], ('pg_catalog', 'pg_proc'), ['oid']),
                (['ammarkpos'], ('pg_catalog', 'pg_proc'), ['oid']),
                (['amrestrpos'], ('pg_catalog', 'pg_proc'), ['oid']),
                (['ambuild'], ('pg_catalog', 'pg_proc'), ['oid']),
                (['ambulkdelete'], ('pg_catalog', 'pg_proc'), ['oid']),
                (['amvacuumcleanup'], ('pg_catalog', 'pg_proc'), ['oid']),
                (['amcostestimate'], ('pg_catalog', 'pg_proc'), ['oid']),
                (['amoptions'], ('pg_catalog', 'pg_proc'), ['oid']),
            ],
            ('pg_catalog', 'pg_amop'): [
                (['amopfamily'], ('pg_catalog', 'pg_opfamily'), ['oid']),
                (['amoplefttype'], ('pg_catalog', 'pg_type'), ['oid']),
                (['amoprighttype'], ('pg_catalog', 'pg_type'), ['oid']),
                (['amopopr'], ('pg_catalog', 'pg_operator'), ['oid']),
                (['amopmethod'], ('pg_catalog', 'pg_am'), ['oid']),
            ],
            ('pg_catalog', 'pg_amproc'): [
                (['amprocfamily'], ('pg_catalog', 'pg_opfamily'), ['oid']),
                (['amproclefttype'], ('pg_catalog', 'pg_type'), ['oid']),
                (['amprocrighttype'], ('pg_catalog', 'pg_type'), ['oid']),
                (['amproc'], ('pg_catalog', 'pg_proc'), ['oid']),
            ],
            ('pg_catalog', 'pg_attrdef'): [
                (['adrelid'], ('pg_catalog', 'pg_class'), ['oid']),
                (['adrelid', 'adnum'], ('pg_catalog', 'pg_attribute'),
                    ['attrelid', 'attnum']),
            ],
            ('pg_catalog', 'pg_attribute'): [
                (['attrelid'], ('pg_catalog', 'pg_class'), ['oid']),
                (['atttypid'], ('pg_catalog', 'pg_type'), ['oid']),
            ],
            ('pg_catalog', 'pg_auth_members'): [
                (['roleid'], ('pg_catalog', 'pg_authid'), ['oid']),
                (['member'], ('pg_catalog', 'pg_authid'), ['oid']),
                (['grantor'], ('pg_catalog', 'pg_authid'), ['oid']),
            ],
            ('pg_catalog', 'pg_cast'): [
                (['castsource'], ('pg_catalog', 'pg_type'), ['oid']),
                (['casttarget'], ('pg_catalog', 'pg_type'), ['oid']),
                (['castfunc'], ('pg_catalog', 'pg_proc'), ['oid']),
            ],
            ('pg_catalog', 'pg_class'): [
                (['relnamespace'], ('pg_catalog', 'pg_namespace'), ['oid']),
                (['reltype'], ('pg_catalog', 'pg_type'), ['oid']),
                (['relowner'], ('pg_catalog', 'pg_authid'), ['oid']),
                (['relam'], ('pg_catalog', 'pg_am'), ['oid']),
                (['reltablespace'], ('pg_catalog', 'pg_tablespace'), ['oid']),
                (['reltoastrelid'], ('pg_catalog', 'pg_class'), ['oid']),
                (['reltoastidxid'], ('pg_catalog', 'pg_class'), ['oid']),
            ],
            ('pg_catalog', 'pg_constraint'): [
                (['connamespace'], ('pg_catalog', 'pg_namespace'), ['oid']),
                (['conrelid'], ('pg_catalog', 'pg_class'), ['oid']),
                (['contypid'], ('pg_catalog', 'pg_type'), ['oid']),
                (['confrelid'], ('pg_catalog', 'pg_class'), ['oid']),
            ],
            ('pg_catalog', 'pg_conversion'): [
                (['connamespace'], ('pg_catalog', 'pg_namespace'), ['oid']),
                (['conowner'], ('pg_catalog', 'pg_authid'), ['oid']),
                (['conproc'], ('pg_catalog', 'pg_proc'), ['oid']),
            ],
            ('pg_catalog', 'pg_database'): [
                (['datdba'], ('pg_catalog', 'pg_authid'), ['oid']),
                (['dattablespace'], ('pg_catalog', 'pg_tablespace'), ['oid']),
            ],
            ('pg_catalog', 'pg_depend'): [
                (['classid'], ('pg_catalog', 'pg_class'), ['oid']),
                (['refclassid'], ('pg_catalog', 'pg_class'), ['oid']),
            ],
            ('pg_catalog', 'pg_description'): [
                (['classoid'], ('pg_catalog', 'pg_class'), ['oid']),
            ],
            ('pg_catalog', 'pg_enum'): [
                (['enumtypid'], ('pg_catalog', 'pg_type'), ['oid']),
            ],
            ('pg_catalog', 'pg_foreign_data_wrapper'): [
                (['fdwowner'], ('pg_catalog', 'pg_authid'), ['oid']),
                (['fdwvalidator'], ('pg_catalog', 'pg_proc'), ['oid']),
            ],
            ('pg_catalog', 'pg_foreign_server'): [
                (['srvowner'], ('pg_catalog', 'pg_authid'), ['oid']),
                (['srvfdw'], ('pg_catalog', 'pg_foreign_data_wrapper'), ['oid']),
            ],
            ('pg_catalog', 'pg_index'): [
                (['indexrelid'], ('pg_catalog', 'pg_class'), ['oid']),
                (['indrelid'], ('pg_catalog', 'pg_class'), ['oid']),
            ],
            ('pg_catalog', 'pg_inherits'): [
                (['inhrelid'], ('pg_catalog', 'pg_class'), ['oid']),
                (['inhparent'], ('pg_catalog', 'pg_class'), ['oid']),
            ],
            ('pg_catalog', 'pg_language'): [
                (['lanowner'], ('pg_catalog', 'pg_authid'), ['oid']),
                (['lanplcallfoid'], ('pg_catalog', 'pg_proc'), ['oid']),
                (['lanvalidator'], ('pg_catalog', 'pg_proc'), ['oid']),
            ],
            ('pg_catalog', 'pg_namespace'): [
                (['nspowner'], ('pg_catalog', 'pg_authid'), ['oid']),
            ],
            ('pg_catalog', 'pg_opclass'): [
                (['opcmethod'], ('pg_catalog', 'pg_am'), ['oid']),
                (['opcnamespace'], ('pg_catalog', 'pg_namespace'), ['oid']),
                (['opcowner'], ('pg_catalog', 'pg_authid'), ['oid']),
                (['opcfamily'], ('pg_catalog', 'pg_opfamily'), ['oid']),
                (['opcintype'], ('pg_catalog', 'pg_type'), ['oid']),
                (['opckeytype'], ('pg_catalog', 'pg_type'), ['oid']),
            ],
            ('pg_catalog', 'pg_operator'): [
                (['oprnamespace'], ('pg_catalog', 'pg_namespace'), ['oid']),
                (['oprowner'], ('pg_catalog', 'pg_authid'), ['oid']),
                (['oprleft'], ('pg_catalog', 'pg_type'), ['oid']),
                (['oprright'], ('pg_catalog', 'pg_type'), ['oid']),
                (['oprresult'], ('pg_catalog', 'pg_type'), ['oid']),
                (['oprcom'], ('pg_catalog', 'pg_operator'), ['oid']),
                (['oprnegate'], ('pg_catalog', 'pg_operator'), ['oid']),
                (['oprcode'], ('pg_catalog', 'pg_proc'), ['oid']),
                (['oprrest'], ('pg_catalog', 'pg_proc'), ['oid']),
                (['oprjoin'], ('pg_catalog', 'pg_proc'), ['oid']),
            ],
            ('pg_catalog', 'pg_opfamily'): [
                (['opfmethod'], ('pg_catalog', 'pg_am'), ['oid']),
                (['opfnamespace'], ('pg_catalog', 'pg_namespace'), ['oid']),
                (['opfowner'], ('pg_catalog', 'pg_authid'), ['oid']),
            ],
            ('pg_catalog', 'pg_proc'): [
                (['pronamespace'], ('pg_catalog', 'pg_namespace'), ['oid']),
                (['proowner'], ('pg_catalog', 'pg_authid'), ['oid']),
                (['prolang'], ('pg_catalog', 'pg_language'), ['oid']),
                (['provariadic'], ('pg_catalog', 'pg_type'), ['oid']),
                (['prorettype'], ('pg_catalog', 'pg_type'), ['oid']),
            ],
            ('pg_catalog', 'pg_rewrite'): [
                (['ev_class'], ('pg_catalog', 'pg_class'), ['oid']),
            ],
            ('pg_catalog', 'pg_shdepend'): [
                (['dbid'], ('pg_catalog', 'pg_database'), ['oid']),
                (['classid'], ('pg_catalog', 'pg_class'), ['oid']),
                (['refclassid'], ('pg_catalog', 'pg_class'), ['oid']),
            ],
            ('pg_catalog', 'pg_shdescription'): [
                (['classoid'], ('pg_catalog', 'pg_class'), ['oid']),
            ],
            ('pg_catalog', 'pg_statistic'): [
                (['starelid'], ('pg_catalog', 'pg_class'), ['oid']),
                (['starelid', 'staattnum'], ('pg_catalog', 'pg_attribute'),
                    ['attrelid', 'attnum']),
            ],
            ('pg_catalog', 'pg_tablespace'): [
                (['spcowner'], ('pg_catalog', 'pg_authid'), ['oid']),
            ],
            ('pg_catalog', 'pg_trigger'): [
                (['tgrelid'], ('pg_catalog', 'pg_class'), ['oid']),
                (['tgfoid'], ('pg_catalog', 'pg_proc'), ['oid']),
                (['tgconstrrelid'], ('pg_catalog', 'pg_class'), ['oid']),
                (['tgconstraint'], ('pg_catalog', 'pg_constraint'), ['oid']),
            ],
            ('pg_catalog', 'pg_ts_config'): [
                (['cfgnamespace'], ('pg_catalog', 'pg_namespace'), ['oid']),
                (['cfgowner'], ('pg_catalog', 'pg_authid'), ['oid']),
                (['cfgparser'], ('pg_catalog', 'pg_ts_parser'), ['oid']),
            ],
            ('pg_catalog', 'pg_ts_config_map'): [
                (['mapcfg'], ('pg_catalog', 'pg_ts_config'), ['oid']),
                (['mapdict'], ('pg_catalog', 'pg_ts_dict'), ['oid']),
            ],
            ('pg_catalog', 'pg_ts_dict'): [
                (['dictnamespace'], ('pg_catalog', 'pg_namespace'), ['oid']),
                (['dictowner'], ('pg_catalog', 'pg_authid'), ['oid']),
                (['dicttemplate'], ('pg_catalog', 'pg_ts_template'), ['oid']),
            ],
            ('pg_catalog', 'pg_ts_parser'): [
                (['prsnamespace'], ('pg_catalog', 'pg_namespace'), ['oid']),
                (['prsstart'], ('pg_catalog', 'pg_proc'), ['oid']),
                (['prstoken'], ('pg_catalog', 'pg_proc'), ['oid']),
                (['prsend'], ('pg_catalog', 'pg_proc'), ['oid']),
                (['prsheadline'], ('pg_catalog', 'pg_proc'), ['oid']),
                (['prslextype'], ('pg_catalog', 'pg_proc'), ['oid']),
            ],
            ('pg_catalog', 'pg_ts_template'): [
                (['tmplnamespace'], ('pg_catalog', 'pg_namespace'), ['oid']),
                (['tmplinit'], ('pg_catalog', 'pg_proc'), ['oid']),
                (['tmpllexize'], ('pg_catalog', 'pg_proc'), ['oid']),
            ],
            ('pg_catalog', 'pg_type'): [
                (['typnamespace'], ('pg_catalog', 'pg_namespace'), ['oid']),
                (['typowner'], ('pg_catalog', 'pg_authid'), ['oid']),
                (['typrelid'], ('pg_catalog', 'pg_class'), ['oid']),
                (['typelem'], ('pg_catalog', 'pg_type'), ['oid']),
                (['typarray'], ('pg_catalog', 'pg_type'), ['oid']),
                (['typinput'], ('pg_catalog', 'pg_proc'), ['oid']),
                (['typoutput'], ('pg_catalog', 'pg_proc'), ['oid']),
                (['typreceive'], ('pg_catalog', 'pg_proc'), ['oid']),
                (['typsend'], ('pg_catalog', 'pg_proc'), ['oid']),
                (['typmodin'], ('pg_catalog', 'pg_proc'), ['oid']),
                (['typmodout'], ('pg_catalog', 'pg_proc'), ['oid']),
                (['typanalyze'], ('pg_catalog', 'pg_proc'), ['oid']),
                (['typbasetype'], ('pg_catalog', 'pg_type'), ['oid']),
            ],
            ('pg_catalog', 'pg_user_mapping'): [
                (['umuser'], ('pg_catalog', 'pg_authid'), ['oid']),
                (['umserver'], ('pg_catalog', 'pg_foreign_server'), ['oid']),
            ],

    }

    def permit_schema(self, schema_name):
        if schema_name == 'pg_catalog':
            return True
        return super(PGCatalogIntrospectPGSQL, self).permit_schema(schema_name)

    def introspect_unique_keys(self, table_oid):
        unique_keys = (super(PGCatalogIntrospectPGSQL, self)
                        .introspect_unique_keys(table_oid))
        rel = self.meta.pg_class[table_oid]
        schema_name = self.meta.pg_namespace[rel.relnamespace].nspname
        table_name = rel.relname
        if (schema_name, table_name) in self.primary_keys:
            column_names = self.primary_keys[schema_name, table_name]
            unique_key = PrimaryKeyEntity(schema_name, table_name,
                                          column_names)
            unique_keys.append(unique_key)
        return unique_keys

    def introspect_foreign_keys(self, table_oid):
        foreign_keys = (super(PGCatalogIntrospectPGSQL, self)
                        .introspect_foreign_keys(table_oid))
        rel = self.meta.pg_class[table_oid]
        schema_name = self.meta.pg_namespace[rel.relnamespace].nspname
        table_name = rel.relname
        if (schema_name, table_name) in self.foreign_keys:
            for key in self.foreign_keys[schema_name, table_name]:
                column_names, target_name, target_column_names = key
                target_schema_name, target_table_name = target_name
                foreign_key = ForeignKeyEntity(schema_name, table_name,
                                               column_names,
                                               target_schema_name,
                                               target_table_name,
                                               target_column_names)
                foreign_keys.append(foreign_key)
        return foreign_keys


