import re
from htsql.core.entity import PrimaryKeyEntity, ForeignKeyEntity

class RTEKind(object):
    """
    Range table enumeration
    """
    RTE_RELATION = 0			# ordinary relation reference
    RTE_SUBQUERY = 1			# subquery in FROM
    RTE_JOIN = 2				# join
    RTE_SPECIAL = 3				# special rule relation (NEW or OLD)
    RTE_FUNCTION = 4			# function in FROM
    RTE_VALUES = 5				# VALUES (<exprlist>), (<exprlist>), ...
    RTE_CTE = 6					# common table expr (WITH list element)


class RuleTreeParser(object):

    def __init__(self):
        pass

    def parse(self, instr):
        (item, instr) = self.parse_list(instr)
        return item

    def parse_list(self, instr):
        assert instr[0] == '(', '( expected, but met ' + instr[0]

        instr = instr[1:]
        res = []
        while instr[0] != ')':
            (item, instr) = self.parse_value(instr)
            res.append(item)
            instr = instr.strip()

        return res, instr[1:]

    def parse_object(self, instr):
        assert instr[0] == '{', '{ expected, but met ' + instr[0]

        instr = instr[1:]
        fields = {}
        (classname, instr) = self.parse_token(instr)
        instr = instr.strip()

        while instr[0] != '}':
            (field, instr) = self.parse_field(instr)
            (value, instr) = self.parse_value(instr.strip())
            instr = instr.strip()
            fields[field] = value

        t = type(classname, (object,), fields)

        return t(), instr[1:]

    def parse_field(self, instr):
        m = re.match(':([^ )}]+)', instr)
        return m.group(1), instr[m.end(1):]

    def parse_token(self, instr):
        m = re.match('(\\\\.|[^ )}])+', instr)
        return m.group(0), instr[m.end(0):]

    def parse_value(self, instr):
        if instr[0] == '(':
            return self.parse_list(instr)
        elif instr[0] == '{':
            return self.parse_object(instr)
        else:
            # match array
            m = re.match('\d+ \[.*?\]', instr)
            if m is not None:
                return m.group(0), instr[m.end(0):]

            return self.parse_token(instr)


class Scenario(object):

    def accepts(self, rule_tree):
        pass

    def find_keys(self, rule_tree, view, tablemap):
        pass


class SingleTableIdScenario(Scenario):

    def accepts(self, rule_tree):
        if len(rule_tree) > 1:
            return False

        query = rule_tree[0]
        assert query.__class__.__name__ == 'QUERY', query.__class__.__name__
        if query.commandType != '1' \
                or query.rtable == '<>' \
                or query.groupClause != '<>' \
                or query.distinctClause != '<>' \
                or query.setOperations != '<>':
            return False

        return self.find_rtable(query) is not None

    def find_rtable(self, query):
        if len(query.jointree.fromlist) != 1:
            return None
        tableref = query.jointree.fromlist[0]
        if tableref.__class__.__name__ != 'RANGETBLREF':
            return None
        if query.rtable == '<>':
            return None
        rtindex = int(tableref.rtindex) - 1
        rtable = query.rtable[rtindex]

        if int(rtable.rtekind) == RTEKind.RTE_RELATION:
            return rtable
        if int(rtable.rtekind) == RTEKind.RTE_SUBQUERY:
            return self.find_rtable(rtable.subquery)
        return None

    def find_keys(self, rule_tree, view, tablemap):
        query = rule_tree[0]
        rtable = self.find_rtable(query)
        relid = int(rtable.relid)
        if relid not in tablemap:
            # not introspected table
            return []
        o_table = tablemap[relid]
        o_pkey = None
        for ukey in o_table.unique_keys:
            if ukey.is_primary:
                o_pkey = ukey
                break
        if o_pkey is None:
            return []

        view_columns = {}
        for target in query.targetList:
            if target.expr.__class__.__name__ == 'VAR' \
                    and target.resorigtbl != '0' \
                    and target.resjunk != 'true':
                assert rtable.relid == target.resorigtbl
                attindex = int(target.resorigcol) - 1
                rcolname = rtable.eref.colnames[attindex].strip('"')
                vcolname = target.resname
                view_columns[rcolname] = vcolname

        v_colnames = []
        for pkey_colname in o_pkey.origin_column_names:
            try:
                vcolname = view_columns[pkey_colname]
                v_colnames.append(vcolname)
            except KeyError:
                return []
        v_pkey = PrimaryKeyEntity(view.schema_name, view.name, v_colnames)
        v_fkey = ForeignKeyEntity(view.schema_name, view.name, v_colnames,
                                  o_table.schema_name, o_table.name, o_pkey.origin_column_names)
        return [v_pkey, v_fkey]

class GroupByScenario(Scenario):

    def accepts(self, rule_tree):
        if len(rule_tree) > 1:
            return False

        query = rule_tree[0]
        assert query.__class__.__name__ == 'QUERY', query.__class__.__name__
        if query.commandType != '1' \
                or query.rtable == '<>' \
                or query.distinctClause != '<>' \
                or query.setOperations != '<>':
            return False

        return query.hasAggs == 'true'

    def find_keys(self, rule_tree, view, tablemap):
        query = rule_tree[0]
        v_colnames = []
        for target in query.targetList:
            if target.expr.__class__.__name__ == 'VAR' \
                    and target.ressortgroupref != '0' \
                    and target.resjunk != 'true':
                v_colnames.append(target.resname)

        if len(v_colnames) != len(query.groupClause):
            return []

        return [PrimaryKeyEntity(view.schema_name, view.name, v_colnames)]

class FKColumnRef(object):

    def __init__(self, schema_name, table_name, column_name, key):
        self.schema_name = schema_name
        self.table_name = table_name
        self.column_name = column_name
        self.key = key
        self.alias = None

class SelectFKScenario(Scenario):

    def accepts(self, rule_tree):
        if len(rule_tree) > 1:
            return False

        query = rule_tree[0]
        assert query.__class__.__name__ == 'QUERY', query.__class__.__name__
        if query.commandType != '1' \
                or query.rtable == '<>':
            return False

        return True

    def find_rtables(self, query):
        """
        Find all relations in the query and return in a map by oid.
        """
        result = {}
        if query.rtable == '<>':
            return result
        for rtable in query.rtable:
            if int(rtable.rtekind) == RTEKind.RTE_RELATION:
                result[rtable.relid] = rtable
            elif int(rtable.rtekind) == RTEKind.RTE_SUBQUERY:
                result.update(self.find_rtables(rtable.subquery))
        return result

    def get_key_column(self, table_entity, column_name):
        """
        Returns tuple of (schema-name, table-name, column-name, key) of the primary key column
        referenced by specified column.
        """
        if table_entity.primary_key is not None:
            if column_name in table_entity.primary_key.origin_column_names:
                return FKColumnRef(table_entity.schema_name, \
                           table_entity.name, \
                           column_name, \
                           table_entity.primary_key)
        for fkey in table_entity.foreign_keys:
            if column_name in fkey.origin_column_names:
                index = fkey.origin_column_names.index(column_name)
                return FKColumnRef(fkey.target_schema_name, \
                           fkey.target_name, \
                           fkey.target_column_names[index], \
                           fkey)
        return None

    def find_target_keys(self, query, rtablemap, tablemap):
        result = []
        for target in query.targetList:
            if target.resjunk != 'true':
                item = None
                if target.expr.__class__.__name__ == 'VAR' \
                        and target.resorigtbl != '0' \
                        and int(target.resorigtbl) in tablemap :
                    colname = rtablemap[target.resorigtbl].eref.colnames[int(target.resorigcol) - 1].strip('"')
                    table_entity = tablemap[int(target.resorigtbl)]
                    item = self.get_key_column(table_entity, colname)
                    if item is not None:
                        item.alias = target.resname
                result.append(item)
        return result

    def find_setoparg_keys(self, query, setoparg, rtablemap, tablemap):
        if setoparg.__class__.__name__ == 'RANGETBLREF':
            rel1 = query.rtable[int(setoparg.rtindex) - 1]
            assert int(rel1.rtekind) == RTEKind.RTE_SUBQUERY
            return self.find_query_keys(rel1.subquery, rtablemap, tablemap)
        elif setoparg.__class__.__name__ == 'SETOPERATIONSTMT':
            return self.find_setop_keys(query, setoparg, rtablemap, tablemap)

    def find_setop_keys(self, query, setop, rtablemap, tablemap):
        assert query.setOperations.__class__.__name__ == 'SETOPERATIONSTMT'
        candidates1 = self.find_setoparg_keys(query, setop.larg, rtablemap, tablemap)
        candidates2 = self.find_setoparg_keys(query, setop.rarg, rtablemap, tablemap)
        assert len(candidates1) == len(candidates2)
        result = []
        for c1, c2 in zip(candidates1, candidates2):
            item = None
            if c1 is not None and c2 is not None:
                if c1.schema_name == c2.schema_name \
                        and c1.table_name == c2.table_name \
                        and c1.column_name == c2.column_name:
                    # which key - doesn't matter
                    item = c1

            result.append(item)
        return result

    def find_query_keys(self, query, rtablemap, tablemap):
        if query.setOperations == '<>':
            return self.find_target_keys(query, rtablemap, tablemap)
        else:
            assert query.setOperations.__class__.__name__ == 'SETOPERATIONSTMT'
            return self.find_setop_keys(query, query.setOperations, rtablemap, tablemap)

    def find_keys(self, rule_tree, view, tablemap):
        query = rule_tree[0]
        candidates = self.find_query_keys(query, self.find_rtables(query), tablemap)
        keys = []
        schemas = {}
        for c in candidates:
            if c is not None:
                if c.schema_name not in schemas:
                    schemas[c.schema_name] = {}
                tables = schemas[c.schema_name]
                if c.table_name not in tables:
                    tables[c.table_name] = {}
                columns = tables[c.table_name]
                if c.column_name not in columns:
                    columns[c.column_name] = c.alias
                if c.key not in keys:
                    keys.append(c.key)

        result = []
        for key in keys:
            if isinstance(key, PrimaryKeyEntity):
                ref_schema = key.origin_schema_name
                ref_table = key.origin_name
                ref_columns = key.origin_column_names
            elif isinstance(key, ForeignKeyEntity):
                ref_schema = key.target_schema_name
                ref_table = key.target_name
                ref_columns = key.target_column_names
            else:
                continue
            fkey_columns = []
            selected_columns = schemas[ref_schema][ref_table]
            for pkey_column in ref_columns:
                if pkey_column in selected_columns:
                    fkey_columns.append(selected_columns[pkey_column])
            if len(ref_columns) == len(fkey_columns):
                result.append(ForeignKeyEntity(view.schema_name, view.name, fkey_columns,
                                               ref_schema, ref_table, ref_columns))
        return result


scenario_list = [SingleTableIdScenario(), GroupByScenario(), SelectFKScenario()]
