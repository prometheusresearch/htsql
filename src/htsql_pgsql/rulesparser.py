import re
from htsql.entity import (PrimaryKeyEntity, ForeignKeyEntity)

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
        m = re.match('[^ )}]+', instr)
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


class SingleTableScenario(Scenario):

    def accepts(self, rule_tree):
        if len(rule_tree) > 1:
            return False

        query = rule_tree[0]
        assert query.__class__.__name__ == 'QUERY'
        if query.commandType != '1' \
                or query.groupClause != '<>' \
                or query.distinctClause != '<>' \
                or query.setOperations != '<>':
            return False

        if len(query.jointree.fromlist) != 1:
            return False

        return True

    def find_keys(self, rule_tree, view, tablemap):
        query = rule_tree[0]
        rtindex = int(query.jointree.fromlist[0].rtindex) - 1
        rtable = query.rtable[rtindex]
        o_table = tablemap[int(rtable.relid)]
        o_pkey = None
        for ukey in o_table.unique_keys:
            if ukey.is_primary:
                o_pkey = ukey
                break
        if o_pkey is None:
            return []

        view_columns = {}
        for target in query.targetList:
            if target.expr.__class__.__name__ == 'VAR':
                attindex = int(target.expr.varattno) - 1
                rcolname = rtable.eref.colnames[attindex].strip('"')
                vcolname = target.resname
                view_columns[rcolname] = vcolname

        v_colnames = []
        for pkey_colname in o_pkey.origin_column_names:
            try:
                vcolname = view_columns[pkey_colname]
                v_colnames.append(vcolname)
            except ValueError:
                return []
        v_pkey = PrimaryKeyEntity(view.schema_name, view.name, v_colnames)
        v_fkey = ForeignKeyEntity(view.schema_name, view.name, v_colnames,
                                  o_table.schema_name, o_table.name, o_pkey.origin_column_names)
        return [v_pkey, v_fkey]


scenario_list = [SingleTableScenario()]
