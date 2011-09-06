import sys
from htsql import HTSQL
from htsql.request import produce
from htsql.tr.lookup import normalize, lookup_attribute, get_catalog
from htsql.tr.parse import parse
from htsql.tr.bind import bind
app = HTSQL("pgsql:htsql_regress")

#----------------
# build the graph
#

graph = {}

with app:
    # force introspection 
    catalog = get_catalog()
    reverse = {}

    # record all potential reverse links
    for schema in catalog.schemas:
        for table in schema.tables:
            tname = normalize(table.name)
            for fk in table.foreign_keys:
                fk_name = normalize(fk.target_name)
                reverse.setdefault(fk_name,[]).append(tname)
  
    # traverse links and build graph     
    for schema in catalog.schemas:
        for table in schema.tables:
            forward_links = []
            reverse_links = []
            tname = normalize(table.name)

            # create table binding to verify lookups
            query = "/%s" % normalize(table.name)
            syntax = parse(query)
            binding = bind(syntax)
            btable = binding.segment.seed
            
            # add foreign keys that lookup
            for fk in table.foreign_keys:
                # for parental links fk is subset of pk
                is_parental = False
                if table.primary_key:
                    spk = set(table.primary_key.origin_column_names)
                    sfk = set(fk.origin_column_names)
                    if sfk.issubset(spk):
                        is_parental = True
                 
                # probe the binding for the link name
                fk_name = normalize(fk.target_name)
                if lookup_attribute(btable, fk_name):
                    forward_links.append((fk_name, is_parental, fk_name))
                else:
                    if 1 == len(fk.origin_column_names):
                        oc_name = normalize(fk.origin_column_names[0])
                        if lookup_attribute(btable, oc_name):
                            forward_links.append((oc_name, is_parental, 
                                                  fk_name))

            # add reverse links that lookup
            for fk_name in reverse.get(tname,[]):
                if lookup_attribute(btable, fk_name):
                    reverse_links.append((fk_name, False, fk_name))

            graph[tname] = (set(forward_links), set(reverse_links))

#
# build the output JSON structure
# 

for table in graph.keys():
    print "var t_%s = graph.newNode({label: '%s'});" % (table, table)

for (table, (forward, reverse)) in graph.items():
    for (name, is_parent, target) in forward:
        style = '{}'
        if name != target:
            style = '{label:"%s"}' % name
        weight = 1
        if is_parent:
           weight = 2
        print '    graph.newEdge(t_%s, t_%s, {});' % (
                 table, target )
    for (name, is_parent, target) in reverse:
        style = '{}'
        weight = 1
        print '    graph.newEdge(t_%s, t_%s, {});' % (
                 table, target )


if 0:
	import yaml
	print yaml.dump(graph)
