#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.context import context
from htsql.cache import once
from htsql.introspect import introspect
from htsql.model import (HomeNode, TableNode, TableArc, ColumnArc, ChainArc,
                         SyntaxArc)
from htsql.classify import (classify, Trace, TraceHome, TraceTable,
                            Call, CallTable, CallColumn, CallChain, CallSyntax,
                            OrderTable)


class ClassCache(object):

    def __init__(self):
        self.names_by_arc = {}
        self.arc_by_name = {}
        addon = context.app.tweak.override
        catalog = introspect()
        node = HomeNode()
        for name in sorted(addon.class_labels):
            pattern = addon.class_labels[name]
            arc = pattern.extract(node)
            if arc is None:
                addon.unused_pattern_cache.add(str(pattern))
                continue
            self.names_by_arc.setdefault(arc, []).append(name)
            self.arc_by_name[name] = arc


class FieldCache(object):

    def __init__(self):
        self.names_by_arc_by_node = {}
        self.arc_by_name_by_node = {}
        self.node_by_name = {}
        self.name_by_node = {}
        addon = context.app.tweak.override
        for label in classify(HomeNode()):
            self.node_by_name[label.name] = label.target
            self.name_by_node[label.target] = label.name
        for class_name, field_name in sorted(addon.field_labels):
            name = u"%s.%s" % (class_name, field_name)
            pattern = addon.field_labels[class_name, field_name]
            if class_name not in self.node_by_name:
                addon.unused_pattern_cache.add(name.encode('utf-8'))
                continue
            node = self.node_by_name[class_name]
            arc = pattern.extract(node)
            if arc is None:
                addon.unused_pattern_cache.add(str(pattern))
                continue
            self.names_by_arc_by_node.setdefault(node, {})
            self.names_by_arc_by_node[node].setdefault(arc, [])
            self.names_by_arc_by_node[node][arc].append(field_name)
            self.arc_by_name_by_node.setdefault(node, {})
            self.arc_by_name_by_node[node][field_name] = arc


@once
def class_cache():
    return ClassCache()


@once
def field_cache():
    return FieldCache()


class OverrideTraceHome(TraceHome):

    def __call__(self):
        addon = context.app.tweak.override
        cache = class_cache()
        arcs = []
        arcs.extend(super(OverrideTraceHome, self).__call__())
        for name in sorted(cache.arc_by_name):
            arc = cache.arc_by_name[name]
            arcs.append(arc)
        for arc in arcs:
            if isinstance(arc, TableArc):
                if any(pattern.matches(arc.table)
                       for pattern in addon.unlabeled_tables):
                    continue
            yield arc


class OverrideTraceTable(TraceTable):

    def __call__(self):
        addon = context.app.tweak.override
        cache = field_cache()
        arcs = []
        arcs.extend(super(OverrideTraceTable, self).__call__())
        arc_by_name = cache.arc_by_name_by_node.get(self.node, {})
        for name in sorted(arc_by_name):
            arc = arc_by_name[name]
            arcs.append(arc)
        for arc in arcs:
            if isinstance(arc, ColumnArc):
                if any(pattern.matches(arc.column)
                       for pattern in addon.unlabeled_columns):
                    continue
            if isinstance(arc, ChainArc):
                if any(pattern.matches(arc.target.table)
                       for pattern in addon.unlabeled_tables):
                    continue
            yield arc


class OverrideCallTable(CallTable):

    def __call__(self):
        cache = class_cache()
        names = cache.names_by_arc.get(self.arc)
        if names is not None:
            for name in cache.names_by_arc[self.arc]:
                yield name, 20
            return
        for name, weight in super(OverrideCallTable, self).__call__():
            yield name, weight


class OverrideCallColumn(CallColumn):

    def __call__(self):
        cache = field_cache()
        names_by_arc = cache.names_by_arc_by_node.get(self.arc.origin, {})
        names = names_by_arc.get(self.arc)
        if names is not None:
            for name in names:
                yield name, 20
            return
        for name, weight in super(OverrideCallColumn, self).__call__():
            yield name, weight


class OverrideCallChain(CallChain):

    def __call__(self):
        cache = field_cache()
        names_by_arc = cache.names_by_arc_by_node.get(self.arc.origin, {})
        names = names_by_arc.get(self.arc)
        if names is not None:
            for name in names:
                yield name, 20
            return
        for name, weight in super(OverrideCallChain, self).__call__():
            yield name, weight


class OverrideCallSyntax(CallSyntax):

    def __call__(self):
        if isinstance(self.arc.origin, HomeNode):
            cache = class_cache()
            names = cache.names_by_arc.get(self.arc)
            if names is not None:
                for name in cache.names_by_arc[self.arc]:
                    yield name, 20
                return
        elif isinstance(self.arc.origin, TableNode):
            cache = field_cache()
            names_by_arc = cache.names_by_arc_by_node.get(self.arc.origin, {})
            names = names_by_arc.get(self.arc)
            if names is not None:
                for name in names:
                    yield name, 20
                return
        for name, weight in super(OverrideCallSyntax, self).__call__():
            yield name, weight


class OverrideOrderTable(OrderTable):

    def __call__(self):
        addon = context.app.tweak.override
        cache = field_cache()
        class_name = cache.name_by_node.get(self.node)
        if class_name not in addon.field_orders:
            return super(OverrideOrderTable, self).__call__()
        names = set(label.name for label in self.labels)
        orders = {}
        for idx, name in enumerate(addon.field_orders[class_name]):
            orders[name] = idx
            if name not in names:
                name = u"%s.%s" % (class_name, name)
                addon.unused_pattern_cache.add(name.encode('utf-8'))
        labels = [label.clone(is_public=(label.name in orders))
                  for label in self.labels]
        labels.sort(key=(lambda label: (label.name not in orders,
                                        orders.get(label.name, 0))))
        return labels


@once
def validate():
    addon = context.app.tweak.override
    cache = field_cache()
    for name in sorted(addon.field_orders):
        if name not in cache.node_by_name:
            addon.unused_pattern_cache.add(name.encode('utf-8'))
            continue
        node = cache.node_by_name[name]
        classify(node)


