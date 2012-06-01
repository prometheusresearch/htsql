#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from .context import context
from .cache import once
from .adapter import Adapter, adapt
from .model import (Node, Arc, Label, HomeNode, TableNode, TableArc, ChainArc,
                    ColumnArc, SyntaxArc, AmbiguousArc)
from .entity import DirectJoin, ReverseJoin
from .introspect import introspect
import re
import unicodedata


def normalize(name):
    """
    Normalizes a name to provide a valid HTSQL identifier.

    We assume `name` is a Unicode string.  Then it is:

    - translated to Unicode normal form C;
    - converted to lowercase;
    - has non-alphanumeric characters replaced with underscores;
    - preceded with an underscore if it starts with a digit.

    The result is a valid HTSQL identifier.
    """
    assert isinstance(name, unicode) and len(name) > 0
    name = unicodedata.normalize('NFC', name).lower()
    name = re.sub(ur"(?u)^(?=\d)|\W", u"_", name)
    return name


class Classify(Adapter):

    adapt(Node)

    def __init__(self, node):
        self.node = node

    def __call__(self):
        arcs = self.trace(self.node)
        bids_by_arc = {}
        for arc in arcs:
            bids_by_arc[arc] = self.call(arc)

        names_by_weight = {}
        arcs_by_bid = {}
        for arc in arcs:
            for bid in bids_by_arc[arc]:
                name, weight = bid
                names_by_weight.setdefault(weight, set()).add(name)
                arcs_by_bid.setdefault(bid, []).append(arc)

        arc_by_signature = {}
        name_by_arc = {}
        rejections_by_signature = {}

        for weight in sorted(names_by_weight, reverse=True):
            names = sorted(names_by_weight[weight],
                           key=(lambda name: (len(name), name)))
            for name in names:
                contenders_by_arity = {}
                for arc in arcs_by_bid[name, weight]:
                    contenders_by_arity.setdefault(arc.arity, []).append(arc)
                for arity in sorted(contenders_by_arity):
                    signature = (name, arity)
                    contenders = contenders_by_arity[arity]
                    if signature in arc_by_signature:
                        continue
                    if (len(contenders) > 1 or
                            signature in rejections_by_signature):
                        rejections_by_signature.setdefault(signature, [])
                        rejections_by_signature[signature].extend(contenders)
                        continue
                    [arc] = contenders
                    if arc in name_by_arc:
                        rejections_by_signature[signature] = [arc]
                        continue
                    arc_by_signature[signature] = arc
                    name_by_arc[arc] = name

        labels = []
        for arc in arcs:
            if arc not in name_by_arc:
                continue
            name = name_by_arc[arc]
            label = Label(name, arc, False)
            labels.append(label)
        for signature in sorted(rejections_by_signature):
            name, arity = signature
            alternatives = []
            duplicates = set()
            for arc in rejections_by_signature[signature]:
                if arc in duplicates:
                    continue
                alternatives.append(arc)
                duplicates.add(arc)
            arc = AmbiguousArc(arity, alternatives)
            label = Label(name, arc, False)
            labels.append(label)

        labels = self.order(labels)

        return labels

    def trace(self, node):
        arcs = []
        duplicates = set()
        for arc in Trace.__invoke__(node):
            if arc in duplicates:
                continue
            arcs.append(arc)
            duplicates.add(arc)
        return arcs

    def call(self, arc):
        bids = []
        duplicates = set()
        for name, weight in Call.__invoke__(arc):
            name = normalize(name)
            if (name, weight) in duplicates:
                continue
            bids.append((name, weight))
            duplicates.add((name, weight))
        return bids

    def order(self, labels):
        return Order.__invoke__(self.node, labels)


class Trace(Adapter):

    adapt(Node)

    def __init__(self, node):
        self.node = node

    def __call__(self):
        return []


class TraceHome(Trace):

    adapt(HomeNode)

    def __call__(self):
        catalog = introspect()
        for schema in catalog.schemas:
            for table in schema.tables:
                yield TableArc(table)


class TraceTable(Trace):

    adapt(TableNode)

    def __call__(self):
        table = self.node.table
        for column in table.columns:
            link = self.find_link(column)
            yield ColumnArc(table, column, link)
        for foreign_key in table.foreign_keys:
            join = DirectJoin(foreign_key)
            yield ChainArc(table, [join])
        for foreign_key in table.referring_foreign_keys:
            join = ReverseJoin(foreign_key)
            yield ChainArc(table, [join])

    def find_link(self, column):
        # Determines if the column may represents a link to another table.
        # This is the case when the column is associated with a foreign key.

        # Get a list of foreign keys associated with the given column.
        candidates = [foreign_key for foreign_key in column.foreign_keys
                                  if len(foreign_key.origin_columns) == 1]

        # Return immediately if there are no candidate keys.
        if not candidates:
            return None

        # Generate the joins corresponding to each alternative.
        alternatives = []
        for foreign_key in candidates:
            join = DirectJoin(foreign_key)
            arc = ChainArc(column.table, [join])
            alternatives.append(arc)
        # We got an unambiguous link if there's only one foreign key
        # associated with the column.
        if len(alternatives) == 1:
            return alternatives[0]
        else:
            return AmbiguousArc(None, alternatives)


class Call(Adapter):

    adapt(Arc)

    def __init__(self, arc):
        self.arc = arc

    def __call__(self):
        return []


class CallTable(Call):

    adapt(TableArc)

    def __call__(self):
        table = self.arc.table
        yield table.name, table.schema.priority
        if table.schema.name:
            name = u"%s %s" % (table.schema.name, table.name)
            yield name, -1


class CallColumn(Call):

    adapt(ColumnArc)

    def __call__(self):
        yield self.arc.column.name, 10


class CallChain(Call):

    adapt(ChainArc)

    path_word = u"via"

    def __call__(self):
        is_primary = True
        for join in self.arc.joins:
            foreign_key = join.foreign_key
            primary_key = foreign_key.origin.primary_key
            if primary_key is None:
                is_primary = False
                break
            if not all(column in primary_key.origin_columns
                       for column in foreign_key.origin_columns):
                is_primary = False
                break

        is_direct = all(join.is_direct for join in self.arc.joins)

        target = self.arc.target.table.name
        prefix = None
        column = None
        if len(self.arc.joins) == 1:
            foreign_key = join.foreign_key
            origin_name = foreign_key.origin_columns[-1].name
            target_name = foreign_key.target_columns[-1].name
            if origin_name.endswith(target_name):
                prefix = origin_name[:-len(target_name)].rstrip(u' _-')
                if not prefix:
                    prefix = target
            column = origin_name

        if is_direct and prefix:
            yield prefix, 5
        if is_primary:
            yield target, 4
        else:
            yield target, 3
        if not is_direct and prefix:
            name = u"%s %s %s" % (target, self.path_word, prefix)
            yield name, 2
        if not is_direct and column:
            name = u"%s %s %s" % (target, self.path_word, column)
            yield name, 1


class CallSyntax(Call):

    adapt(SyntaxArc)


class Order(Adapter):

    adapt(Node)

    def __init__(self, node, labels):
        self.node = node
        self.labels = labels

    def __call__(self):
        return self.labels


class OrderHome(Order):

    adapt(HomeNode)


class OrderTable(Order):

    adapt(TableNode)

    def __call__(self):
        return [label.clone(is_public=(label.is_public or
                                       isinstance(label.arc, ColumnArc)))
                for label in self.labels]


class Localize(Adapter):

    adapt(Node)

    def __init__(self, node):
        self.node = node

    def __call__(self):
        return None


class LocalizeTable(Localize):

    adapt(TableNode)

    def __call__(self):
        arcs = set()
        for label in classify(self.node):
            arc = label.arc
            if isinstance(arc, ColumnArc):
                arcs.add(arc)
                if arc.link is not None:
                    if isinstance(arc.link, ChainArc):
                        arcs.add(arc.link)
                    arc = arc.clone(link=None)
                    arcs.add(arc)
            elif isinstance(arc, ChainArc):
                arcs.add(arc)
        table = self.node.table
        for key in [table.primary_key]+table.unique_keys:
            if key is None:
                continue
            if key.is_partial:
                continue
            if not all(not column.is_nullable for column in key.origin_columns):
                continue
            columns = key.origin_columns[:]
            identity = []
            while columns:
                for foreign_key in self.node.table.foreign_keys:
                    if foreign_key.is_partial:
                        continue
                    width = len(foreign_key.origin_columns)
                    if foreign_key.origin_columns == columns[:width]:
                        join = DirectJoin(foreign_key)
                        arc = ChainArc(table, [join])
                        if arc not in arcs:
                            continue
                        if localize(arc.target) is None:
                            continue
                        identity.append(arc)
                        columns = columns[width:]
                        break
                else:
                    column = columns[0]
                    arc = ColumnArc(table, column)
                    if arc not in arcs:
                        break
                    identity.append(arc)
                    columns.pop(0)
            if not columns:
                return identity


@once
def classify(node):
    assert isinstance(node, Node)
    return Classify.__invoke__(node)


@once
def relabel(arc):
    assert isinstance(arc, Arc)
    cache = context.app.htsql.cache
    labels = classify(arc.origin)
    duplicates = set()
    labels_by_arc = {}
    labels_by_arc[arc] = []
    arcs = [arc]
    for label in labels:
        assert label.name not in duplicates, label
        duplicates.add(label.name)
        arc = label.arc
        if arc not in labels_by_arc:
            labels_by_arc[arc] = []
            arcs.append(arc)
        labels_by_arc[arc].append(label)
    for arc in arcs:
        key = (relabel.__module__, relabel.__name__, arc)
        value = labels_by_arc[arc]
        if key not in cache.values:
            cache.set(key, value)
    return labels_by_arc[arcs[0]]


@once
def localize(node):
    assert isinstance(node, Node)
    return Localize.__invoke__(node)


