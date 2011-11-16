#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from .util import listof
from .context import context
from .cache import once
from .adapter import Adapter, adapts
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

    adapts(Node)

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

        arc_by_name = {}
        name_by_arc = {}
        rejections_by_name = {}

        for weight in sorted(names_by_weight, reverse=True):
            names = sorted(names_by_weight[weight],
                           key=(lambda name: (len(name), name)))
            for name in names:
                contenders = arcs_by_bid[name, weight]
                if name in arc_by_name:
                    continue
                if len(contenders) > 1 or name in rejections_by_name:
                    rejections_by_name.setdefault(name, []).extend(contenders)
                    continue
                [arc] = contenders
                if arc in name_by_arc:
                    rejections_by_name[name] = [arc]
                    continue
                arc_by_name[name] = arc
                name_by_arc[arc] = name

        labels = []
        for arc in arcs:
            if arc not in name_by_arc:
                continue
            name = name_by_arc[arc]
            label = Label(name, arc, False)
            labels.append(label)
        for name in sorted(rejections_by_name):
            alternatives = []
            duplicates = set()
            for arc in rejections_by_name[name]:
                if arc in duplicates:
                    continue
                alternatives.append(arc)
                duplicates.add(arc)
            arc = AmbiguousArc(alternatives)
            label = Label(name, arc, False)
            labels.append(label)

        labels = self.order(labels)

        return labels

    def trace(self, node):
        trace = Trace(node)
        arcs = []
        duplicates = set()
        for arc in trace():
            if arc in duplicates:
                continue
            arcs.append(arc)
            duplicates.add(arc)
        return arcs

    def call(self, arc):
        call = Call(arc)
        bids = []
        duplicates = set()
        for name, weight in call():
            name = normalize(name)
            if (name, weight) in duplicates:
                continue
            bids.append((name, weight))
            duplicates.add((name, weight))
        return bids

    def order(self, labels):
        order = Order(self.node, labels)
        return order()


class Trace(Adapter):

    adapts(Node)

    def __init__(self, node):
        self.node = node

    def __call__(self):
        return []


class TraceHome(Trace):

    adapts(HomeNode)

    def __call__(self):
        catalog = introspect()
        for schema in catalog.schemas:
            for table in schema.tables:
                yield TableArc(table)


class TraceTable(Trace):

    adapts(TableNode)

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
            return AmbiguousArc(alternatives)


class Call(Adapter):

    adapts(Arc)

    def __init__(self, arc):
        self.arc = arc

    def __call__(self):
        return []


class CallTable(Call):

    adapts(TableArc)

    def __call__(self):
        table = self.arc.table
        yield table.name, table.schema.priority
        if table.schema.name:
            name = u"%s %s" % (table.schema.name, table.name)
            yield name, -1


class CallColumn(Call):

    adapts(ColumnArc)

    def __call__(self):
        yield self.arc.column.name, 10


class CallChain(Call):

    adapts(ChainArc)

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

    adapts(SyntaxArc)


class Order(Adapter):

    adapts(Node)

    def __init__(self, node, labels):
        self.node = node
        self.labels = labels

    def __call__(self):
        return self.labels


class OrderHome(Order):

    adapts(HomeNode)


class OrderTable(Order):

    adapts(TableNode)

    def __call__(self):
        return [label.clone(is_public=(label.is_public or
                                       isinstance(label.arc, ColumnArc)))
                for label in self.labels]


@once
def classify(node):
    assert isinstance(node, Node)
    classify = Classify(node)
    labels = classify()
    return labels


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


