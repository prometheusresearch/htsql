#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from .util import listof
from .context import context
from .cache import once
from .adapter import Adapter, adapts
from .model import (Node, Arc, Label, HomeNode, TableNode, TableArc, ChainArc,
                    ColumnArc, AmbiguousArc)
from .entity import DirectJoin, ReverseJoin
from .introspect import introspect
import re
import unicodedata


def normalize(name):
    """
    Normalizes a name to provide a valid HTSQL identifier.

    We assume `name` is a valid UTF-8 string.  Then it is:

    - translated to Unicode normal form C;
    - converted to lowercase;
    - has non-alphanumeric characters replaced with underscores;
    - preceded with an underscore if it starts with a digit.

    The result is a valid HTSQL identifier.
    """
    assert isinstance(name, str) and len(name) > 0
    name = name.decode('utf-8')
    name = unicodedata.normalize('NFC', name).lower()
    name = re.sub(ur"(?u)^(?=\d)|\W", u"_", name)
    name = name.encode('utf-8')
    return name


class Classify(Adapter):

    adapts(Node)

    def __init__(self, node):
        self.node = node
        self.catalog = introspect()

    def __call__(self):
        return []


class ClassifyHome(Classify):

    adapts(HomeNode)

    def __call__(self):
        buckets = {}
        for schema in self.catalog.schemas:
            for table in schema.tables:
                buckets.setdefault(normalize(table.name), []).append(table)

        labels = []
        collisions = []
        for name in sorted(buckets):
            candidates = buckets[name]
            if len(candidates) > 1:
                rankings = [table.schema.priority for table in candidates]
                max_rank = max(rankings)
                if rankings.count(max_rank) == 1:
                    chosen = candidates[rankings.index(max_rank)]
                    collisions.extend(table for table in candidates
                                      if table != chosen)
                    candidates = [chosen]
                else:
                    # schema ranking did not resolve ambiguity
                    collisions.extend(candidates)
            if len(candidates) > 1:
                alternatives = [TableArc(table) for table in candidates]
                arc = AmbiguousArc(alternatives)
            else:
                table = candidates[0]
                arc = TableArc(table)
            label = Label(name, arc, False)
            labels.append(label)

        duplicates = set(buckets)
        for table in collisions:
            fq_name = "%s_%s" % (normalize(table.schema.name),
                                 normalize(table.name))
            if fq_name in duplicates:
                # TODO: find some way to report when this
                # secondary naming scheme creates collisions
                continue
            arc = TableArc(table)
            label = Label(fq_name, arc, False)
            labels.append(label)
            duplicates.add(fq_name)

        return labels


class ClassifyTable(Classify):

    adapts(TableNode)

    def __init__(self, node):
        super(ClassifyTable, self).__init__(node)
        self.table = node.table

    def __call__(self):
        # FIXME: keep the original case of the object names.
        # Builds enumeration such that columns override
        # direct joins, which override reverse joins.
        labels = []
        duplicates = set()
        for label in (self.classify_columns() +
                      self.classify_direct_joins() +
                      self.classify_reverse_joins()):
            if label.name not in duplicates:
                labels.append(label)
                duplicates.add(label.name)
        return labels

    def find_link(self, column):
        # Determines if the column may represents a link to another table.
        # This is the case when the column is associated with a foreign key.

        # Get a list of foreign keys associated with the given column.
        candidates = []
        for fk in self.table.foreign_keys:
            if fk.origin_columns != [column]:
                continue
            candidates.append(fk)

        # Return immediately if there are no candidate keys.
        if not candidates:
            return None

        # Generate the joins corresponding to each alternative.
        alternatives = []
        for fk in candidates:
            join = DirectJoin(fk)
            arc = ChainArc(self.table, [join])
            alternatives.append(arc)
        # We got an unambiguous link if there's only one foreign key
        # associated with the column.
        if len(alternatives) == 1:
            return alternatives[0]
        else:
            return AmbiguousArc(alternatives)

    def classify_columns(self):
        # Builds mapping of column names into column recipes.
        # If two columns have the same normalized name, then
        # the result is ambiguous.

        label_by_column = {}
        columns_by_name = {}
        for column in self.table.columns:
            name = normalize(column.name)
            columns_by_name.setdefault(name, []).append(column)
            link = self.find_link(column)
            arc = ColumnArc(self.table, column, link)
            label = Label(name, arc, True)
            label_by_column[column] = label
        labels = []
        for column in self.table.columns:
            label = label_by_column[column]
            if len(columns_by_name[label.name]) == 1:
                labels.append(label)
            else:
                if columns_by_name[label.name][0] == column:
                    alternatives = [label_by_column[column].arc
                                    for column in columns_by_name[label.name]]
                    arc = AmbiguousArc(alternatives)
                    label = Label(label.name, arc, False)
                    labels.append(label)
        return labels

    def classify_direct_joins(self):
        # Builds mapping of table names into link recipies using
        # foreign keys originating from the current table.

        arc_by_key = {}
        names_by_key = {}
        keys_by_name = {}
        weight_by_name = {}
        for key in self.table.foreign_keys:
            weight = 0
            for uk in self.table.unique_keys:
                if all(column in uk.origin_columns
                       for column in key.origin_columns):
                    weight = 1
                    break
            join = DirectJoin(key)
            arc = ChainArc(self.table, [join])
            arc_by_key[key] = arc
            names = []
            name = normalize(key.target.name)
            names.append((name, weight))
            origin_name = normalize(key.origin_columns[-1].name)
            target_name = normalize(key.target_columns[-1].name)
            if origin_name.endswith(target_name):
                name = origin_name[:-len(target_name)].rstrip('_')
                if name:
                    names.append((name, weight+2))
            names_by_key[key] = names
            for name, weight in names:
                weight_by_name.setdefault(name, -1)
                if weight_by_name[name] < weight:
                    weight_by_name[name] = weight
                    keys_by_name[name] = []
                if weight_by_name[name] == weight:
                    keys_by_name[name].append(key)
        labels = []
        for key in self.table.foreign_keys:
            for name, weight in names_by_key[key]:
                if keys_by_name[name][0] == key:
                    if len(keys_by_name[name]) == 1:
                        arc = arc_by_key[key]
                        label = Label(name, arc, False)
                        labels.append(label)
                    else:
                        arcs = [arc_by_key[other_key]
                                for other_key in keys_by_name[name]]
                        arc = AmbiguousArc(arcs)
                        label = Label(name, arc, False)
                        labels.append(label)
        return labels

    def classify_reverse_joins(self):
        # Builds mapping of referencing tables that possess a foreign
        # key to current context table.

        arc_by_key = {}
        names_by_key = {}
        keys_by_name = {}
        weight_by_name = {}
        for key in self.table.referring_foreign_keys:
            weight = 0
            for uk in key.origin.unique_keys:
                if all(column in uk.origin_columns
                       for column in key.origin_columns):
                    weight = 1
                    break
            join = ReverseJoin(key)
            arc = ChainArc(self.table, [join])
            arc_by_key[key] = arc
            names = []
            name = normalize(key.origin.name)
            names.append((name, weight))
            origin_name = normalize(key.origin_columns[-1].name)
            target_name = normalize(key.target_columns[-1].name)
            if origin_name.endswith(target_name):
                name = origin_name[:-len(target_name)].rstrip('_')
                if name:
                    name += '_'+normalize(key.origin.name)
                    names.append((name, weight+2))
            names_by_key[key] = names
            for name, weight in names:
                weight_by_name.setdefault(name, -1)
                if weight_by_name[name] < weight:
                    weight_by_name[name] = weight
                    keys_by_name[name] = []
                if weight_by_name[name] == weight:
                    keys_by_name[name].append(key)
        labels = []
        for key in self.table.referring_foreign_keys:
            for name, weight in names_by_key[key]:
                if keys_by_name[name][0] == key:
                    if len(keys_by_name[name]) == 1:
                        arc = arc_by_key[key]
                        label = Label(name, arc, False)
                        labels.append(label)
                    else:
                        arcs = [arc_by_key[other_key]
                                for other_key in keys_by_name[name]]
                        arc = AmbiguousArc(arcs)
                        label = Label(name, arc, False)
                        labels.append(label)
        return labels


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
        key = ('relabel', arc)
        value = labels_by_arc[arc]
        if key not in cache.values:
            cache.set(key, value)
    return labels_by_arc[arcs[0]]


