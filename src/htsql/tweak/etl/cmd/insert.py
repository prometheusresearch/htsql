#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ....core.adapter import adapt
from ....core.error import BadRequestError
from ....core.connect import transaction
from ....core.domain import ListDomain, RecordDomain
from ....core.classify import normalize, classify
from ....core.model import HomeNode, TableNode, TableArc, ColumnArc
from ....core.cmd.act import Act, ProduceAction, produce
from ....core.cmd.retrieve import Product
from ....core.tr.binding import VoidBinding
from ....core.tr.decorate import decorate
from ....core.tr.coerce import coerce
from .command import InsertCmd
from ..tr.dump import serialize_insert


class ProduceInsert(Act):

    adapt(InsertCmd, ProduceAction)

    def __call__(self):
        with transaction() as connection:
            product = produce(self.command.feed)
            profile = product.meta
            domain = profile.domain
            if not (isinstance(domain, ListDomain) and
                    isinstance(domain.item_domain, RecordDomain)):
                feed_type = domain.family
                if isinstance(domain, ListDomain):
                    feed_type += " of " + domain.item_domain.family
                raise BadRequestError("unexpected feed type: expected"
                                      " a list of records; got %s" % feed_type)
            fields = domain.item_domain.fields
            if profile.tag is None:
                raise BadRequestError("missing table name")
            signature = (normalize(profile.tag), None)
            arc_by_signature = dict(((label.name, label.arity), label.arc)
                                    for label in classify(HomeNode()))
            if signature not in arc_by_signature:
                raise BadRequestError("unknown table name %s"
                                      % profile.tag.encode('utf-8'))
            arc = arc_by_signature[signature]
            if not isinstance(arc, TableArc):
                raise BadRequestError("expected a table name; got %s"
                                      % profile.tag.encode('utf-8'))
            table = arc.table
            arc_by_signature = dict(((label.name, label.arity), label.arc)
                                    for label in classify(TableNode(table)))
            slots = []
            index_by_column = {}
            for idx, field in enumerate(fields):
                if field.tag is None:
                    continue
                signature = (normalize(field.tag), None)
                if signature not in arc_by_signature:
                    raise BadRequestError("unknown column name %s"
                                          % field.tag.encode('utf-8'))
                arc = arc_by_signature[signature]
                if not isinstance(arc, ColumnArc):
                    raise BadRequestError("expected a column name; got %s"
                                          % field.tag.encode('utf-8'))
                column = arc.column
                index_by_column[column] = idx
            slice = []
            columns = []
            for column in table.columns:
                if column not in index_by_column:
                    continue
                idx = index_by_column[column]
                field = fields[idx]
                if coerce(field.domain, column.domain) is None:
                    raise BadRequestError("invalid type for column %s:"
                                          " expected %s; got %s"
                                          % (field.tag.encode('utf-8'),
                                             column.domain.family,
                                             field.domain.family))
                slice.append(idx)
                columns.append(column)
            returning_columns = []
            if table.primary_key is not None:
                returning_columns = table.primary_key.origin_columns
            else:
                for key in table.unique_keys:
                    if key.is_partial:
                        continue
                    if all(not column.is_nullable
                           for column in key.origin_columns):
                        returning_columns = key.origin_columns
                        break
            if not returning_columns:
                raise BadRequestError("table does not have a primary key")
            sql = serialize_insert(table, columns, returning_columns)
            sql = sql.encode('utf-8')
            if product.data is not None:
                cursor = connection.cursor()
                for record in product.data:
                    if record is None:
                        continue
                    values = tuple(record[idx] for idx in slice)
                    cursor.execute(sql, values)
                    returning_values = cursor.fetchall()
                    if len(returning_values) != 1:
                        raise BadRequestError("unable to locate inserted row")
                    [returning_values] = returning_values
        meta = decorate(VoidBinding())
        data = None
        return Product(meta, data)


