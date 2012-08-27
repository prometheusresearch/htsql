#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ....core.adapter import Utility
from ....core.util import listof, maybe
from ....core.entity import TableEntity, ColumnEntity
from ....core.tr.dump import SerializingState, DumpBase


class SerializeInsert(Utility, DumpBase):

    def __init__(self, table, columns, returning_columns):
        assert isinstance(table, TableEntity)
        assert isinstance(columns, listof(ColumnEntity))
        assert isinstance(returning_columns, maybe(listof(ColumnEntity)))
        self.table = table
        self.columns = columns
        self.returning_columns = returning_columns
        self.state = SerializingState()
        self.stream = self.state.stream

    def __call__(self):
        self.dump_insert()
        if self.columns:
            self.dump_columns()
            self.dump_values()
        else:
            self.dump_no_columns()
            self.dump_no_values()
        if self.returning_columns:
            self.dump_returning()
        return self.stream.flush()

    def dump_insert(self):
        if self.table.schema.name:
            self.format("INSERT INTO {schema:name}.{table:name}",
                        schema=self.table.schema.name,
                        table=self.table.name)
        else:
            self.format("INSERT INTO {table:name}",
                        schema=self.table.schema.name,
                        table=self.table.name)

    def dump_columns(self):
        self.write(u" (")
        for idx, column in enumerate(self.columns):
            self.format("{column:name}", column=column.name)
            if idx < len(self.columns)-1:
                self.write(u", ")
        self.write(u")")

    def dump_no_columns(self):
        pass

    def dump_values(self):
        self.newline()
        self.write(u"VALUES (")
        for idx, column in enumerate(self.columns):
            self.format("{index:placeholder}", index=idx+1)
            if idx < len(self.columns)-1:
                self.write(u", ")
        self.write(u")")

    def dump_no_values(self):
        self.write(u"DEFAULT VALUES")

    def dump_returning(self):
        self.newline()
        self.write(u"RETURNING ")
        for idx, column in enumerate(self.returning_columns):
            self.format("{column:name}", column=column.name)
            if idx < len(self.returning_columns)-1:
                self.write(u", ")


def serialize_insert(table, columns, returning_columns):
    return SerializeInsert.__invoke__(table, columns, returning_columns)


