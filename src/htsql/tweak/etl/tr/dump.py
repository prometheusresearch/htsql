#
# Copyright (c) 2006-2013, Prometheus Research, LLC
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
                        table=self.table.name)

    def dump_columns(self):
        self.write(" (")
        for idx, column in enumerate(self.columns):
            self.format("{column:name}", column=column.name)
            if idx < len(self.columns)-1:
                self.write(", ")
        self.write(")")

    def dump_no_columns(self):
        pass

    def dump_values(self):
        self.newline()
        self.write("VALUES (")
        for idx, column in enumerate(self.columns):
            self.format("{index:placeholder}", index=None)
            if idx < len(self.columns)-1:
                self.write(", ")
        self.write(")")

    def dump_no_values(self):
        self.newline()
        self.write("DEFAULT VALUES")

    def dump_returning(self):
        self.newline()
        self.write("RETURNING ")
        for idx, column in enumerate(self.returning_columns):
            self.format("{column:name}", column=column.name)
            if idx < len(self.returning_columns)-1:
                self.write(", ")


class SerializeUpdate(Utility, DumpBase):

    def __init__(self, table, columns, key_columns, returning_columns):
        assert isinstance(table, TableEntity)
        assert isinstance(columns, listof(ColumnEntity))
        assert isinstance(key_columns, listof(ColumnEntity))
        assert isinstance(returning_columns, maybe(listof(ColumnEntity)))
        self.table = table
        self.columns = columns
        self.key_columns = key_columns
        self.returning_columns = returning_columns
        self.state = SerializingState()
        self.stream = self.state.stream

    def __call__(self):
        self.dump_update()
        if self.columns:
            self.dump_columns()
        if self.key_columns:
            self.dump_keys()
        if self.returning_columns:
            self.dump_returning()
        return self.stream.flush()

    def dump_update(self):
        if self.table.schema.name:
            self.format("UPDATE {schema:name}.{table:name}",
                        schema=self.table.schema.name,
                        table=self.table.name)
        else:
            self.format("UPDATE {table:name}",
                        table=self.table.name)

    def dump_columns(self):
        self.newline()
        self.write("SET ")
        self.indent()
        for idx, column in enumerate(self.columns):
            if idx > 0:
                self.newline()
            self.format("{column:name} = {index:placeholder}",
                        column=column.name, index=None)
            if idx < len(self.columns)-1:
                self.write(",")
        self.dedent()

    def dump_keys(self):
        self.newline()
        self.write("WHERE ")
        for idx, column in enumerate(self.key_columns):
            if idx > 0:
                self.write(" AND ")
            self.format("{column:name} = {index:placeholder}",
                        column=column.name, index=None)

    def dump_returning(self):
        self.newline()
        self.write("RETURNING ")
        for idx, column in enumerate(self.returning_columns):
            self.format("{column:name}", column=column.name)
            if idx < len(self.returning_columns)-1:
                self.write(", ")


class SerializeDelete(Utility, DumpBase):

    def __init__(self, table, key_columns):
        assert isinstance(table, TableEntity)
        assert isinstance(key_columns, listof(ColumnEntity))
        self.table = table
        self.key_columns = key_columns
        self.state = SerializingState()
        self.stream = self.state.stream

    def __call__(self):
        self.dump_delete()
        if self.key_columns:
            self.dump_keys()
        return self.stream.flush()

    def dump_delete(self):
        if self.table.schema.name:
            self.format("DELETE FROM {schema:name}.{table:name}",
                        schema=self.table.schema.name,
                        table=self.table.name)
        else:
            self.format("DELETE FROM {table:name}",
                        table=self.table.name)

    def dump_keys(self):
        self.newline()
        self.write("WHERE ")
        for idx, column in enumerate(self.key_columns):
            if idx > 0:
                self.write(" AND ")
            self.format("{column:name} = {index:placeholder}",
                        column=column.name, index=None)


class SerializeTruncate(Utility, DumpBase):

    def __init__(self, table):
        assert isinstance(table, TableEntity)
        self.table = table
        self.state = SerializingState()
        self.stream = self.state.stream

    def __call__(self):
        if self.table.schema.name:
            self.format("TRUNCATE {schema:name}.{table:name}"
                        " RESTART IDENTITY CASCADE",
                        schema=self.table.schema.name,
                        table=self.table.name)
        else:
            self.format("TRUNCATE {table:name}"
                        " RESTART IDENTITY CASCADE",
                        table=self.table.name)
        return self.stream.flush()


def serialize_insert(table, columns, returning_columns):
    return SerializeInsert.__invoke__(table, columns, returning_columns)


def serialize_update(table, columns, key_columns, returning_columns):
    return SerializeUpdate.__invoke__(table, columns, key_columns,
                                      returning_columns)


def serialize_delete(table, key_columns):
    return SerializeDelete.__invoke__(table, key_columns)


def serialize_truncate(table):
    return SerializeTruncate.__invoke__(table)


