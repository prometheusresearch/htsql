#
# Copyright (c) 2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.context import context
from htsql.connect import Connect
from htsql.adapter import weigh
from htsql.tr.lookup import itemize, enumerate_table
from htsql.tr.binding import (AttachedTableRecipe, ColumnRecipe,
                              AmbiguousRecipe)
from htsql.entity import DirectJoin, ReverseJoin
import sqlite3


def create_meta_schema(connection):
    cursor = connection.cursor()
    cursor.executescript("""
        CREATE TABLE "table" (
            name TEXT NOT NULL,
            PRIMARY KEY (name)
        );
        CREATE TABLE "field" (
            table_name TEXT NOT NULL,
            name TEXT NOT NULL,
            "kind" TEXT NOT NULL,
            "sort" INTEGER,
            PRIMARY KEY (table_name, name),
            FOREIGN KEY (table_name)
              REFERENCES "table"(name),
            CHECK ("kind" IN ('column', 'link'))
        );
        CREATE TABLE "column" (
            table_name TEXT NOT NULL,
            field_name TEXT NOT NULL,
            domain_type TEXT NOT NULL,
            is_mandatory BOOLEAN NOT NULL,
            PRIMARY KEY (table_name, field_name),
            FOREIGN KEY (table_name, field_name)
               REFERENCES "field"(table_name, name),
            FOREIGN KEY (table_name)
               REFERENCES "table"(name)
        );
        CREATE TABLE "link" (
            origin_table TEXT NOT NULL,
            field_name TEXT NOT NULL,
            is_singular BOOLEAN NOT NULL,
            target_table TEXT NOT NULL,
            reverse_of TEXT,
            replica_of TEXT,
            PRIMARY KEY (origin_table, field_name),
            FOREIGN KEY (origin_table, field_name)
               REFERENCES "field"(table_name, name),
            FOREIGN KEY (origin_table)
               REFERENCES "table"(name),
            FOREIGN KEY (target_table)
               REFERENCES "table"(name),
            FOREIGN KEY (target_table, reverse_of)
               REFERENCES "link"(origin_table, field_name),
            FOREIGN KEY (origin_table, replica_of)
               REFERENCES "link"(origin_table, field_name)
        );
    """)


def populate_meta_schema(connection):
    cursor = connection.cursor()
    tables = itemize()
    table_handles = {}

    for (table_name, recipe) in tables.items():
        cursor.execute("""
          INSERT INTO "table" (name)
          VALUES (?)
        """, [table_name])
        table_handles[recipe.table] = table_name

    link_by_fk = {}
    reverse_links = []
    column_links = []

    for (table_name, recipe) in tables.items():
        fields = itemize(recipe.table)
        public = enumerate_table(recipe.table)

        def make_field(field_name, kind):
            sort = None
            if field_name in public:
                sort = public.index(field_name)
            cursor.execute("""
              INSERT INTO field (table_name, name, kind, sort)
              VALUES (?,?,?,?)
            """, [table_name, field_name, kind, sort])

        def make_column(field_name, domain_type, is_mandatory):
            cursor.execute("""
              INSERT INTO "column" (table_name, field_name,
                                    domain_type, is_mandatory)
              VALUES (?,?,?,?)
            """, [table_name, field_name, domain_type, is_mandatory])

        def make_link(field_name, link):
            is_singular = all(join.is_contracting for join in link.joins)
            target_table = table_handles[link.joins[-1].target]
            cursor.execute("""
              INSERT INTO "link" (origin_table, field_name,
                                  is_singular, target_table)
              VALUES (?,?,?,?)
            """, [table_name, field_name, is_singular, target_table])

        for (field_name, recipe) in fields.items():
            if isinstance(recipe, ColumnRecipe):
                make_field(field_name, 'column')
                make_column(field_name, recipe.column.domain.family,
                            not recipe.column.is_nullable)
                if isinstance(recipe.link, AttachedTableRecipe):
                    assert len(recipe.link.joins) == 1
                    join = recipe.link.joins[0]
                    if table_handles.get(join.target):
                        make_link(field_name, recipe.link)
                        column_links.append((table_name, field_name,
                                             join.foreign_key))
            elif isinstance(recipe, AttachedTableRecipe):
                if table_handles.get(recipe.joins[-1].target):
                    make_field(field_name, 'link')
                    make_link(field_name, recipe)
                    if len(recipe.joins) == 1:
                        join = recipe.joins[0]
                        if isinstance(join, ReverseJoin):
                            reverse_links.append((table_name, field_name,
                                                  join.foreign_key))
                        elif isinstance(join, DirectJoin):
                            link_by_fk[join.foreign_key] = \
                                (table_name, field_name)
            elif isinstance(recipe, AmbiguousRecipe):
                pass
            else:
                assert False, "Unexpected Recipe Type"

    for (table_name, field_name, foreign_key) in reverse_links:
        if foreign_key in link_by_fk:
            (target_table, reverse_of) = link_by_fk[foreign_key]
            cursor.execute("""
              UPDATE "link" SET reverse_of = ?
               WHERE origin_table = ? AND field_name = ?
            """, [reverse_of, table_name, field_name])

    for (table_name, field_name, foreign_key) in column_links:
        if foreign_key in link_by_fk:
            (target_table, replica_of) = link_by_fk[foreign_key]
            cursor.execute("""
              UPDATE "link" SET replica_of = ?
               WHERE origin_table = ? AND field_name = ?
            """, [replica_of, table_name, field_name])


class MetaSlaveConnect(Connect):

    weigh(2.0) # ensure connections here are not pooled

    def open_connection(self, with_autocommit=False):
        app = context.app
        connection = app.tweak.meta.slave.cached_connection
        if connection is None:
            connection = sqlite3.connect(':memory:', check_same_thread=False)
            slave_app = app
            master_app = app.tweak.meta.slave.master()
            context.switch(slave_app, master_app)
            try:
                create_meta_schema(connection)
                populate_meta_schema(connection)
            finally:
                context.switch(master_app, slave_app)
            connection.commit()
            app.tweak.meta.slave.cached_connection = connection
        return connection


