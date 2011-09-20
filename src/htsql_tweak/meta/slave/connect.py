#
# Copyright (c) 2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#

from htsql.context import context
from htsql.connect import Connect
from htsql.adapter import weigh
from htsql.tr.lookup import itemize, enumerate_table
from htsql.tr.binding import (AttachedTableRecipe, ColumnRecipe,
                              FreeTableRecipe)
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
            name       TEXT NOT NULL,
            domain_type TEXT NOT NULL,
            is_mandatory BOOLEAN NOT NULL,
            PRIMARY KEY (table_name, name),
            FOREIGN KEY (table_name, name)
               REFERENCES "field"(table_name, name),
            FOREIGN KEY (table_name)
               REFERENCES "table"(name)
        );
        CREATE TABLE "link" (
            table_name TEXT NOT NULL,
            name TEXT NOT NULL,
            is_singular BOOLEAN NOT NULL,
            target_table_name TEXT NOT NULL,
            reverse_link_name TEXT,
            PRIMARY KEY (table_name, name),
            FOREIGN KEY (table_name, name)
               REFERENCES "field"(table_name, name),
            FOREIGN KEY (table_name)
               REFERENCES "table"(name),
            FOREIGN KEY (target_table_name)
               REFERENCES "table"(name),
            FOREIGN KEY (target_table_name, reverse_link_name)
               REFERENCES "link"(table_name, name)
        );
    """)


def populate_meta_schema(connection):
    cursor = connection.cursor()
    tables = itemize()
    table_handles = {}

    for (table_name, recipe) in tables.items():
        if not isinstance(recipe, FreeTableRecipe):
            # only handle unambiguous top-level table links
            continue
        cursor.execute("""
          INSERT INTO "table" (name)
          VALUES (?)
        """, [table_name])
        table_handles[recipe.table] = table_name

    link_by_fk = {}
    reverse_links = []

    for (table_name, recipe) in tables.items():
        if not isinstance(recipe, FreeTableRecipe):
            # only handle unambiguous top-level table links
            continue
        fields = itemize(recipe.table)
        public = enumerate_table(recipe.table)

        def make_field(name, kind):
            sort = None
            if name in public:
                sort = public.index(name)
            cursor.execute("""
              INSERT INTO field (table_name, name, kind, sort)
              VALUES (?,?,?,?)
            """, [table_name, name, kind, sort])

        def make_column(name, domain_type, is_mandatory):
            cursor.execute("""
              INSERT INTO "column" (table_name, name,
                                    domain_type, is_mandatory)
              VALUES (?,?,?,?)
            """, [table_name, name, domain_type, is_mandatory])

        def make_link(name, is_singular, target_table_name):
            cursor.execute("""
              INSERT INTO "link" (table_name, name,
                                  is_singular, target_table_name)
              VALUES (?,?,?,?)
            """, [table_name, name, is_singular, target_table_name])

        for (name, recipe) in fields.items():
            if isinstance(recipe, ColumnRecipe):
                make_field(name, 'column')
                make_column(name, recipe.column.domain.family,
                            not recipe.column.is_nullable)
            elif isinstance(recipe, AttachedTableRecipe):
                target_table_name = table_handles.get(recipe.target_table)
                if target_table_name:
                    make_field(name, 'link')
                    make_link(name, recipe.is_singular, target_table_name)
                    if recipe.is_direct:
                        link_by_fk[recipe.joins[0].foreign_key] = \
                            (table_name, name)
                    elif recipe.is_reverse:
                        reverse_links.append((table_name, name,
                                              recipe.joins[0].foreign_key))
                    else:
                        # this is a complex link, and we don't bother
                        # tracking reverse links for them
                        pass
            else:
                # at this point, we don't handle anything other
                # than Columns or Links (attached tables)
                pass

    for (table_name, name, foreign_key) in reverse_links:
        if foreign_key in link_by_fk:
            (target_table_name, reverse_link_name) = link_by_fk[foreign_key]
            cursor.execute("""
              UPDATE "link" SET reverse_link_name = ?
               WHERE table_name = ? AND name = ?
            """, [reverse_link_name, table_name, name])

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


