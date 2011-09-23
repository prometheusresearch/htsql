#
# Copyright (c) 2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#

from htsql.context import context
from htsql.connect import Connect
from htsql.adapter import weigh
from htsql.classify import classify, relabel
from htsql.model import HomeNode, TableNode, TableArc, ColumnArc, ChainArc
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
    tables = classify(HomeNode())
    table_handles = {}

    for label in tables:
        if not isinstance(label.arc, TableArc):
            # only handle unambiguous top-level table links
            continue
        cursor.execute("""
          INSERT INTO "table" (name)
          VALUES (?)
        """, [label.name])
        table_handles[label.arc.table] = label.name

    name_by_chain = {}

    for label in tables:
        if not isinstance(label.arc, TableArc):
            # only handle unambiguous top-level table links
            continue
        fields = classify(TableNode(label.arc.table))
        public = [field.name for field in fields if field.is_public]

        def make_field(name, kind):
            sort = None
            if name in public:
                sort = public.index(name)
            cursor.execute("""
              INSERT INTO field (table_name, name, kind, sort)
              VALUES (?,?,?,?)
            """, [label.name, name, kind, sort])

        def make_column(name, domain_type, is_mandatory):
            cursor.execute("""
              INSERT INTO "column" (table_name, name,
                                    domain_type, is_mandatory)
              VALUES (?,?,?,?)
            """, [label.name, name, domain_type, is_mandatory])

        def make_link(name, is_singular, target_table_name):
            cursor.execute("""
              INSERT INTO "link" (table_name, name,
                                  is_singular, target_table_name)
              VALUES (?,?,?,?)
            """, [label.name, name, is_singular, target_table_name])

        for field in fields:
            name = field.name
            arc = field.arc
            all_labels = relabel(arc)

            if isinstance(arc, ColumnArc):
                make_field(name, 'column')
                make_column(name, arc.column.domain.family,
                            not arc.column.is_nullable)
            elif isinstance(arc, ChainArc):
                if arc in name_by_chain:
                    continue
                target_table_name = table_handles.get(arc.target.table)
                if target_table_name:
                    make_field(name, 'link')
                    make_link(name, arc.is_contracting, target_table_name)
                name_by_chain[arc] = (label.name, name)
            else:
                # at this point, we don't handle anything other
                # than Columns or Links (attached tables)
                pass

    for arc in name_by_chain:
        table_name, name = name_by_chain[arc]
        reverse = arc.reverse()
        if reverse in name_by_chain:
            target_table_name, reverse_link_name = name_by_chain[reverse]
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


