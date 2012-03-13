#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#

from ....core.context import context
from ....core.cache import once
from ....core.connect import Connect
from ....core.adapter import rank, Utility
from ....core.classify import classify, relabel
from ....core.model import HomeNode, TableArc, ColumnArc, ChainArc
import sqlite3


class MetaSlaveConnect(Connect):

    rank(2.0) # ensure connections here are not pooled

    def open(self):
        return build_meta()


class BuildMetaDatabase(Utility):

    def __init__(self, connection):
        self.connection = connection

    def __call__(self):
        master_app = context.app.tweak.meta.slave.master()
        with master_app:
            self.build_schema()
            self.build_data()

    def build_schema(self):
        cursor = self.connection.cursor()
        cursor.executescript("""
            CREATE TABLE "table" (
                name                TEXT NOT NULL,
                PRIMARY KEY (name)
            );

            CREATE TABLE "field" (
                table_name          TEXT NOT NULL,
                name                TEXT NOT NULL,
                kind                TEXT NOT NULL,
                sort                INTEGER,
                PRIMARY KEY (table_name, name),
                UNIQUE (table_name, sort),
                FOREIGN KEY (table_name)
                    REFERENCES "table"(name),
                CHECK ("kind" IN ('column', 'link'))
            );

            CREATE TABLE "column" (
                table_name          TEXT NOT NULL,
                name                TEXT NOT NULL,
                domain              TEXT NOT NULL,
                is_mandatory        BOOLEAN NOT NULL,
                PRIMARY KEY (table_name, name),
                FOREIGN KEY (table_name, name)
                   REFERENCES "field"(table_name, name),
                FOREIGN KEY (table_name)
                   REFERENCES "table"(name)
            );

            CREATE TABLE "link" (
                table_name          TEXT NOT NULL,
                name                TEXT NOT NULL,
                is_singular         BOOLEAN NOT NULL,
                target_name         TEXT NOT NULL,
                reverse_name        TEXT,
                PRIMARY KEY (table_name, name),
                UNIQUE (target_name, reverse_name),
                FOREIGN KEY (table_name, name)
                   REFERENCES "field"(table_name, name),
                FOREIGN KEY (table_name)
                   REFERENCES "table"(name),
                FOREIGN KEY (target_name)
                   REFERENCES "table"(name),
                FOREIGN KEY (target_name, reverse_name)
                   REFERENCES "link"(table_name, name)
            );

            PRAGMA FOREIGN_KEYS = ON;
        """)

    def build_data(self):
        cursor = self.connection.cursor()

        home_arcs = []
        duplicates = set()
        for label in classify(HomeNode()):
            arc = label.arc
            if not isinstance(arc, TableArc):
                continue
            if arc in duplicates:
                continue
            home_arcs.append(arc)
            duplicates.add(arc)

        for arc in home_arcs:
            labels = relabel(arc)
            assert len(labels) > 0
            label = labels[0]
            name = label.name
            cursor.execute("""
                INSERT INTO "table" (name)
                VALUES (?)
            """, [name])

        reverse_names = []

        for home_arc in home_arcs:
            origin = home_arc.target
            table_arcs = []
            duplicates = set()
            for label in classify(origin):
                arc = label.arc
                if not isinstance(arc, (ColumnArc, ChainArc)):
                    continue
                if arc in duplicates:
                    continue
                if (isinstance(arc, ChainArc) and
                        not relabel(TableArc(arc.target.table))):
                    continue
                table_arcs.append(arc)
                duplicates.add(arc)

            table_name = relabel(TableArc(origin.table))[0].name
            last_sort = 0
            for arc in table_arcs:
                label = relabel(arc)[0]
                name = label.name
                kind = None
                if isinstance(arc, ColumnArc):
                    kind = 'column'
                elif isinstance(arc, ChainArc):
                    kind = 'link'
                sort = None
                if label.is_public:
                    last_sort += 1
                    sort = last_sort
                cursor.execute("""
                    INSERT INTO "field" (table_name, name, kind, sort)
                    VALUES (?, ?, ?, ?)
                """, [table_name, name, kind, sort])
                if isinstance(arc, ColumnArc):
                    domain = arc.column.domain.family
                    is_mandatory = (not arc.column.is_nullable)
                    cursor.execute("""
                        INSERT INTO "column" (table_name, name,
                                              domain, is_mandatory)
                        VALUES (?, ?, ?, ?)
                    """, [table_name, name, domain, is_mandatory])
                if isinstance(arc, ChainArc):
                    is_singular = arc.is_contracting
                    target_arc = TableArc(arc.target.table)
                    target_label = relabel(target_arc)[0]
                    target_name = target_label.name
                    cursor.execute("""
                        INSERT INTO "link" (table_name, name, is_singular,
                                            target_name)
                        VALUES (?, ?, ?, ?)
                    """, [table_name, name, is_singular, target_name])
                    reverse_labels = relabel(arc.reverse())
                    if reverse_labels:
                        reverse_name = reverse_labels[0].name
                        reverse_names.append((table_name, name, reverse_name))

        for table_name, name, reverse_name in reverse_names:
            cursor.execute("""
                UPDATE "link"
                SET reverse_name = ?
                WHERE table_name = ? AND name = ?
            """, [reverse_name, table_name, name])


@once
def build_meta():
    connection = sqlite3.connect(':memory:', check_same_thread=False)
    BuildMetaDatabase.__invoke__(connection)
    connection.commit()
    return connection


