#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#

from ...core.context import context
from ...core.util import to_name
from ...core.cache import once
from ...core.connect import Connect, DBErrorGuard
from ...core.adapter import rank, Utility
from ...core.error import Error
import sqlite3
import os.path
import csv
import re
import glob


class FileDBConnect(Connect):

    rank(2.0) # ensure connections here are not pooled

    def open(self):
        return build_filedb()


class BuildFileDB(Utility):

    def __init__(self, connection):
        self.connection = connection

    def __call__(self):
        encoding = context.app.tweak.filedb.encoding
        cursor = self.connection.cursor()
        source_meta = {}
        cursor.execute("""
            SELECT 1
            FROM sqlite_master
            WHERE name = '!source'
        """)
        if cursor.fetchone() is None:
            cursor.execute("""
                CREATE TABLE "!source" (
                    name        TEXT PRIMARY KEY NOT NULL,
                    file        TEXT UNIQUE NOT NULL,
                    size        INTEGER NOT NULL,
                    timestamp   FLOAT NOT NULL
                )
            """)
        else:
            cursor.execute("""
                SELECT name, file, size, timestamp
                FROM "!source"
                ORDER BY name
            """)
            for name, file, size, timestamp in cursor.fetchall():
                source_meta[name] = (file, size, timestamp)
        for table_name, source_file in build_names():
            if not os.path.exists(source_file):
                raise Error("File does not exist", source_file)
            stat = os.stat(source_file)
            meta = (source_file, stat.st_size, stat.st_mtime)
            if table_name in source_meta:
                if meta == source_meta[table_name]:
                    continue
                else:
                    cursor.execute("""
                        UPDATE "!source"
                        SET file = ?,
                            size = ?,
                            timestamp = ?
                        WHERE name = ?
                    """, meta+(table_name,))
                    cursor.execute("""
                        DROP TABLE "%s"
                    """ % table_name)
            else:
                cursor.execute("""
                    INSERT INTO "!source" (name, file, size, timestamp)
                    VALUES (?, ?, ?, ?)
                """, (table_name,)+meta)
            try:
                stream = open(source_file, mode="rU", encoding=encoding)
            except IOError as exc:
                raise Error("Failed to open file", source_file)
            reader = csv.reader(stream)
            try:
                columns_row = next(reader)
            except StopIteration:
                continue
            if not columns_row:
                continue
            column_names = []
            for idx, name in enumerate(columns_row):
                if name:
                    name = to_name(name)
                if not name or name in column_names or re.match(r"^_\d+$", name):
                    name = "_%s" % (idx+1)
                column_names.append(name)
            records = []
            for row in reader:
                record = []
                for idx in range(len(column_names)):
                    if idx < len(row):
                        value = row[idx]
                        if not value:
                            value = None
                    else:
                        value = None
                    record.append(value)
                records.append(record)
            chunks = []
            chunks.append("CREATE TABLE \"%s\" (" % table_name)
            for idx, column_name in enumerate(column_names):
                chunks.append("    \"%s\" TEXT" % column_name
                              + ("," if idx < len(column_names)-1 else ""))
            chunks.append(")")
            sql = "\n".join(chunks)
            cursor.execute(sql)
            if records:
                chunks = []
                chunks.append("INSERT INTO \"%s\"" % table_name)
                chunks.append("VALUES (%s)" % ",".join(["?"]*len(column_names)))
                sql = "\n".join(chunks)
                cursor.executemany(sql, records)


@once
def build_names():
    sources = context.app.tweak.filedb.sources
    names = []
    table_names = set()
    source_idx = 0
    for source in sources:
        filenames = sorted(glob.glob(source.file)) or [source.file]
        for filename in filenames:
            table_name = os.path.splitext(os.path.basename(filename))[0]
            table_name = to_name(table_name)
            if (table_name in table_names or
                    re.match(r"^sqlite_", table_name) or
                    re.match(r"^_\d+$", table_name)):
                table_name = "_%s" % (source_idx+1)
            table_names.add(table_name)
            names.append((table_name, filename))
            source_idx += 1
    return names


@once
def build_filedb():
    cache_file = context.app.tweak.filedb.cache_file
    if cache_file is None:
        cache_file = ':memory:'
    with DBErrorGuard():
        connection = sqlite3.connect(cache_file, check_same_thread=False)
        BuildFileDB.__invoke__(connection)
        connection.commit()
    return connection


