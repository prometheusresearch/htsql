#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#

from ...core.context import context
from ...core.util import to_name
from ...core.cache import once
from ...core.connect import Connect
from ...core.adapter import rank, Utility
from htsql_sqlite.core.connect import SQLiteError
import sqlite3
import os.path
import csv
import re


class FileDBConnect(Connect):

    rank(2.0) # ensure connections here are not pooled

    def open(self):
        return build_filedb()


class BuildFileDB(Utility):

    def __init__(self, connection):
        self.connection = connection

    def __call__(self):
        sources = context.app.tweak.filedb.sources
        cursor = self.connection.cursor()
        table_names = set()
        for source_idx, source in enumerate(sources):
            cursor = self.connection.cursor()
            if not os.path.exists(source.file):
                raise SQLiteError("file does not exist: %s" % source.file)
            try:
                stream = open(source.file)
            except IOError, exc:
                raise SQLiteError("failed to open file: %s" % source.file)
            table_name = os.path.splitext(os.path.basename(source.file))[0]
            table_name = table_name.decode('utf-8', 'replace')
            if table_name:
                table_name = to_name(table_name)
            if (not table_name or table_name in table_names or
                    re.match(r"^_\d+$", table_name)):
                table_name = "_%s" % (source_idx+1)
            table_names.add(table_name)
            reader = csv.reader(stream)
            try:
                columns_row = next(reader)
            except StopIteration:
                return
            if not columns_row:
                return
            column_names = []
            for idx, name in enumerate(columns_row):
                name = name.decode('utf-8', 'replace')
                if name:
                    name = to_name(name)
                if not name or name in column_names or re.match(r"^_\d+$", name):
                    name = u"_%s" % (idx+1)
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
                    if value is not None:
                        value = value.decode('utf-8', 'replace')
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
def build_filedb():
    connection = sqlite3.connect(':memory:', check_same_thread=False)
    BuildFileDB.__invoke__(connection)
    connection.commit()
    return connection


