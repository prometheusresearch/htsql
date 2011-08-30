#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#

# A script to load regression data into the database.


from __future__ import with_statement
from htsql.util import listof
from htsql.connect import Connect
import yaml, sys

sys.path.append('test/regress/sql/datagen')
import data_generator

REGRESS_DATA = 'test/regress/sql/regress-data.yaml'

assert state.app is not None

converter = (lambda item: item)
with_schema = True
with_pyparams = False
with_numparams = False
prelude = []

if state.app.htsql.db.engine == 'sqlite':
    with_schema = False
if state.app.htsql.db.engine == 'pgsql':
    with_pyparams = True
if state.app.htsql.db.engine == 'mysql':
    with_schema = False
    with_pyparams = True
if state.app.htsql.db.engine == 'mssql':
    converter = (lambda item: 'TRUE' if item is True else
                              'FALSE' if item is False else item)
    with_pyparams = True
    prelude = ["SET IDENTITY_INSERT cd.class ON"]
if state.app.htsql.db.engine == 'oracle':
    converter = (lambda item: 1 if item is True else
                              0 if item is False else
                              item.encode('utf-8')
                                  if isinstance(item, unicode) else item)
    with_schema = False
    with_numparams = True
    prelude = ["ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'"]


def insert_table_data(line, cursor):
    assert (isinstance(line, dict) and
            set(line) == set(['table', 'columns', 'data']))
    table = line['table']
    assert isinstance(table, str)
    columns = line['columns']
    assert isinstance(columns, listof(str))
    data = line['data']
    assert isinstance(data, listof(list))
    data = [tuple(converter(item) for item in record)
            for record in data]
    if not with_schema:
        table = table[table.find('.')+1:]
    arguments = ", ".join(columns)
    parameters = ", ".join(["?"]*len(columns))
    if with_pyparams:
        parameters = ", ".join(["%s"]*len(columns))
    if with_numparams:
        parameters = ", ".join(":"+str(idx+1) for idx in range(len(columns)))
    sql = "INSERT INTO %s (%s) VALUES (%s)" \
          % (table, arguments, parameters)
    cursor.executemany(sql, data)


with state.app:

    connect = Connect()
    connection = connect()
    cursor = connection.cursor()

    content = yaml.load(open(REGRESS_DATA))
    assert isinstance(content, list)

    for sql in prelude:
        cursor.execute(sql)

    for line in content:
        insert_table_data(line, cursor)

    generated_content = data_generator.generate(content)
    for line in generated_content:
        insert_table_data(line, cursor)

    connection.commit()
    connection.release()


