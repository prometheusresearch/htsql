import psycopg2
from bundle_config import config
conn = psycopg2.connect(
    host = config['postgres']['host'],
    port = int(config['postgres']['port']),
    user = config['postgres']['username'],
    password = config['postgres']['password'],
    database = config['postgres']['database'],
)
print "CHECKING FOR SCHEMA"
curr = conn.cursor()
curr.execute("BEGIN")
curr.execute("SELECT nspname FROM pg_namespace WHERE nspname = 'ad';")
rows = curr.fetchall()
if len(rows) > 0:
    curr = None
    conn.rollback()
    print "SCHEMA EXISTS"
else:
    print "NO SCHEMA, INSTALLING"
    schema = open("regress-pgsql.sql").read()
    curr.execute(schema)
    curr = None
    conn.commit()
    print "INSTALLED SCHEMA"
