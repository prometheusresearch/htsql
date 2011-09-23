import os, yaml
from htsql import HTSQL
from wsgiref.simple_server import make_server

# create HTSQL application
config = yaml.load(open("config.yaml").read())
dburi = os.environ['DATABASE_URL'].replace('postgres','pgsql')
app = HTSQL(dburi, config)

# start webserver
port = int(os.environ['PORT'])
srv = make_server('0.0.0.0', port, app)
srv.serve_forever()
