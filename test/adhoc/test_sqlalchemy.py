from htsql import HTSQL
from htsql.request import produce
from sqlalchemy import create_engine
engine = create_engine('sqlite:///:memory:')
htsql = HTSQL(None, {'tweak.sqlalchemy': {'engine': engine }})

with htsql:
   for row in produce("/{'Hello World'}"):
       print row
