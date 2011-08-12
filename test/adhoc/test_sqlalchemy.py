from htsql import HTSQL
from htsql.request import produce
from sqlalchemy import create_engine
engine = create_engine('sqlite:///:memory:')
#htsql = HTSQL(None, {'tweak.sqlalchemy': {'engine': engine }})
htsql = HTSQL('sqlite:///memory', {'tweak.sqlalchemy': {'engine': engine }})
# or PYTHONPATH=test/adhoc htsql-ctl shell -E tweak.sqlalchemy:engine=test_sqlalchemy.engine

with htsql:
   for row in produce("/{'Hello World'}"):
       print row
