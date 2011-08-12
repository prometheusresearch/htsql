import htsql
from sqlalchemy import create_engine
engine = create_engine('sqlite:///:memory:')
app = htsql.HTSQL('sqlite:///?', {'tweak.sqlalchemy': {'engine': engine }})
