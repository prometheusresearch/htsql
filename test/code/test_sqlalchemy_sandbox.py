
from sqlalchemy import (create_engine, MetaData, Table, Column, ForeignKey,
                        Integer, String)

uri = None
if sandbox.engine == 'sqlite':
    uri = "sqlite:///%s" % sandbox.database
else:
    uri = ""
    if sandbox.host is not None:
        uri += sandbox.host
        if sandbox.port is not None:
            uri += ":"+str(sandbox.port)
    if sandbox.username is not None:
        uri = "@"+uri
        if sandbox.password is not None:
            uri = ":"+sandbox.password+uri
        uri = sandbox.username+uri
    scheme = {
            'pgsql': "postgresql",
            'mssql': "mssql+pymssql",
    }.get(sandbox.engine, sandbox.engine)
    uri = "%s://%s/%s" % (scheme, uri, sandbox.database)

engine = create_engine(uri)

metadata = MetaData(engine)

# Data and metadata are borrowed from SQLAlchemy documentation.

users = Table('users', metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String(50)),
        Column('fullname', String(50)))

addresses = Table('addresses', metadata,
        Column('id', Integer, primary_key=True),
        Column('user_id', None, ForeignKey('users.id'), nullable=False),
        Column('email_address', String(50), nullable=False))

def createdb():
    metadata.create_all()
    conn = engine.connect()
    conn.execute(users.insert(), [
        {'id': 1, 'name': 'jack', 'fullname': 'Jack Jones'},
        {'id': 2, 'name': 'wendy', 'fullname': 'Wendy Williams'}])
    conn.execute(addresses.insert(), [
        {'id': 1, 'user_id': 1, 'email_address': 'jack@yahoo.com'},
        {'id': 2, 'user_id': 1, 'email_address': 'jack@msn.com'},
        {'id': 3, 'user_id': 2, 'email_address': 'www@www.org'},
        {'id': 4, 'user_id': 2, 'email_address': 'wendy@aol.com'}])

def dropdb():
    metadata.drop_all()


