
from sqlalchemy import (create_engine, MetaData, Table, Column, ForeignKey,
                        ForeignKeyConstraint, Integer, String, Text, Enum)

uri = None
if demo.engine == 'sqlite':
    uri = "sqlite:///%s" % demo.database
else:
    uri = ""
    if demo.host is not None:
        uri += demo.host
        if demo.port is not None:
            uri += ":"+str(demo.port)
    if demo.username is not None:
        uri = "@"+uri
        if demo.password is not None:
            uri = ":"+demo.password+uri
        uri = demo.username+uri
    scheme = {
            'pgsql': "postgresql",
            'mssql': "mssql+pymssql",
    }.get(demo.engine, demo.engine)
    uri = "%s://%s/%s" % (scheme, uri, demo.database)

engine = create_engine(uri)

metadata = MetaData(engine)

schema = None
prefix = ''
if demo.engine in ['pgsql', 'mssql']:
    schema = 'ad'
    prefix = schema+'.'

Table('school', metadata,
      Column('code', String(16), primary_key=True),
      Column('name', String(64), nullable=False, unique=True),
      Column('campus', Enum('old', 'north', 'south')),
      schema=schema)

Table('department', metadata,
      Column('code', String(16), primary_key=True),
      Column('name', String(64), nullable=False, unique=True),
      Column('school_code', String(16), ForeignKey(prefix+'school.code')),
      schema=schema)

Table('program', metadata,
      Column('school_code', String(16), ForeignKey(prefix+'school.code'),
             primary_key=True),
      Column('code', String(16), primary_key=True),
      Column('title', String(64), nullable=False, unique=True),
      Column('degree', Enum('ba', 'bs', 'ct', 'ma', 'ms', 'ph')),
      Column('part_of_code', String(16)),
      ForeignKeyConstraint(['school_code',
                            'part_of_code'],
                           [prefix+'program.school_code',
                            prefix+'program.code']),
      schema=schema)

Table('course', metadata,
      Column('department_code', String(16),
             ForeignKey(prefix+'department.code'), primary_key=True),
      Column('no', Integer, primary_key=True),
      Column('title', String(64), nullable=False, unique=True),
      Column('credits', Integer),
      Column('description', Text),
      schema=schema)


