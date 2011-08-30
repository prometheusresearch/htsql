#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# This example source is released under the MIT License
#
import os, yaml
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (Column, Integer, String, Enum, Text, Table,
                        ForeignKey, ForeignKeyConstraint, create_engine)
from sqlalchemy.orm import relationship, backref, sessionmaker

Base = declarative_base()

class School(Base):
    __table__ = Table('school', Base.metadata,
        Column('code', String(16), primary_key=True),
        Column('name', String(64), nullable=False, unique=True),
        Column('campus', Enum('old','north','south', native_enum=False)),
    )

    def __init__(self, code, name, campus):
        self.code = code
        self.name = name
        self.campus = campus

class Department(Base):
    __tablename__ = 'department'
    code = Column(String(16), primary_key=True)
    name = Column(String(64), nullable=False, unique=True)
    school_code = Column(String(16), ForeignKey('school.code'))

    school = relationship('School', 
               backref=backref('department', lazy='dynamic'))

    def __init__(self, code, name, school_code):
        self.code = code
        self.name = name
        self.school_code = school_code

class Program(Base):
    __tablename__ = 'program'
    school_code = Column(String(16), ForeignKey('school.code'), 
                         primary_key=True)
    code = Column(String(16), primary_key=True)
    title = Column(String(64), nullable=False, unique=True)
    degree = Column(Enum('ba','bs','ct','pb','ma','ms','ph'))
    part_of = Column(String(16)) 

    school = relationship('School', 
               backref=backref('program', lazy='dynamic'))

    __table_args__ = (ForeignKeyConstraint(('school_code','part_of'),
                             ('program.school_code','program.code')), {} )

    def __init__(self, school_code, code, title, degree, part_of):
        self.school_code = school_code
        self.code = code
        self.title = title
        self.degree = degree
        self.part_of = part_of

class Course(Base):
    __tablename__ = 'course'
    department_code = Column(String(16), ForeignKey('department.code'),
                              primary_key=True)
    no = Column(Integer, primary_key=True)
    title = Column(String(64), nullable=False, unique=True)
    credits = Column(Integer)
    description = Column(Text)
    
    department = relationship('Department',
                   backref=backref('course', lazy='dynamic'))

    def __init__(self, department_code, no, title, credits, description):
        self.department_code = department_code
        self.no = no
        self.title = title
        self.credits = credits
        self.description = description


def populate(engine):
    Base.metadata.create_all(engine)
    fn = os.path.join(os.path.dirname(__file__), '..', '..', 
                      'test', 'regress', 'sql', 'regress-data.yaml')
    regress = yaml.load(open(fn).read())
    Session = sessionmaker(bind=engine)
    session = Session()
    for (code, name, campus) in regress[0]['data']:
        session.add(School(code, name, campus))       
    for (code, name, school_code) in regress[1]['data']:
        session.add(Department(code, name, school_code))       
    for (school_code, code, title, degree, part_of) in regress[2]['data']:
        session.add(Program(school_code, code, title, degree, part_of))
    for (dept_code, no, title, credits, description) in regress[3]['data']:
        session.add(Course(dept_code, no, title, credits, description))
    session.commit()
    return (session.query(School).count(),
            session.query(Program).count(),
            session.query(Department).count(),
            session.query(Course).count())

def extract(engine, metadata=None):
    """ extract meta-data from module """
    return { 'driver': engine.driver,
    }
            
engine = create_engine("sqlite:///:memory:")
metadata = Base.metadata
populate(engine)
metadata.bind = engine

from htsql import HTSQL
from htsql.request import produce
htsql = HTSQL(None, {'tweak.sqlalchemy': {'engine': engine, 
                                          'metadata': metadata }})
with htsql:
   for row in produce("/{'Hello World'}"):
       print row
   for row in produce("/department{school.campus, name}"):
       print row

