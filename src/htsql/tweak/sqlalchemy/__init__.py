#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#

from . import connect, introspect
from sqlalchemy.engine.base import Engine as SQLAlchemyEngine
from sqlalchemy.schema import MetaData as SQLAlchemyMetaData
from sqlalchemy.engine.url import make_url
from ...core.validator import ClassVal
from ...core.addon import Addon, Parameter
from ...core.util import DB

class TweakSQLAlchemyAddon(Addon):

    prerequisites = []
    postrequisites = ['htsql']
    name = 'tweak.sqlalchemy'
    hint = """use SQLAlchemy engine and model"""
    help = """
    This addon provides SQLAlchemy integration in two ways.
    First, it permits using database connections from SQLAlchemy.
    Second, it uses SQLAlchemy model instead of introspecting
    the database.

    Parameter `engine` and `metadata` must point to the SQLAlchemy
    `engine` and `metadata` objects.  The objects are specified
    by a dotted string of module names.  The last component in
    the dotted string is a module attribute.
    """

    parameters = [
            Parameter('engine', ClassVal(SQLAlchemyEngine),
              value_name="MODULE.NAME",
              hint='the SQLAlchemy `engine` object'),
            Parameter('metadata', ClassVal(SQLAlchemyMetaData),
              value_name="MODULE.NAME",
              hint='the SQLAlchemy `metadata` object')
    ]

    @classmethod
    def get_extension(cls, app, attributes):
        # This provides the htsql.db plugin parameter (1st argument
        # of HTSQL) if it is not otherwise provided.  If htsql.db
        # is provided, than this operation is effectively a noop,
        # the return result is ignored.
        sqlalchemy_metadata = attributes['metadata']
        sqlalchemy_engine = attributes['engine']
        if sqlalchemy_metadata:
            assert isinstance(sqlalchemy_metadata, SQLAlchemyMetaData)
            if not sqlalchemy_engine:
                sqlalchemy_engine = sqlalchemy_metadata.bind
        if sqlalchemy_engine:
            assert isinstance(sqlalchemy_engine, SQLAlchemyEngine)
            engine = sqlalchemy_engine.dialect.name
            engine = {
                    'postgresql': 'pgsql',
            }.get(engine, engine)
            url = make_url(sqlalchemy_engine.url)
            return { 'htsql': { 'db': DB(engine=engine, 
                                         database=url.database,
                                         username=url.username,
                                         password=url.password,
                                         host=url.host, port=url.port) },
                     'engine.%s' % engine : {}}
        return {}


