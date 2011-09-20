#
# Copyright (c) 2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#

from . import connect, introspect
from sqlalchemy.engine.base import Engine as SQLAlchemyEngine
from sqlalchemy.schema import MetaData as SQLAlchemyMetaData
from sqlalchemy.engine.url import make_url
from htsql.validator import ClassVal
from htsql.addon import Addon, Parameter
from htsql.util import DB

class TweakSQLAlchemyAddon(Addon):

    prerequisites = []
    postrequisites = ['htsql']
    name = 'tweak.sqlalchemy'
    hint = """adapts to SQLAlchemy engine and model"""
    help = """
      This plugin provides SQLAlchemy integration in two ways.
      First, if the dburi is omitted, it attempts to use the
      database connection from SQLAlchemy.  Secondly, it uses
      the SQLAlchemy model instead of introspecting.
    """

    parameters = [
            Parameter('engine', ClassVal(SQLAlchemyEngine),
              hint='the SQLAlchemy ``engine`` object',
              value_name='package.module.attribute'),
            Parameter('metadata', ClassVal(SQLAlchemyMetaData),
              hint='the SQLAlchemy ``metadata`` object',
              value_name='package.module.attribute')
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
            url = make_url(sqlalchemy_engine.url)
            return { 'htsql': { 'db': DB(engine=engine, 
                                         database=url.database,
                                         username=url.username,
                                         password=url.password,
                                         host=url.host, port=url.port) },
                     'engine.%s' % engine : {}}
        return {}

