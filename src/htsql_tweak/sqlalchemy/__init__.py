#
# Copyright (c) 2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#

from . import connect
from sqlalchemy.engine.base import Engine as SQLAlchemyEngine
from htsql.validator import ClassVal
from htsql.addon import Addon, Parameter

class TweakSQLAlchemyAddon(Addon):

    prerequisites = []
    postrequisites = ['htsql']
    name = 'tweak.sqlalchemy'

    parameters = [
            Parameter('engine', ClassVal(SQLAlchemyEngine))
    ]

    @classmethod
    def get_extensions(cls, app, attributes):
        sqlalchemy_engine = attributes['engine']
        if sqlalchemy_engine:
            assert isinstance(sqlalchemy_engine, SQLAlchemyEngine)
            return { 'htsql': { 'db': 'sqlite:///?' }}
        return {}

