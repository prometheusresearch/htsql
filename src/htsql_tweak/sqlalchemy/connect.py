#
# Copyright (c) 2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#

from htsql.context import context
from htsql.connect import Connect
from htsql.adapter import weigh
from sqlalchemy.engine.base import Engine as SQLAlchemyEngine

class SQLAlchemyConnect(Connect):
    """ override normal connection with one from SQLAlchemy """

    # ensure this gets loaded after PoolConnect in htsql/connect.py
    weigh(2.0) 

    def open_connection(self, with_autocommit=False):
        sqlalchemy_engine = context.app.tweak.sqlalchemy.engine
        if sqlalchemy_engine:
            assert isinstance(sqlalchemy_engine, SQLAlchemyEngine)
            return sqlalchemy_engine.connect() \
                      .execution_options(autocommit=with_autocommit) \
                      .connection
        return super(SQLAlchemyConnect, self) \
                 .open_connection(with_autocommit)

