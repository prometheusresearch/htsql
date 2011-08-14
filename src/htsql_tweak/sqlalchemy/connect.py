#
# Copyright (c) 2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#

from htsql.context import context
from htsql.connect import Connect
from htsql.adapter import weigh

class SQLAlchemyConnect(Connect):
    """ override normal connection with one from SQLAlchemy """

    weigh(2.0) # ensure connections here are not pooled

    def open_connection(self, with_autocommit=False):
        sqlalchemy_engine = context.app.tweak.sqlalchemy.engine
        if sqlalchemy_engine:
            wrapper = sqlalchemy_engine.connect() \
                      .execution_options(autocommit=with_autocommit)
            # wrapper.connection is a proxied DBAPI connection
            # that is in the SQLAlchemy connection pool.
            return wrapper.connection
        return super(SQLAlchemyConnect, self) \
                 .open_connection(with_autocommit)
