#!/usr/bin/env python
from bundle_config import config
from htsql import HTSQL
conn = "pgsql://%(username)s:%(password)s@%(host)s:%(port)s/%(database)s"
_app = HTSQL(conn % config['postgres'])
def app(environ, start_response):
    # epio wsgi server fails to handle iterators properly
    return list(_app(environ, start_response))
