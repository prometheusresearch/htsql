#!/usr/bin/env python
from bundle_config import config
from htsql.application import Application
connect = "pgsql://%(username)s:%(password)s@%(host)s:%(port)s/%(database)s"
app = Application(connect % config['postgres'])
