#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


from .util import DB


class Application(object):

    def __init__(self, db):
        self.db = DB.parse(db)

    def __call__(self, environ, start_response):
        start_response("200 OK", [('Content-Type', 'text/plain')])
        return ["Hello World!\n"]


