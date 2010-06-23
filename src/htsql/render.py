#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.render`
===================

This module implements the render utility.
"""


from .adapter import Utility, find_adapters


class Output(object):

    def __init__(self, status, headers, body):
        self.status = status
        self.headers = headers
        self.body = body


class Render(Utility):

    def __call__(self, product, environ):
        status = "200 OK"
        headers = [('Content-Type', 'text/plain; charset=UTF-8')]
        body = self.render_product(product)
        return Output(status, headers, body)

    def render_product(self, product):
        if not product:
            yield "(no data)\n"
        else:
            for row in product:
                yield ", ".join(str(value) for value in row)+"\n"


render_adapters = find_adapters()


