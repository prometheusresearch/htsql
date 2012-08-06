#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ...core.context import context
from ...core.adapter import rank
from ...core.wsgi import WSGI
from ...core.fmt.html import Template
import Cookie
import os
import binascii


class CSRFWSGI(WSGI):

    rank(10.0)

    csrf_secret_length = 16

    def __call__(self):
        token = None
        if 'HTTP_COOKIE' in self.environ:
            cookie = Cookie.SimpleCookie(self.environ['HTTP_COOKIE'])
            if 'htsql-csrf-token' in cookie:
                token = cookie['htsql-csrf-token'].value
                secret = None
                try:
                    secret = binascii.a2b_hex(token)
                except TypeError:
                    pass
                if secret is None or len(secret) != self.csrf_secret_length:
                    token = None
        header = self.environ.get('HTTP_X_HTSQL_CSRF_TOKEN')
        env = context.env
        can_read = env.can_read
        can_write = env.can_write
        if not (token and header and token == header):
            addon = context.app.tweak.csrf
            can_read = can_read and addon.allow_cs_read
            can_write = can_write and addon.allow_cs_write
        if not token:
            token = binascii.b2a_hex(os.urandom(self.csrf_secret_length))
            path = self.environ.get('SCRIPT_NAME', '')
            if not path.endswith('/'):
                path += '/'
            morsel = Cookie.Morsel()
            morsel.set('htsql-csrf-token', token, Cookie._quote(token))
            morsel['path'] = path
            cookie = morsel.OutputString()
            # FIXME: avoid state changes in the adapter.
            original_start_response = self.start_response
            def start_response(status, headers, exc=None):
                headers = headers+[('Set-Cookie', cookie)]
                return original_start_response(status, headers, exc)
            self.start_response = start_response
        with env(can_read=can_read, can_write=can_write):
            for chunk in super(CSRFWSGI, self).__call__():
                yield chunk


